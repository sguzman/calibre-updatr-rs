use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

const ENGLISH_CODES: &[&str] = &["en", "eng", "en-us", "en-gb"];
const INCLUDE_MISSING_LANGUAGE: bool = true;
const MIN_SCORE_TO_SKIP_FETCH: i32 = 6;
const DELAY_BETWEEN_FETCHES_SECONDS: f64 = 0.35;
const REPROCESS_ON_METADATA_CHANGE: bool = false;

const CALIBRE_ENVS: &[&[(&str, &str)]] = &[
    &[
        ("LC_ALL", "en_US.utf8"),
        ("LANG", "en_US.utf8"),
        ("LANGUAGE", "en_US:en"),
        ("CALIBRE_OVERRIDE_LANG", "en"),
    ],
    &[
        ("LC_ALL", "C.utf8"),
        ("LANG", "C.utf8"),
        ("LANGUAGE", "en"),
        ("CALIBRE_OVERRIDE_LANG", "en"),
    ],
    &[
        ("LC_ALL", "C"),
        ("LANG", "C"),
        ("LANGUAGE", "en"),
        ("CALIBRE_OVERRIDE_LANG", "en"),
    ],
];

#[derive(ValueEnum, Clone, Copy, Debug)]
enum CalibreEnvMode {
    Inherit,
    Clean,
    Override,
}

#[derive(Parser, Debug)]
#[command(name = "calibre-updatr")]
#[command(about = "Calibre bulk metadata updater + format embedder", long_about = None)]
struct Args {
    #[arg(long, required = true, help = "Path to Calibre library")]
    library: String,
    #[arg(
        long,
        default_value = None,
        help = "Path to state file (default: <library>/.calibre_metadata_state.json or CWD for remote libraries)"
    )]
    state_path: Option<String>,
    #[arg(
        long,
        default_value = "epub",
        help = "Comma-separated Calibre formats to process (default: %(default)s)"
    )]
    formats: String,
    #[arg(long, action = clap::ArgAction::SetTrue, help = "Show what would happen without modifying the library")]
    dry_run: bool,
    #[arg(
        long,
        value_enum,
        default_value_t = CalibreEnvMode::Inherit,
        help = "Environment handling for calibredb: inherit (default), clean, or override"
    )]
    calibredb_env: CalibreEnvMode,
    #[arg(long, action = clap::ArgAction::SetTrue, help = "Log environment info for calibredb troubleshooting")]
    debug_calibredb_env: bool,
}

#[derive(Debug)]
struct CmdResult {
    status_code: i32,
    stdout: String,
    stderr: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(default)]
struct BookState {
    status: String,
    last_hash: String,
    last_attempt_utc: String,
    last_ok_utc: Option<String>,
    message: Option<String>,
    fail_count: i32,
}

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(default)]
struct StateFile {
    version: i32,
    updated_at_utc: Option<String>,
    books: HashMap<String, BookState>,
}

#[derive(Debug, Serialize)]
struct Snapshot {
    title: String,
    authors: Vec<String>,
    publisher: String,
    pubdate: String,
    languages: Vec<String>,
    isbn: String,
    identifiers: HashMap<String, String>,
    tags: Vec<String>,
    comments_present: bool,
    cover_present: bool,
}

#[derive(Debug)]
struct Runner {
    calibredb_env_mode: CalibreEnvMode,
    debug_calibredb_env: bool,
}

fn log(msg: &str) {
    eprintln!("{msg}");
}

fn now_iso() -> String {
    Utc::now().to_rfc3339()
}

fn require_tool(name: &str) -> Result<()> {
    which::which(name).with_context(|| format!("Missing required tool on PATH: {name}"))?;
    Ok(())
}

fn is_calibredb(cmd0: &str) -> bool {
    Path::new(cmd0)
        .file_name()
        .and_then(OsStr::to_str)
        .map(|s| s == "calibredb")
        .unwrap_or(false)
}

fn trim_if_present(s: &str) -> String {
    s.trim().to_string()
}

fn truncate(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

fn should_clean_env_key(key: &str) -> bool {
    key.starts_with("PYTHON")
        || key.starts_with("VIRTUAL_ENV")
        || key.starts_with("UV_")
        || key.starts_with("PIP_")
        || key.starts_with("CONDA")
        || key.starts_with("POETRY")
        || key.starts_with("PYENV")
}

impl Runner {
    fn run(
        &self,
        cmd: &[String],
        capture: bool,
        extra_env: Option<&HashMap<String, String>>,
    ) -> Result<CmdResult> {
        if cmd.is_empty() {
            anyhow::bail!("empty command");
        }
        log(&format!("[cmd] {}", cmd.join(" ")));
        let mut base_env: HashMap<String, String> = std::env::vars().collect();
        if let Some(extra) = extra_env {
            for (k, v) in extra {
                base_env.insert(k.clone(), v.clone());
            }
        }

        let run_with_env = |env: &HashMap<String, String>| -> Result<CmdResult> {
            let mut command = Command::new(&cmd[0]);
            for arg in &cmd[1..] {
                command.arg(arg);
            }
            if capture {
                command.stdout(Stdio::piped()).stderr(Stdio::piped());
            }
            command.env_clear();
            for (k, v) in env {
                command.env(k, v);
            }
            let output = command.output().with_context(|| {
                format!("Failed to run command: {}", cmd.join(" "))
            })?;
            Ok(CmdResult {
                status_code: output.status.code().unwrap_or(1),
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            })
        };

        if is_calibredb(&cmd[0]) {
            if self.debug_calibredb_env {
                let keys = [
                    "PYTHONPATH",
                    "PYTHONHOME",
                    "PYTHONNOUSERSITE",
                    "PYTHONUSERBASE",
                    "VIRTUAL_ENV",
                    "UV_PROJECT_ENVIRONMENT",
                    "UV_PYTHON",
                    "UV_PYTHON_BIN",
                    "UV_SYSTEM_PYTHON",
                    "CONDA_PREFIX",
                    "POETRY_ACTIVE",
                    "PYENV_VERSION",
                    "PATH",
                ];
                log(&format!(
                    "[calibredb debug] current_exe={}",
                    std::env::current_exe()
                        .ok()
                        .and_then(|p| p.to_str().map(|s| s.to_string()))
                        .unwrap_or_else(|| "<unknown>".to_string())
                ));
                for k in keys {
                    if let Some(val) = base_env.get(k) {
                        log(&format!("[calibredb debug] {k}={val}"));
                    }
                }
            }

            match self.calibredb_env_mode {
                CalibreEnvMode::Clean => {
                    base_env.retain(|k, _| !should_clean_env_key(k));
                    return run_with_env(&base_env);
                }
                CalibreEnvMode::Override => {
                    let first = run_with_env(&base_env)?;
                    if first.status_code == 0 {
                        return Ok(first);
                    }
                    let mut last = first;
                    for overrides in CALIBRE_ENVS {
                        let mut env = base_env.clone();
                        for (k, v) in *overrides {
                            env.insert((*k).to_string(), (*v).to_string());
                        }
                        let attempt = run_with_env(&env)?;
                        last = attempt;
                        if last.status_code == 0 {
                            return Ok(last);
                        }
                    }
                    if !last.stderr.trim().is_empty() {
                        log(&format!(
                            "[calibredb stderr] {}",
                            truncate(&trim_if_present(&last.stderr), 2000)
                        ));
                    }
                    if !last.stdout.trim().is_empty() {
                        log(&format!(
                            "[calibredb stdout] {}",
                            truncate(&trim_if_present(&last.stdout), 2000)
                        ));
                    }
                    return Ok(last);
                }
                CalibreEnvMode::Inherit => {
                    let first = run_with_env(&base_env)?;
                    if first.status_code == 0 {
                        return Ok(first);
                    }
                    if !first.stderr.trim().is_empty() {
                        log(&format!(
                            "[calibredb stderr] {}",
                            truncate(&trim_if_present(&first.stderr), 2000)
                        ));
                    }
                    if !first.stdout.trim().is_empty() {
                        log(&format!(
                            "[calibredb stdout] {}",
                            truncate(&trim_if_present(&first.stdout), 2000)
                        ));
                    }
                    if first.stderr.contains("No module named 'msgpack'") {
                        base_env.retain(|k, _| !should_clean_env_key(k));
                        let retry = run_with_env(&base_env)?;
                        if retry.status_code == 0 {
                            log("[info] calibredb succeeded after cleaning env vars");
                            return Ok(retry);
                        }
                        if !retry.stderr.trim().is_empty() {
                            log(&format!(
                                "[calibredb retry stderr] {}",
                                truncate(&trim_if_present(&retry.stderr), 2000)
                            ));
                        }
                        return Ok(retry);
                    }
                    return Ok(first);
                }
            }
        }

        run_with_env(&base_env)
    }
}

fn stable_json_string(value: &Value) -> Result<String> {
    let sorted = sort_value(value);
    Ok(serde_json::to_string(&sorted)?)
}

fn sort_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            for k in keys {
                if let Some(v) = map.get(&k) {
                    out.insert(k, sort_value(v));
                }
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(sort_value).collect()),
        _ => value.clone(),
    }
}

fn sha256_text(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn normalize_languages(val: &Value) -> Vec<String> {
    match val {
        Value::Null => vec![],
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.trim().to_lowercase()))
            .filter(|s| !s.is_empty())
            .collect(),
        _ => {
            let s = val.as_str().unwrap_or(&val.to_string()).trim().to_lowercase();
            if s.is_empty() {
                vec![]
            } else {
                vec![s]
            }
        }
    }
}

fn is_english_or_missing(langs: &[String]) -> bool {
    if langs.is_empty() {
        return INCLUDE_MISSING_LANGUAGE;
    }
    for x in langs {
        let x2 = x.replace('_', "-").to_lowercase();
        if ENGLISH_CODES.contains(&x2.as_str()) {
            return true;
        }
        if x2.starts_with("en-") {
            return true;
        }
        if x2 == "english" {
            return true;
        }
    }
    false
}

fn normalize_formats(val: &Value) -> Vec<String> {
    match val {
        Value::Null => vec![],
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.trim().to_lowercase()))
            .filter(|s| !s.is_empty())
            .collect(),
        _ => {
            let s = val.as_str().unwrap_or(&val.to_string()).to_lowercase();
            s.replace(';', ",")
                .split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        }
    }
}

fn has_any_format(formats_val: &Value, targets: &BTreeMap<String, ()>) -> bool {
    let fmts = normalize_formats(formats_val);
    if fmts.is_empty() {
        return false;
    }
    fmts.iter().any(|f| targets.contains_key(f))
}

fn normalize_identifiers(val: &Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Value::Object(map) = val {
        for (k, v) in map {
            let key = k.trim().to_lowercase();
            let val_s = v.as_str().unwrap_or(&v.to_string()).trim().to_string();
            if !key.is_empty() && !val_s.is_empty() {
                out.insert(key, val_s);
            }
        }
    }
    out
}

fn metadata_snapshot(book: &Value) -> Snapshot {
    let identifiers = normalize_identifiers(book.get("identifiers").unwrap_or(&Value::Null));
    let langs = normalize_languages(book.get("languages").unwrap_or(&Value::Null));

    let authors_val = book.get("authors").unwrap_or(&Value::Null);
    let authors = match authors_val {
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => {
            let s = authors_val.as_str().unwrap_or("").trim();
            if s.is_empty() {
                vec![]
            } else {
                vec![s.to_string()]
            }
        }
    };

    let tags_val = book.get("tags").unwrap_or(&Value::Null);
    let tags = match tags_val {
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => {
            let s = tags_val.as_str().unwrap_or("").trim();
            if s.is_empty() {
                vec![]
            } else {
                s.split(',')
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect()
            }
        }
    };

    Snapshot {
        title: book
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string(),
        authors,
        publisher: book
            .get("publisher")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string(),
        pubdate: book
            .get("pubdate")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string(),
        languages: langs,
        isbn: book
            .get("isbn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string(),
        identifiers,
        tags,
        comments_present: book
            .get("comments")
            .and_then(|v| v.as_str())
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false),
        cover_present: book.get("cover").is_some() && !book.get("cover").unwrap().is_null(),
    }
}

fn snapshot_hash(snap: &Snapshot) -> Result<String> {
    let value = serde_json::to_value(snap)?;
    let stable = stable_json_string(&value)?;
    Ok(sha256_text(&stable))
}

fn score_good_enough(snap: &Snapshot) -> (i32, Vec<String>) {
    let mut score = 0;
    let mut reasons = Vec::new();

    if !snap.title.is_empty() {
        score += 1;
    } else {
        reasons.push("missing title".to_string());
    }
    if !snap.authors.is_empty() {
        score += 1;
    } else {
        reasons.push("missing authors".to_string());
    }
    if !snap.publisher.is_empty() {
        score += 1;
    } else {
        reasons.push("missing publisher".to_string());
    }
    if !snap.pubdate.is_empty() {
        score += 1;
    } else {
        reasons.push("missing pubdate".to_string());
    }

    if !snap.isbn.is_empty() {
        score += 2;
    } else if !snap.identifiers.is_empty() {
        score += 2;
    } else {
        reasons.push("missing identifiers/isbn".to_string());
    }

    if !snap.tags.is_empty() {
        score += 1;
    } else {
        reasons.push("missing tags".to_string());
    }

    if snap.comments_present {
        score += 1;
    } else {
        reasons.push("missing description/comments".to_string());
    }

    if snap.cover_present {
        score += 1;
    } else {
        reasons.push("missing cover".to_string());
    }

    (score, reasons)
}

fn load_state(path: &Path) -> Result<StateFile> {
    if !path.exists() {
        return Ok(StateFile {
            version: 1,
            updated_at_utc: None,
            books: HashMap::new(),
        });
    }
    let contents = fs::read_to_string(path)
        .with_context(|| format!("Failed to read state file {}", path.display()))?;
    let mut state: StateFile = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse state file {}", path.display()))?;
    if state.version == 0 {
        state.version = 1;
    }
    Ok(state)
}

fn save_state(path: &Path, state: &mut StateFile) -> Result<()> {
    state.updated_at_utc = Some(now_iso());
    let tmp_path = path.with_extension("json.tmp");
    let mut file = fs::File::create(&tmp_path)
        .with_context(|| format!("Failed to create {}", tmp_path.display()))?;
    let json = serde_json::to_string_pretty(state)?;
    file.write_all(json.as_bytes())?;
    file.write_all(b"\n")?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("Failed to move {} -> {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn get_book_state(state: &StateFile, book_id: i64) -> Option<BookState> {
    state.books.get(&book_id.to_string()).cloned()
}

fn put_book_state(state: &mut StateFile, book_id: i64, bs: BookState) {
    state.books.insert(book_id.to_string(), bs);
}

fn list_candidate_books(
    runner: &Runner,
    lib: &str,
    target_formats: &BTreeMap<String, ()>,
) -> Result<Vec<Value>> {
    let fields = [
        "id",
        "title",
        "authors",
        "publisher",
        "pubdate",
        "languages",
        "formats",
        "isbn",
        "identifiers",
        "tags",
        "comments",
        "cover",
        "last_modified",
    ]
    .join(",");

    if target_formats.is_empty() {
        anyhow::bail!("No target formats provided.");
    }
    let search_expr = target_formats
        .keys()
        .map(|f| format!("formats:{f}"))
        .collect::<Vec<_>>()
        .join(" or ");

    let cmd = vec![
        "calibredb".to_string(),
        "--with-library".to_string(),
        lib.to_string(),
        "list".to_string(),
        "--for-machine".to_string(),
        "--fields".to_string(),
        fields,
        "--search".to_string(),
        search_expr,
    ];

    let cp = runner.run(&cmd, true, None)?;
    if cp.status_code != 0 {
        let stderr = cp.stderr.to_lowercase();
        if stderr.contains("no books matching the search expression") {
            return Ok(vec![]);
        }
        log(&format!(
            "[fatal] calibredb list failed rc={}",
            cp.status_code
        ));
        if !cp.stderr.trim().is_empty() {
            log(&cp.stderr[..cp.stderr.len().min(500)]);
        }
        anyhow::bail!("calibredb list failed");
    }

    let data: Value = serde_json::from_str(&cp.stdout)
        .with_context(|| "Failed to parse JSON from calibredb list --for-machine")?;
    let arr = data
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Unexpected JSON shape from calibredb list"))?;

    let mut out = Vec::new();
    for b in arr {
        if !b.is_object() {
            continue;
        }
        let formats_val = b.get("formats").unwrap_or(&Value::Null);
        if !has_any_format(formats_val, target_formats) {
            continue;
        }
        let langs = normalize_languages(b.get("languages").unwrap_or(&Value::Null));
        if !is_english_or_missing(&langs) {
            continue;
        }
        out.push(b.clone());
    }
    Ok(out)
}

fn fetch_metadata_to_opf_and_cover(
    runner: &Runner,
    book: &Value,
    opf_path: &Path,
    cover_path: &Path,
) -> Result<(bool, String)> {
    let title = book
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let authors_val = book.get("authors").unwrap_or(&Value::Null);
    let authors = match authors_val {
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(", "),
        _ => authors_val.as_str().unwrap_or("").trim().to_string(),
    };

    let isbn = book
        .get("isbn")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let identifiers = normalize_identifiers(book.get("identifiers").unwrap_or(&Value::Null));

    let mut cmd = vec![
        "fetch-ebook-metadata".to_string(),
        "--opf".to_string(),
        opf_path.display().to_string(),
        "--cover".to_string(),
        cover_path.display().to_string(),
    ];

    if !isbn.is_empty() {
        cmd.push("--isbn".to_string());
        cmd.push(isbn);
    } else {
        for (k, v) in identifiers {
            cmd.push("--identifier".to_string());
            cmd.push(format!("{k}:{v}"));
        }
        if !title.is_empty() {
            cmd.push("--title".to_string());
            cmd.push(title);
        }
        if !authors.is_empty() {
            cmd.push("--authors".to_string());
            cmd.push(authors);
        }
    }

    let cp = runner.run(&cmd, true, None)?;
    if cp.status_code != 0 {
        let mut msg = format!("fetch-ebook-metadata failed rc={}", cp.status_code);
        if !cp.stderr.trim().is_empty() {
            msg.push_str(&format!(
                " stderr={}",
                truncate(cp.stderr.trim(), 500)
            ));
        }
        return Ok((false, msg));
    }
    if !opf_path.exists() || opf_path.metadata()?.len() == 0 {
        return Ok((false, "fetch-ebook-metadata produced no OPF".to_string()));
    }
    Ok((true, "fetched".to_string()))
}

fn apply_opf_to_calibre_db(
    runner: &Runner,
    lib: &str,
    book_id: i64,
    opf_path: &Path,
) -> Result<(bool, String)> {
    let cmd = vec![
        "calibredb".to_string(),
        "--with-library".to_string(),
        lib.to_string(),
        "set_metadata".to_string(),
        book_id.to_string(),
        opf_path.display().to_string(),
    ];
    let cp = runner.run(&cmd, true, None)?;
    if cp.status_code != 0 {
        let mut msg = format!("set_metadata failed rc={}", cp.status_code);
        if !cp.stderr.trim().is_empty() {
            msg.push_str(&format!(
                " stderr={}",
                truncate(cp.stderr.trim(), 500)
            ));
        }
        return Ok((false, msg));
    }
    Ok((true, "metadata applied".to_string()))
}

fn apply_cover_to_calibre_db(
    runner: &Runner,
    lib: &str,
    book_id: i64,
    cover_path: &Path,
) -> Result<(bool, String)> {
    if !cover_path.exists() || cover_path.metadata()?.len() == 0 {
        return Ok((true, "no cover downloaded".to_string()));
    }

    let cmd = vec![
        "calibredb".to_string(),
        "--with-library".to_string(),
        lib.to_string(),
        "set_metadata".to_string(),
        book_id.to_string(),
        "--field".to_string(),
        format!("cover:{}", cover_path.display()),
    ];
    let cp = runner.run(&cmd, true, None)?;
    if cp.status_code != 0 {
        let mut msg = format!("cover set failed rc={}", cp.status_code);
        if !cp.stderr.trim().is_empty() {
            msg.push_str(&format!(
                " stderr={}",
                truncate(cp.stderr.trim(), 500)
            ));
        }
        return Ok((false, msg));
    }
    Ok((true, "cover applied".to_string()))
}

fn embed_metadata_into_formats(
    runner: &Runner,
    lib: &str,
    book_id: i64,
    target_formats: &BTreeMap<String, ()>,
) -> Result<(bool, String)> {
    if target_formats.is_empty() {
        return Ok((false, "no target formats".to_string()));
    }
    let fmt_arg = target_formats
        .keys()
        .map(|f| f.to_uppercase())
        .collect::<Vec<_>>()
        .join(",");
    let cmd = vec![
        "calibredb".to_string(),
        "--with-library".to_string(),
        lib.to_string(),
        "embed_metadata".to_string(),
        "--only-formats".to_string(),
        fmt_arg,
        book_id.to_string(),
    ];
    let cp = runner.run(&cmd, true, None)?;
    if cp.status_code != 0 {
        let mut msg = format!("embed_metadata failed rc={}", cp.status_code);
        if !cp.stderr.trim().is_empty() {
            msg.push_str(&format!(
                " stderr={}",
                truncate(cp.stderr.trim(), 500)
            ));
        }
        return Ok((false, msg));
    }
    Ok((true, "embedded".to_string()))
}

fn refresh_one_book(runner: &Runner, lib: &str, book_id: i64) -> Result<Option<Value>> {
    let fields = [
        "id",
        "title",
        "authors",
        "publisher",
        "pubdate",
        "languages",
        "formats",
        "isbn",
        "identifiers",
        "tags",
        "comments",
        "cover",
        "last_modified",
    ]
    .join(",");
    let cmd = vec![
        "calibredb".to_string(),
        "--with-library".to_string(),
        lib.to_string(),
        "list".to_string(),
        "--for-machine".to_string(),
        "--fields".to_string(),
        fields,
        "--search".to_string(),
        format!("id:{book_id}"),
    ];
    let cp = runner.run(&cmd, true, None)?;
    if cp.status_code != 0 || cp.stdout.trim().is_empty() {
        return Ok(None);
    }
    let data: Value = serde_json::from_str(&cp.stdout)?;
    if let Some(arr) = data.as_array() {
        if let Some(first) = arr.first() {
            if first.is_object() {
                return Ok(Some(first.clone()));
            }
        }
    }
    Ok(None)
}

fn process_one_book(
    runner: &Runner,
    state: &mut StateFile,
    book: &Value,
    workdir: &Path,
    lib: &str,
    target_formats: &BTreeMap<String, ()>,
    dry_run: bool,
) -> Result<String> {
    let book_id = book
        .get("id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow::anyhow!("missing book id"))?;
    let title = book
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    let snap = metadata_snapshot(book);
    let h = snapshot_hash(&snap)?;

    let prev = get_book_state(state, book_id);
    if let Some(prev_state) = &prev {
        if ["done", "skipped_good_enough", "embedded_only"].contains(&prev_state.status.as_str())
            && (!REPROCESS_ON_METADATA_CHANGE || prev_state.last_hash == h)
        {
            let reason = if !REPROCESS_ON_METADATA_CHANGE {
                "already processed"
            } else {
                "already processed for current metadata hash"
            };
            log(&format!(
                "[skip] id={} title={:?} ({})",
                book_id, title, reason
            ));
            return Ok("skipped".to_string());
        }
    }

    let (score, reasons) = score_good_enough(&snap);
    let good_enough = score >= MIN_SCORE_TO_SKIP_FETCH && !snap.title.is_empty() && !snap.authors.is_empty();

    if good_enough {
        log(&format!(
            "[ok?] id={} title={:?} score={} -> good enough; embedding only",
            book_id, title, score
        ));
        if dry_run {
            log(&format!(
                "[dry-run] id={} title={:?} would embed metadata into formats: {}",
                book_id,
                title,
                target_formats.keys().cloned().collect::<Vec<_>>().join(",")
            ));
            return Ok("embedded_only".to_string());
        }

        let (ok_embed, msg_embed) =
            embed_metadata_into_formats(runner, lib, book_id, target_formats)?;
        let bs = BookState {
            status: if ok_embed { "embedded_only".to_string() } else { "failed".to_string() },
            last_hash: h,
            last_attempt_utc: now_iso(),
            last_ok_utc: if ok_embed {
                Some(now_iso())
            } else {
                prev.as_ref().and_then(|p| p.last_ok_utc.clone())
            },
            message: Some(if ok_embed {
                "good enough; embedded".to_string()
            } else {
                format!(
                    "{} (good enough reasons: {})",
                    msg_embed,
                    reasons.join(", ")
                )
            }),
            fail_count: if ok_embed {
                0
            } else {
                prev.as_ref().map(|p| p.fail_count + 1).unwrap_or(1)
            },
        };
        put_book_state(state, book_id, bs);
        if ok_embed {
            log(&format!(
                "[done] id={} title={:?} (good enough; embedded)",
                book_id, title
            ));
        } else {
            log(&format!(
                "[fail] id={} title={:?} ({})",
                book_id, title, msg_embed
            ));
        }
        return Ok(if ok_embed {
            "done".to_string()
        } else {
            "failed".to_string()
        });
    }

    log(&format!(
        "[work] id={} title={:?} score={} (not good enough; will fetch). missing: {}",
        book_id,
        title,
        score,
        reasons.join(", ")
    ));

    let opf_path = workdir.join(format!("{book_id}.opf"));
    let cover_path = workdir.join(format!("{book_id}.cover.jpg"));

    if dry_run {
        log(&format!(
            "[dry-run] id={} title={:?} would fetch metadata -> apply -> embed (formats: {})",
            book_id,
            title,
            target_formats.keys().cloned().collect::<Vec<_>>().join(",")
        ));
        return Ok("updated".to_string());
    }

    let (ok_fetch, msg_fetch) = fetch_metadata_to_opf_and_cover(runner, book, &opf_path, &cover_path)?;
    if !ok_fetch {
        let bs = BookState {
            status: "failed".to_string(),
            last_hash: h,
            last_attempt_utc: now_iso(),
            last_ok_utc: prev.as_ref().and_then(|p| p.last_ok_utc.clone()),
            message: Some(msg_fetch.clone()),
            fail_count: prev.as_ref().map(|p| p.fail_count + 1).unwrap_or(1),
        };
        put_book_state(state, book_id, bs);
        log(&format!(
            "[skip] id={} title={:?} ({})",
            book_id, title, msg_fetch
        ));
        return Ok("failed".to_string());
    }

    if DELAY_BETWEEN_FETCHES_SECONDS > 0.0 {
        std::thread::sleep(Duration::from_secs_f64(DELAY_BETWEEN_FETCHES_SECONDS));
    }

    let (ok_set, msg_set) = apply_opf_to_calibre_db(runner, lib, book_id, &opf_path)?;
    if !ok_set {
        let bs = BookState {
            status: "failed".to_string(),
            last_hash: h,
            last_attempt_utc: now_iso(),
            last_ok_utc: prev.as_ref().and_then(|p| p.last_ok_utc.clone()),
            message: Some(msg_set.clone()),
            fail_count: prev.as_ref().map(|p| p.fail_count + 1).unwrap_or(1),
        };
        put_book_state(state, book_id, bs);
        log(&format!(
            "[skip] id={} title={:?} ({})",
            book_id, title, msg_set
        ));
        return Ok("failed".to_string());
    }

    let (ok_cov, msg_cov) = apply_cover_to_calibre_db(runner, lib, book_id, &cover_path)?;
    if !ok_cov {
        log(&format!(
            "[warn] id={} title={:?} ({})",
            book_id, title, msg_cov
        ));
    }

    let (ok_embed, msg_embed) =
        embed_metadata_into_formats(runner, lib, book_id, target_formats)?;
    if !ok_embed {
        let bs = BookState {
            status: "failed".to_string(),
            last_hash: h,
            last_attempt_utc: now_iso(),
            last_ok_utc: prev.as_ref().and_then(|p| p.last_ok_utc.clone()),
            message: Some(msg_embed.clone()),
            fail_count: prev.as_ref().map(|p| p.fail_count + 1).unwrap_or(1),
        };
        put_book_state(state, book_id, bs);
        log(&format!(
            "[skip] id={} title={:?} ({})",
            book_id, title, msg_embed
        ));
        return Ok("failed".to_string());
    }

    let refreshed = refresh_one_book(runner, lib, book_id)?;
    let new_snap = if let Some(refreshed_book) = refreshed {
        metadata_snapshot(&refreshed_book)
    } else {
        snap
    };
    let new_hash = snapshot_hash(&new_snap)?;

    let bs = BookState {
        status: "done".to_string(),
        last_hash: new_hash,
        last_attempt_utc: now_iso(),
        last_ok_utc: Some(now_iso()),
        message: Some("fetched+applied+embedded".to_string()),
        fail_count: 0,
    };
    put_book_state(state, book_id, bs);
    log(&format!(
        "[done] id={} title={:?} (updated + embedded)",
        book_id, title
    ));
    Ok("done".to_string())
}

fn main() -> Result<()> {
    let args = Args::parse();
    require_tool("calibredb")?;
    require_tool("fetch-ebook-metadata")?;

    let lib = args.library.clone();
    let is_remote = lib.starts_with("http://") || lib.starts_with("https://");
    let state_path = if let Some(p) = args.state_path.clone() {
        PathBuf::from(p)
    } else if is_remote {
        std::env::current_dir()?.join(".calibre_metadata_state.json")
    } else {
        PathBuf::from(&lib).join(".calibre_metadata_state.json")
    };

    if !is_remote && !Path::new(&lib).is_dir() {
        anyhow::bail!("Library path does not exist or is not a directory: {lib}");
    }

    let target_formats: BTreeMap<String, ()> = args
        .formats
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .map(|s| (s, ()))
        .collect();
    if target_formats.is_empty() {
        anyhow::bail!("No formats specified. Use --formats epub,pdf");
    }

    let runner = Runner {
        calibredb_env_mode: args.calibredb_env,
        debug_calibredb_env: args.debug_calibredb_env,
    };

    let mut state = load_state(&state_path)?;
    let books = list_candidate_books(&runner, &lib, &target_formats)?;

    log(&format!("[info] library={}", lib));
    log(&format!("[info] state={}", state_path.display()));
    log(&format!(
        "[info] candidates={} (formats={} + English-or-missing-language)",
        books.len(),
        target_formats.keys().cloned().collect::<Vec<_>>().join(",")
    ));
    if args.dry_run {
        log("[info] dry-run enabled (no changes will be written)");
    }

    let mut ok = 0;
    let mut fail = 0;
    let mut skipped = 0;

    let workdir = tempfile::TempDir::new().context("failed to create temp dir")?;
    for b in books {
        let book_id = b.get("id").and_then(|v| v.as_i64()).unwrap_or(-1);
        let title = b
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let result = (|| -> Result<String> {
            let prev = get_book_state(&state, book_id);
            let before_hash = snapshot_hash(&metadata_snapshot(&b))?;
            if let Some(prev_state) = prev {
                if ["done", "skipped_good_enough", "embedded_only"]
                    .contains(&prev_state.status.as_str())
                    && (!REPROCESS_ON_METADATA_CHANGE || prev_state.last_hash == before_hash)
                {
                    skipped += 1;
                    let reason = if !REPROCESS_ON_METADATA_CHANGE {
                        "already processed"
                    } else {
                        "already processed for current metadata hash"
                    };
                    log(&format!(
                        "[skip] id={} title={:?} ({})",
                        book_id, title, reason
                    ));
                    return Ok("skipped".to_string());
                }
            }

            let action = process_one_book(
                &runner,
                &mut state,
                &b,
                workdir.path(),
                &lib,
                &target_formats,
                args.dry_run,
            )?;

            if args.dry_run {
                if ["done", "updated", "embedded_only"].contains(&action.as_str()) {
                    ok += 1;
                } else if action == "failed" {
                    fail += 1;
                } else {
                    skipped += 1;
                }
            } else {
                let after = get_book_state(&state, book_id);
                if matches!(after.as_ref().map(|s| s.status.as_str()), Some("done")) {
                    ok += 1;
                } else if matches!(after.as_ref().map(|s| s.status.as_str()), Some("failed")) {
                    fail += 1;
                } else {
                    skipped += 1;
                }
            }
            Ok(action)
        })();

        if let Err(err) = result {
            fail += 1;
            if args.dry_run {
                log(&format!(
                    "[fail] id={} title={:?} (exception: {})",
                    book_id, title, err
                ));
                continue;
            }
            let snap = metadata_snapshot(&b);
            let h = snapshot_hash(&snap)?;
            let prev = get_book_state(&state, book_id);
            let bs = BookState {
                status: "failed".to_string(),
                last_hash: h,
                last_attempt_utc: now_iso(),
                last_ok_utc: prev.as_ref().and_then(|p| p.last_ok_utc.clone()),
                message: Some(format!("exception: {err}")),
                fail_count: prev.map(|p| p.fail_count + 1).unwrap_or(1),
            };
            put_book_state(&mut state, book_id, bs);
        }

        if !args.dry_run {
            save_state(&state_path, &mut state)?;
        }
    }

    log(&format!("[summary] done_ok={} done_failed={} skipped={}", ok, fail, skipped));
    Ok(())
}
