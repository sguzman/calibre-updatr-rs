use crate::metadata::{
    has_any_format, is_english_or_missing, normalize_identifiers_for_fetch,
    normalize_languages_for_filter,
};
use crate::runner::Runner;
use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use tracing::{error, info};

pub fn append_calibre_auth(
    cmd: &mut Vec<String>,
    lib: &str,
    username: &Option<String>,
    password: &Option<String>,
) {
    if !(lib.starts_with("http://") || lib.starts_with("https://")) {
        return;
    }
    if let Some(user) = username {
        cmd.push("--username".to_string());
        cmd.push(user.clone());
        if let Some(pass) = password {
            cmd.push("--password".to_string());
            cmd.push(pass.clone());
        }
    }
}

pub fn list_candidate_books(
    runner: &Runner,
    lib: &str,
    include_missing_language: bool,
    english_codes: &[String],
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

    let mut cmd = vec![
        "calibredb".to_string(),
        "--with-library".to_string(),
        lib.to_string(),
    ];
    append_calibre_auth(
        &mut cmd,
        lib,
        &runner.calibre_username,
        &runner.calibre_password,
    );
    cmd.extend([
        "list".to_string(),
        "--for-machine".to_string(),
        "--fields".to_string(),
        fields,
        "--search".to_string(),
        search_expr,
    ]);

    let cp = runner.run(&cmd, true, None)?;
    if cp.status_code != 0 {
        let stderr = cp.stderr.to_lowercase();
        if stderr.contains("another calibre program such as calibre-server")
            || stderr.contains("another calibre program such as calibre server")
        {
            anyhow::bail!(
                "calibredb refused to use the library because Calibre (or calibre-server) is running.\n\
Either close Calibre or pass --library-url pointing at the running Content Server."
            );
        }
        if stderr.contains("not found") && lib.starts_with("http") {
            anyhow::bail!(
                "calibredb returned Not Found for the library URL.\n\
Check the Content Server URL and library id, and avoid a trailing slash after the fragment.\n\
Example: --library-url \"http://localhost:8081/#en_nonfiction\""
            );
        }
        if stderr.contains("no books matching the search expression") {
            return Ok(vec![]);
        }
        error!(rc = cp.status_code, "[fatal] calibredb list failed");
        if !cp.stderr.trim().is_empty() {
            error!(stderr = %cp.stderr.chars().take(500).collect::<String>(), "[fatal] calibredb list stderr");
        }
        anyhow::bail!("calibredb list failed");
    }

    let data: Value = serde_json::from_str(&cp.stdout)?;
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
        let langs = normalize_languages_for_filter(b.get("languages").unwrap_or(&Value::Null));
        if !is_english_or_missing(&langs, include_missing_language, english_codes) {
            continue;
        }
        out.push(b.clone());
    }
    Ok(out)
}

pub fn fetch_metadata_to_opf_and_cover(
    runner: &Runner,
    book: &Value,
    opf_path: &Path,
    cover_path: &Path,
    timeout_seconds: u64,
    heartbeat_seconds: u64,
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
    let identifiers = normalize_identifiers_for_fetch(book.get("identifiers").unwrap_or(&Value::Null));

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
            cmd.push(title.clone());
        }
        if !authors.is_empty() {
            cmd.push("--authors".to_string());
            cmd.push(authors);
        }
    }

    info!(timeout_seconds, title = %title, "[fetch] starting fetch-ebook-metadata");
    let cp = runner.run_fetch_streaming(
        &cmd,
        std::time::Duration::from_secs(timeout_seconds),
        std::time::Duration::from_secs(heartbeat_seconds),
    )?;
    if cp.timed_out {
        return Ok((false, format!("fetch-ebook-metadata timed out after {}s", timeout_seconds)));
    }
    if cp.status_code != 0 {
        let mut msg = format!("fetch-ebook-metadata failed rc={}", cp.status_code);
        if !cp.stderr.trim().is_empty() {
            msg.push_str(&format!(" stderr={}", cp.stderr.trim().chars().take(500).collect::<String>()));
        }
        return Ok((false, msg));
    }
    if !opf_path.exists() || opf_path.metadata()?.len() == 0 {
        return Ok((false, "fetch-ebook-metadata produced no OPF".to_string()));
    }
    Ok((true, "fetched".to_string()))
}

pub fn apply_opf_to_calibre_db(
    runner: &Runner,
    lib: &str,
    book_id: i64,
    opf_path: &Path,
) -> Result<(bool, String)> {
    let mut cmd = vec![
        "calibredb".to_string(),
        "--with-library".to_string(),
        lib.to_string(),
    ];
    append_calibre_auth(
        &mut cmd,
        lib,
        &runner.calibre_username,
        &runner.calibre_password,
    );
    cmd.extend([
        "set_metadata".to_string(),
        book_id.to_string(),
        opf_path.display().to_string(),
    ]);
    info!(book_id, "[apply] set_metadata");
    let cp = runner.run(&cmd, true, None)?;
    if cp.status_code != 0 {
        let mut msg = format!("set_metadata failed rc={}", cp.status_code);
        if !cp.stderr.trim().is_empty() {
            msg.push_str(&format!(" stderr={}", cp.stderr.trim().chars().take(500).collect::<String>()));
        }
        return Ok((false, msg));
    }
    Ok((true, "metadata applied".to_string()))
}

pub fn apply_cover_to_calibre_db(
    runner: &Runner,
    lib: &str,
    book_id: i64,
    cover_path: &Path,
) -> Result<(bool, String)> {
    if !cover_path.exists() || cover_path.metadata()?.len() == 0 {
        return Ok((true, "no cover downloaded".to_string()));
    }

    let mut cmd = vec![
        "calibredb".to_string(),
        "--with-library".to_string(),
        lib.to_string(),
    ];
    append_calibre_auth(
        &mut cmd,
        lib,
        &runner.calibre_username,
        &runner.calibre_password,
    );
    cmd.extend([
        "set_metadata".to_string(),
        book_id.to_string(),
        "--field".to_string(),
        format!("cover:{}", cover_path.display()),
    ]);
    info!(book_id, "[apply] cover");
    let cp = runner.run(&cmd, true, None)?;
    if cp.status_code != 0 {
        let mut msg = format!("cover set failed rc={}", cp.status_code);
        if !cp.stderr.trim().is_empty() {
            msg.push_str(&format!(" stderr={}", cp.stderr.trim().chars().take(500).collect::<String>()));
        }
        return Ok((false, msg));
    }
    Ok((true, "cover applied".to_string()))
}

pub fn embed_metadata_into_formats(
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
    let mut cmd = vec![
        "calibredb".to_string(),
        "--with-library".to_string(),
        lib.to_string(),
    ];
    append_calibre_auth(
        &mut cmd,
        lib,
        &runner.calibre_username,
        &runner.calibre_password,
    );
    cmd.extend([
        "embed_metadata".to_string(),
        "--only-formats".to_string(),
        fmt_arg,
        book_id.to_string(),
    ]);
    info!(book_id, "[embed] embed_metadata");
    let cp = runner.run(&cmd, true, None)?;
    if cp.status_code != 0 {
        let mut msg = format!("embed_metadata failed rc={}", cp.status_code);
        if !cp.stderr.trim().is_empty() {
            msg.push_str(&format!(" stderr={}", cp.stderr.trim().chars().take(500).collect::<String>()));
        }
        return Ok((false, msg));
    }
    Ok((true, "embedded".to_string()))
}

pub fn refresh_one_book(runner: &Runner, lib: &str, book_id: i64) -> Result<Option<Value>> {
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
    let mut cmd = vec![
        "calibredb".to_string(),
        "--with-library".to_string(),
        lib.to_string(),
    ];
    append_calibre_auth(
        &mut cmd,
        lib,
        &runner.calibre_username,
        &runner.calibre_password,
    );
    cmd.extend([
        "list".to_string(),
        "--for-machine".to_string(),
        "--fields".to_string(),
        fields,
        "--search".to_string(),
        format!("id:{book_id}"),
    ]);
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
