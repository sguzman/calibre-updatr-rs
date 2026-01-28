use crate::calibre::{
    apply_cover_to_calibre_db, apply_opf_to_calibre_db, embed_metadata_into_formats,
    fetch_metadata_to_opf_and_cover, list_candidate_books, refresh_one_book,
};
use crate::config::{
    init_tracing, load_config, normalize_library_spec, normalize_optional_string, Args,
};
use crate::metadata::{metadata_snapshot, score_good_enough, snapshot_hash};
use crate::runner::Runner;
use crate::state::{get_book_state, load_state, now_iso, put_book_state, save_state, BookState};
use anyhow::{Context, Result};
use clap::Parser;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{debug, error, info, warn};

fn require_tool(name: &str) -> Result<()> {
    which::which(name).with_context(|| format!("Missing required tool on PATH: {name}"))?;
    Ok(())
}

fn process_one_book(
    runner: &Runner,
    state: &mut crate::state::StateFile,
    book: &serde_json::Value,
    workdir: &Path,
    lib: &str,
    target_formats: &BTreeMap<String, ()>,
    reprocess_on_metadata_change: bool,
    scoring: &crate::config::ScoringConfig,
    delay_between_fetches_seconds: f64,
    fetch_timeout_seconds: u64,
    state_path: &Path,
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
        if ["done", "skipped_good_enough", "embedded_only", "failed_permanent"]
            .contains(&prev_state.status.as_str())
            && (!reprocess_on_metadata_change || prev_state.last_hash == h)
        {
            let reason = if !reprocess_on_metadata_change {
                "already processed"
            } else {
                "already processed for current metadata hash"
            };
            info!(id = book_id, title = %title, reason = %reason, "[skip]");
            return Ok("skipped".to_string());
        }
    }

    let (score, reasons) = score_good_enough(&snap, scoring);
    let good_enough = score >= scoring.min_score_to_skip_fetch
        && (!scoring.require_title || !snap.title.is_empty())
        && (!scoring.require_authors || !snap.authors.is_empty());

    let started = BookState {
        status: "started".to_string(),
        last_hash: h.clone(),
        last_attempt_utc: now_iso(),
        last_ok_utc: prev.as_ref().and_then(|p| p.last_ok_utc.clone()),
        message: Some("started".to_string()),
        fail_count: prev.as_ref().map(|p| p.fail_count).unwrap_or(0),
    };
    put_book_state(state, book_id, started);
    save_state(state_path, state)?;

    if good_enough {
        info!(
            id = book_id,
            title = %title,
            score,
            "[good-enough] embedding only"
        );
        if dry_run {
            info!(
                id = book_id,
                title = %title,
                formats = %target_formats.keys().cloned().collect::<Vec<_>>().join(","),
                "[dry-run] embed metadata"
            );
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
                format!("{} (good enough reasons: {})", msg_embed, reasons.join(", "))
            }),
            fail_count: if ok_embed {
                0
            } else {
                prev.as_ref().map(|p| p.fail_count + 1).unwrap_or(1)
            },
        };
        put_book_state(state, book_id, bs);
        save_state(state_path, state)?;
        if ok_embed {
            info!(id = book_id, title = %title, "[done] good enough; embedded");
        } else {
            warn!(id = book_id, title = %title, error = %msg_embed, "[fail] embed");
        }
        return Ok(if ok_embed { "done".to_string() } else { "failed".to_string() });
    }

    info!(
        id = book_id,
        title = %title,
        score,
        missing = %reasons.join(", "),
        "[work] fetch metadata"
    );

    let opf_path = workdir.join(format!("{book_id}.opf"));
    let cover_path = workdir.join(format!("{book_id}.cover.jpg"));

    if dry_run {
        info!(
            id = book_id,
            title = %title,
            formats = %target_formats.keys().cloned().collect::<Vec<_>>().join(","),
            "[dry-run] fetch -> apply -> embed"
        );
        return Ok("updated".to_string());
    }

    let (ok_fetch, msg_fetch) = fetch_metadata_to_opf_and_cover(
        runner,
        book,
        &opf_path,
        &cover_path,
        fetch_timeout_seconds,
    )?;
    if !ok_fetch {
        let status = if msg_fetch.contains("timed out") {
            "failed_permanent"
        } else {
            "failed"
        };
        let bs = BookState {
            status: status.to_string(),
            last_hash: h,
            last_attempt_utc: now_iso(),
            last_ok_utc: prev.as_ref().and_then(|p| p.last_ok_utc.clone()),
            message: Some(msg_fetch.clone()),
            fail_count: prev.as_ref().map(|p| p.fail_count + 1).unwrap_or(1),
        };
        put_book_state(state, book_id, bs);
        save_state(state_path, state)?;
        warn!(id = book_id, title = %title, error = %msg_fetch, "[skip] fetch");
        return Ok("failed".to_string());
    }

    if delay_between_fetches_seconds > 0.0 {
        std::thread::sleep(Duration::from_secs_f64(delay_between_fetches_seconds));
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
        save_state(state_path, state)?;
        warn!(id = book_id, title = %title, error = %msg_set, "[skip] set_metadata");
        return Ok("failed".to_string());
    }

    let (ok_cov, msg_cov) = apply_cover_to_calibre_db(runner, lib, book_id, &cover_path)?;
    if !ok_cov {
        warn!(id = book_id, title = %title, error = %msg_cov, "[warn] cover");
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
        save_state(state_path, state)?;
        warn!(id = book_id, title = %title, error = %msg_embed, "[skip] embed");
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
    save_state(state_path, state)?;
    info!(id = book_id, title = %title, "[done] updated + embedded");
    Ok("done".to_string())
}

pub fn run() -> Result<()> {
    let args = Args::parse();
    require_tool("calibredb")?;
    require_tool("fetch-ebook-metadata")?;

    let config_path = PathBuf::from(&args.config);
    let mut config = load_config(&config_path)?;
    config.library.path = normalize_optional_string(config.library.path);
    config.library.url = normalize_optional_string(config.library.url);
    config.state.path = normalize_optional_string(config.state.path);
    config.content_server.username = normalize_optional_string(config.content_server.username);
    config.content_server.password = normalize_optional_string(config.content_server.password);

    if args.library.is_some() {
        config.library.path = args.library.clone();
        config.library.url = None;
    }
    if args.library_url.is_some() {
        config.library.url = args.library_url.clone();
    }
    if args.calibre_username.is_some() {
        config.content_server.username = args.calibre_username.clone();
    }
    if args.calibre_password.is_some() {
        config.content_server.password = args.calibre_password.clone();
    }
    if args.dry_run {
        config.policy.dry_run = true;
    }

    init_tracing(&config.logging.level);

    let lib_raw = config
        .library
        .url
        .clone()
        .or(config.library.path.clone())
        .ok_or_else(|| anyhow::anyhow!("Missing library or library_url in config"))?;
    let lib = normalize_library_spec(&lib_raw);
    let is_remote = lib.starts_with("http://") || lib.starts_with("https://");
    let state_path = if let Some(p) = config.state.path.clone() {
        PathBuf::from(p)
    } else {
        default_state_path()?
    };

    if !is_remote && !Path::new(&lib).is_dir() {
        anyhow::bail!("Library path does not exist or is not a directory: {lib}");
    }

    let target_formats: BTreeMap<String, ()> = config
        .formats
        .list
        .iter()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .map(|s| (s, ()))
        .collect();
    if target_formats.is_empty() {
        anyhow::bail!("No formats specified. Set formats in config.toml");
    }

    let runner = Runner {
        calibredb_env_mode: config.calibredb.env_mode,
        debug_calibredb_env: config.calibredb.debug_env,
        headless_fetch: config.fetch.headless,
        headless_env: config.fetch.headless_env.clone(),
        calibre_username: config.content_server.username.clone(),
        calibre_password: config.content_server.password.clone(),
    };

    let mut state = load_state(&state_path)?;
    let books = list_candidate_books(
        &runner,
        &lib,
        config.policy.include_missing_language,
        &config.policy.english_codes,
        &target_formats,
    )?;

    info!(library = %lib, "[info] library");
    if lib.starts_with("http://") || lib.starts_with("https://") {
        info!(
            auth = %config.content_server.username.as_deref().unwrap_or("<none>"),
            "[info] calibre content server auth"
        );
    }
    info!(state = %state_path.display(), "[info] state");
    info!(
        candidates = books.len(),
        formats = %target_formats.keys().cloned().collect::<Vec<_>>().join(","),
        "[info] candidates (English-or-missing-language)"
    );
    if config.policy.dry_run {
        info!("[info] dry-run enabled (no changes will be written)");
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
            debug!(id = book_id, title = %title, "[book] start");
            let prev = get_book_state(&state, book_id);
            let before_hash = snapshot_hash(&metadata_snapshot(&b))?;
            if let Some(prev_state) = prev {
                if ["done", "skipped_good_enough", "embedded_only", "failed_permanent"]
                    .contains(&prev_state.status.as_str())
                    && (!config.policy.reprocess_on_metadata_change
                        || prev_state.last_hash == before_hash)
                {
                    skipped += 1;
                    let reason = if !config.policy.reprocess_on_metadata_change {
                        "already processed"
                    } else {
                        "already processed for current metadata hash"
                    };
                    info!(id = book_id, title = %title, reason = %reason, "[skip]");
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
                config.policy.reprocess_on_metadata_change,
                &config.scoring,
                config.policy.delay_between_fetches_seconds,
                config.fetch.timeout_seconds,
                &state_path,
                config.policy.dry_run,
            )?;

            if config.policy.dry_run {
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
            if config.policy.dry_run {
                error!(id = book_id, title = %title, error = %err, "[fail] exception");
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

        if !config.policy.dry_run {
            save_state(&state_path, &mut state)?;
        }
    }

    info!(done_ok = ok, done_failed = fail, skipped, "[summary]");
    Ok(())
}

fn default_state_path() -> Result<PathBuf> {
    let base = std::env::var("XDG_CACHE_HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(|| std::env::var("HOME").ok().map(|home| PathBuf::from(home).join(".cache")))
        .ok_or_else(|| anyhow::anyhow!("Unable to determine cache directory (set XDG_CACHE_HOME or HOME)"))?;
    let dir = base.join("calibre-updatr");
    std::fs::create_dir_all(&dir)?;
    Ok(dir.join("state.json"))
}
