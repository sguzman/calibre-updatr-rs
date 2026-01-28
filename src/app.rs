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
use tracing::{error, info, warn};

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
    min_score_to_skip_fetch: i32,
    delay_between_fetches_seconds: f64,
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

    let (score, reasons) = score_good_enough(&snap);
    let good_enough =
        score >= min_score_to_skip_fetch && !snap.title.is_empty() && !snap.authors.is_empty();

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
    info!(id = book_id, title = %title, "[done] updated + embedded");
    Ok("done".to_string())
}

pub fn run() -> Result<()> {
    let args = Args::parse();
    require_tool("calibredb")?;
    require_tool("fetch-ebook-metadata")?;

    let config_path = PathBuf::from(&args.config);
    let mut config = load_config(&config_path)?;
    config.library = normalize_optional_string(config.library);
    config.library_url = normalize_optional_string(config.library_url);
    config.state_path = normalize_optional_string(config.state_path);
    config.calibre_username = normalize_optional_string(config.calibre_username);
    config.calibre_password = normalize_optional_string(config.calibre_password);

    if args.library.is_some() {
        config.library = args.library.clone();
        config.library_url = None;
    }
    if args.library_url.is_some() {
        config.library_url = args.library_url.clone();
    }
    if args.calibre_username.is_some() {
        config.calibre_username = args.calibre_username.clone();
    }
    if args.calibre_password.is_some() {
        config.calibre_password = args.calibre_password.clone();
    }
    if args.dry_run {
        config.dry_run = true;
    }

    init_tracing(&config.log_level);

    let lib_raw = config
        .library_url
        .clone()
        .or(config.library.clone())
        .ok_or_else(|| anyhow::anyhow!("Missing library or library_url in config"))?;
    let lib = normalize_library_spec(&lib_raw);
    let is_remote = lib.starts_with("http://") || lib.starts_with("https://");
    let state_path = if let Some(p) = config.state_path.clone() {
        PathBuf::from(p)
    } else if is_remote {
        std::env::current_dir()?.join(".calibre_metadata_state.json")
    } else {
        PathBuf::from(&lib).join(".calibre_metadata_state.json")
    };

    if !is_remote && !Path::new(&lib).is_dir() {
        anyhow::bail!("Library path does not exist or is not a directory: {lib}");
    }

    let target_formats: BTreeMap<String, ()> = config
        .formats
        .iter()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .map(|s| (s, ()))
        .collect();
    if target_formats.is_empty() {
        anyhow::bail!("No formats specified. Set formats in config.toml");
    }

    let runner = Runner {
        calibredb_env_mode: config.calibredb_env,
        debug_calibredb_env: config.debug_calibredb_env,
        headless_fetch: config.headless_fetch,
        headless_env: config.headless_env.clone(),
        calibre_username: config.calibre_username.clone(),
        calibre_password: config.calibre_password.clone(),
    };

    let mut state = load_state(&state_path)?;
    let books = list_candidate_books(
        &runner,
        &lib,
        config.include_missing_language,
        &config.english_codes,
        &target_formats,
    )?;

    info!(library = %lib, "[info] library");
    if lib.starts_with("http://") || lib.starts_with("https://") {
        info!(
            auth = %config.calibre_username.as_deref().unwrap_or("<none>"),
            "[info] calibre content server auth"
        );
    }
    info!(state = %state_path.display(), "[info] state");
    info!(
        candidates = books.len(),
        formats = %target_formats.keys().cloned().collect::<Vec<_>>().join(","),
        "[info] candidates (English-or-missing-language)"
    );
    if config.dry_run {
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
            let prev = get_book_state(&state, book_id);
            let before_hash = snapshot_hash(&metadata_snapshot(&b))?;
            if let Some(prev_state) = prev {
                if ["done", "skipped_good_enough", "embedded_only"]
                    .contains(&prev_state.status.as_str())
                    && (!config.reprocess_on_metadata_change
                        || prev_state.last_hash == before_hash)
                {
                    skipped += 1;
                    let reason = if !config.reprocess_on_metadata_change {
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
                config.reprocess_on_metadata_change,
                config.min_score_to_skip_fetch,
                config.delay_between_fetches_seconds,
                config.dry_run,
            )?;

            if config.dry_run {
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
            if config.dry_run {
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

        if !config.dry_run {
            save_state(&state_path, &mut state)?;
        }
    }

    info!(done_ok = ok, done_failed = fail, skipped, "[summary]");
    Ok(())
}
