Calibre Metadata Updatr (Rust)
==============================

Bulk metadata updater for Calibre EPUB books with idempotent processing.

Intent
------
- Iterate through a Calibre library and update metadata for EPUB books.
- Prefer books that are English or missing language.
- Fetch richer metadata when current data is incomplete.
- Embed metadata directly into EPUB files after updating the Calibre DB.
- Avoid reprocessing the same book on subsequent runs by default.

Behavior
--------
- **Idempotent by default**: each book is processed only once (per book id).
  This is controlled by `REPROCESS_ON_METADATA_CHANGE` in `src/main.rs`.
  - `false` (default): skip any book already processed successfully.
  - `true`: reprocess a book when its metadata snapshot hash changes.
- **Failure handling**: if a step fails, the error is recorded and the script continues.
- **Embedding**: successful runs embed metadata into EPUB files via `calibredb embed_metadata`.

Configuration
-------------
All settings live in `config.toml`. Start from the provided template and edit:
- `log_level`
- `library` or `library_url`
- `calibre_username` / `calibre_password`
- `headless_fetch` + `headless_env`
- `formats`
- processing policy knobs (reprocess, scoring, languages, throttling)
Note: `config.toml` can include credentials; consider excluding it from version control.

Usage
-----
Requires `calibredb` and `fetch-ebook-metadata` on your PATH.

Logging
-------
Uses `tracing` + `tracing-subscriber`. Control verbosity via `config.toml` or `RUST_LOG`:
```bash
RUST_LOG=debug cargo run -- --config config.toml
RUST_LOG=info cargo run -- --config config.toml
```

Run with config:
```bash
cargo run -- --config config.toml
```

Override config values from CLI (optional):
```bash
cargo run -- --config config.toml --library-url "http://localhost:8081/#en_nonfiction"
```

State file
----------
The state is stored in the library directory as:
```
.calibre_metadata_state.json
```
For remote libraries, the state file is stored in the current working directory
unless `state_path` is supplied in config.
