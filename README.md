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
Edit in `src/main.rs`:
- `--formats` CLI default is `epub`; update as needed if you want a different default.
- `REPROCESS_ON_METADATA_CHANGE`: reprocess on metadata changes or not.
- `MIN_SCORE_TO_SKIP_FETCH`: how strict “good enough” is.
- `ENGLISH_CODES` / `INCLUDE_MISSING_LANGUAGE`: language filtering.
- `DELAY_BETWEEN_FETCHES_SECONDS`: throttle external metadata fetching.

Usage
-----
Requires `calibredb` and `fetch-ebook-metadata` on your PATH.

Run against a specific library path:
```bash
cargo run -- --library "/path/to/Calibre Library"
```

Set formats to process:
```bash
cargo run -- --library "/path/to/Calibre Library" --formats epub,pdf
```

Dry run (no changes):
```bash
cargo run -- --library "/path/to/Calibre Library" --dry-run
```

State file
----------
The state is stored in the library directory as:
```
.calibre_metadata_state.json
```
For remote libraries, the state file is stored in the current working directory
unless `--state-path` is supplied.
