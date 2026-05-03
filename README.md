# Calibre Updatr

Calibre Updatr is a Rust CLI for bulk-updating metadata in a Calibre library and embedding the improved metadata back into EPUB files.

## Intent

Automate the repetitive work of scanning a Calibre library, deciding which books are worth enriching, fetching better metadata, and applying those updates in a repeatable way.

## Ambition

The checked-in config, state handling, scoring/policy language, and duplicate-finder mode suggest an ambition beyond a one-off script: a durable personal-library maintenance tool with repeatable operational behavior.

## Current Status

The project already has a structured config system, state tracking, metadata fetch/update pipeline, and a duplicate-finder mode. It appears usable today for a controlled local library workflow.

## Core Capabilities Or Focus Areas

- Idempotent metadata update workflow for Calibre-managed books.
- Policy/scoring-driven selection of books to process.
- Metadata embedding back into EPUB files through Calibre tooling.
- Persistent state tracking to skip previously processed items.
- Duplicate discovery mode for library cleanup.

## Project Layout

- `src/`: Rust source for the main crate or application entrypoint.
- `Cargo.toml`: crate or workspace manifest and the first place to check for package structure.

## Setup And Requirements

- Rust toolchain.
- `calibredb` and `fetch-ebook-metadata` available on `PATH`.
- A valid `config.toml` describing the local library and fetch policy.

## Build / Run / Test Commands

```bash
cargo build
cargo test
cargo run -- --config config.toml
cargo run -- dups --library /path/to/Calibre\ Library
```

## Notes, Limitations, Or Known Gaps

- This workflow is designed around a local Calibre installation and its companion tools.
- A config file is part of the normal runtime, not an optional extra.

## Next Steps Or Roadmap Hints

- Expand fixtures and dry-run affordances so metadata policy changes are easier to validate before writing.
- Clarify the stable policy surface as heuristics mature.
