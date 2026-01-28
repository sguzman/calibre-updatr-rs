use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(default)]
pub struct BookState {
    pub status: String,
    pub last_hash: String,
    pub last_attempt_utc: String,
    pub last_ok_utc: Option<String>,
    pub message: Option<String>,
    pub fail_count: i32,
}

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct StateFile {
    pub version: i32,
    pub updated_at_utc: Option<String>,
    pub books: HashMap<String, BookState>,
}

pub fn now_iso() -> String {
    Utc::now().to_rfc3339()
}

pub fn load_state(path: &Path) -> Result<StateFile> {
    if !path.exists() {
        return Ok(StateFile {
            version: 1,
            updated_at_utc: None,
            books: HashMap::new(),
        });
    }
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read state file {}", path.display()))?;
    let mut state: StateFile = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse state file {}", path.display()))?;
    if state.version == 0 {
        state.version = 1;
    }
    Ok(state)
}

pub fn save_state(path: &Path, state: &mut StateFile) -> Result<()> {
    state.updated_at_utc = Some(now_iso());
    let tmp_path = path.with_extension("json.tmp");
    let mut file = std::fs::File::create(&tmp_path)
        .with_context(|| format!("Failed to create {}", tmp_path.display()))?;
    let json = serde_json::to_string_pretty(state)?;
    use std::io::Write;
    file.write_all(json.as_bytes())?;
    file.write_all(b"\n")?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("Failed to move {} -> {}", tmp_path.display(), path.display()))?;
    Ok(())
}

pub fn get_book_state(state: &StateFile, book_id: i64) -> Option<BookState> {
    state.books.get(&book_id.to_string()).cloned()
}

pub fn put_book_state(state: &mut StateFile, book_id: i64, bs: BookState) {
    state.books.insert(book_id.to_string(), bs);
}
