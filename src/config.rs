use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing_subscriber::{fmt, EnvFilter};

const DEFAULT_ENGLISH_CODES: &[&str] = &["en", "eng", "en-us", "en-gb"];
const DEFAULT_MIN_SCORE_TO_SKIP_FETCH: i32 = 6;
const DEFAULT_DELAY_BETWEEN_FETCHES_SECONDS: f64 = 0.35;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CalibreEnvMode {
    Inherit,
    Clean,
    Override,
}

#[derive(Parser, Debug)]
#[command(name = "calibre-updatr")]
#[command(about = "Calibre bulk metadata updater + format embedder", long_about = None)]
pub struct Args {
    #[arg(long, default_value = "config.toml", help = "Path to config.toml")]
    pub config: String,
    #[arg(long, help = "Override: Path to Calibre library")]
    pub library: Option<String>,
    #[arg(long, help = "Override: Calibre Content Server URL to the library")]
    pub library_url: Option<String>,
    #[arg(long, help = "Override: Calibre Content Server username")]
    pub calibre_username: Option<String>,
    #[arg(long, help = "Override: Calibre Content Server password")]
    pub calibre_password: Option<String>,
    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        help = "Override: dry run (no changes)"
    )]
    pub dry_run: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub log_level: String,
    pub library: Option<String>,
    pub library_url: Option<String>,
    pub state_path: Option<String>,
    pub formats: Vec<String>,
    pub dry_run: bool,
    pub calibredb_env: CalibreEnvMode,
    pub debug_calibredb_env: bool,
    pub headless_fetch: bool,
    pub headless_env: HashMap<String, String>,
    pub calibre_username: Option<String>,
    pub calibre_password: Option<String>,
    pub reprocess_on_metadata_change: bool,
    pub min_score_to_skip_fetch: i32,
    pub include_missing_language: bool,
    pub english_codes: Vec<String>,
    pub delay_between_fetches_seconds: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log_level: "info".to_string(),
            library: None,
            library_url: None,
            state_path: None,
            formats: vec!["epub".to_string(), "pdf".to_string()],
            dry_run: false,
            calibredb_env: CalibreEnvMode::Inherit,
            debug_calibredb_env: false,
            headless_fetch: true,
            headless_env: HashMap::from([
                ("QT_QPA_PLATFORM".to_string(), "offscreen".to_string()),
                ("QTWEBENGINE_DISABLE_SANDBOX".to_string(), "1".to_string()),
                (
                    "QTWEBENGINE_CHROMIUM_FLAGS".to_string(),
                    "--no-sandbox --disable-gpu".to_string(),
                ),
                ("QTWEBENGINE_DISABLE_GPU".to_string(), "1".to_string()),
                ("LIBGL_ALWAYS_SOFTWARE".to_string(), "1".to_string()),
            ]),
            calibre_username: None,
            calibre_password: None,
            reprocess_on_metadata_change: false,
            min_score_to_skip_fetch: DEFAULT_MIN_SCORE_TO_SKIP_FETCH,
            include_missing_language: true,
            english_codes: DEFAULT_ENGLISH_CODES.iter().map(|s| s.to_string()).collect(),
            delay_between_fetches_seconds: DEFAULT_DELAY_BETWEEN_FETCHES_SECONDS,
        }
    }
}

pub fn init_tracing(default_level: &str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_level));
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_level(true)
        .init();
}

pub fn normalize_library_spec(spec: &str) -> String {
    let trimmed = spec.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        let without_trailing = trimmed.trim_end_matches('/');
        return without_trailing.to_string();
    }
    trimmed.to_string()
}

pub fn normalize_optional_string(value: Option<String>) -> Option<String> {
    match value {
        Some(s) if s.trim().is_empty() => None,
        Some(s) => Some(s),
        None => None,
    }
}

pub fn load_config(path: &Path) -> Result<Config> {
    let contents = std::fs::read_to_string(path).with_context(|| {
        format!(
            "Failed to read config file {} (create one from config.toml)",
            path.display()
        )
    })?;
    let cfg: Config = toml::from_str(&contents)
        .with_context(|| format!("Failed to parse config {}", path.display()))?;
    Ok(cfg)
}
