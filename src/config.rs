use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
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

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Find duplicate files in a Calibre library via hashing
    Dups(crate::dups::DupsArgs),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub logging: LoggingConfig,
    pub library: LibraryConfig,
    pub state: StateConfig,
    pub formats: FormatsConfig,
    pub calibredb: CalibredbConfig,
    pub content_server: ContentServerConfig,
    pub fetch: FetchConfig,
    pub policy: PolicyConfig,
    pub scoring: ScoringConfig,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct LibraryConfig {
    pub path: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct StateConfig {
    pub path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct FormatsConfig {
    pub list: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct CalibredbConfig {
    pub env_mode: CalibreEnvMode,
    pub debug_env: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ContentServerConfig {
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct FetchConfig {
    pub headless: bool,
    pub headless_env: HashMap<String, String>,
    pub timeout_seconds: u64,
    pub heartbeat_seconds: u64,
    pub use_xvfb: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PolicyConfig {
    pub dry_run: bool,
    pub reprocess_on_metadata_change: bool,
    pub include_missing_language: bool,
    pub english_codes: Vec<String>,
    pub delay_between_fetches_seconds: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ScoringConfig {
    pub min_score_to_skip_fetch: i32,
    pub require_title: bool,
    pub require_authors: bool,
    pub title_weight: i32,
    pub authors_weight: i32,
    pub publisher_weight: i32,
    pub pubdate_weight: i32,
    pub isbn_weight: i32,
    pub identifiers_weight: i32,
    pub tags_weight: i32,
    pub comments_weight: i32,
    pub cover_weight: i32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            logging: LoggingConfig::default(),
            library: LibraryConfig::default(),
            state: StateConfig::default(),
            formats: FormatsConfig::default(),
            calibredb: CalibredbConfig::default(),
            content_server: ContentServerConfig::default(),
            fetch: FetchConfig::default(),
            policy: PolicyConfig::default(),
            scoring: ScoringConfig::default(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}

impl Default for LibraryConfig {
    fn default() -> Self {
        Self { path: None, url: None }
    }
}

impl Default for StateConfig {
    fn default() -> Self {
        Self { path: None }
    }
}

impl Default for FormatsConfig {
    fn default() -> Self {
        Self {
            list: vec!["epub".to_string(), "pdf".to_string()],
        }
    }
}

impl Default for CalibredbConfig {
    fn default() -> Self {
        Self {
            env_mode: CalibreEnvMode::Inherit,
            debug_env: false,
        }
    }
}

impl Default for ContentServerConfig {
    fn default() -> Self {
        Self {
            username: None,
            password: None,
        }
    }
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            headless: true,
            headless_env: HashMap::from([
                ("QT_QPA_PLATFORM".to_string(), "xcb".to_string()),
                ("QTWEBENGINE_DISABLE_SANDBOX".to_string(), "1".to_string()),
                (
                    "QTWEBENGINE_CHROMIUM_FLAGS".to_string(),
                    "--no-sandbox".to_string(),
                ),
                ("QT_OPENGL".to_string(), "software".to_string()),
                ("LIBGL_ALWAYS_SOFTWARE".to_string(), "1".to_string()),
            ]),
            timeout_seconds: 45,
            heartbeat_seconds: 10,
            use_xvfb: false,
        }
    }
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            dry_run: false,
            reprocess_on_metadata_change: false,
            include_missing_language: true,
            english_codes: DEFAULT_ENGLISH_CODES.iter().map(|s| s.to_string()).collect(),
            delay_between_fetches_seconds: DEFAULT_DELAY_BETWEEN_FETCHES_SECONDS,
        }
    }
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            min_score_to_skip_fetch: DEFAULT_MIN_SCORE_TO_SKIP_FETCH,
            require_title: true,
            require_authors: true,
            title_weight: 1,
            authors_weight: 1,
            publisher_weight: 1,
            pubdate_weight: 1,
            isbn_weight: 2,
            identifiers_weight: 2,
            tags_weight: 1,
            comments_weight: 1,
            cover_weight: 1,
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
