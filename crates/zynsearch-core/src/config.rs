use clap::{ArgAction, Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    Text,
    Json,
    Binary,
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProtocolMode {
    Tcp,
    Http,
    Grpc,
    Both,
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IngestionMode {
    LocalDir,
    S3,
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DistributionChannel {
    Tcp,
    Http,
    Grpc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub host: String,
    pub port: u16,
    pub protocol: ProtocolMode,
    pub output_format: OutputFormat,
    pub query: Option<String>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 7777,
            protocol: ProtocolMode::Tcp,
            output_format: OutputFormat::Text,
            query: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct IngestionConfig {
    pub mode: IngestionMode,
    pub corpus_dir: String,
    pub s3_bucket: Option<String>,
    pub s3_prefix: Option<String>,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            mode: IngestionMode::LocalDir,
            corpus_dir: "./".to_string(),
            s3_bucket: None,
            s3_prefix: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub db_path: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            db_path: "index.bin".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DistributionConfig {
    pub channels: Vec<DistributionChannel>,
}

impl Default for DistributionConfig {
    fn default() -> Self {
        Self {
            channels: vec![DistributionChannel::Tcp],
        }
    }
}

impl DistributionConfig {
    pub fn uses_all_channels(&self) -> bool {
        self.channels.contains(&DistributionChannel::Tcp)
            && self.channels.contains(&DistributionChannel::Http)
            && self.channels.contains(&DistributionChannel::Grpc)
    }

    pub fn to_protocol_mode(&self) -> ProtocolMode {
        let has_tcp = self.channels.contains(&DistributionChannel::Tcp);
        let has_http = self.channels.contains(&DistributionChannel::Http);
        let has_grpc = self.channels.contains(&DistributionChannel::Grpc);

        match (has_tcp, has_http, has_grpc) {
            (true, false, false) => ProtocolMode::Tcp,
            (false, true, false) => ProtocolMode::Http,
            (false, false, true) => ProtocolMode::Grpc,
            (false, false, false) => ProtocolMode::Tcp,
            _ => ProtocolMode::Both,
        }
    }

    pub fn normalize(&mut self) {
        if self.channels.is_empty() {
            self.channels.push(DistributionChannel::Tcp);
        }

        let mut deduped = Vec::new();
        for channel in self.channels.drain(..) {
            if !deduped.contains(&channel) {
                deduped.push(channel);
            }
        }
        self.channels = deduped;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ManifestConfig {
    pub name: String,
    pub version: String,
    pub description: String,
}

impl Default for ManifestConfig {
    fn default() -> Self {
        Self {
            name: "zynsearch".to_string(),
            version: "1.0.0".to_string(),
            description: "Plug-and-play search engine".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CleanupConfig {
    pub enable_periodic_cleanup: bool,
    pub period_seconds: u64,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            enable_periodic_cleanup: true,
            period_seconds: 60,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub manifest: ManifestConfig,
    pub runtime: RuntimeConfig,
    pub ingestion: IngestionConfig,
    pub storage: StorageConfig,
    pub distribution: DistributionConfig,
    pub cleanup: CleanupConfig,
    pub env_path: Option<String>,
    pub config_path: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            manifest: ManifestConfig::default(),
            runtime: RuntimeConfig::default(),
            ingestion: IngestionConfig::default(),
            storage: StorageConfig::default(),
            distribution: DistributionConfig::default(),
            cleanup: CleanupConfig::default(),
            env_path: None,
            config_path: "zynsearch.config.json".to_string(),
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(name = "ZynSearch Daemon")]
#[command(version = "1.0.0")]
#[command(about = "A high-performance zero-copy TCP search engine", long_about = None)]
pub struct CliArgs {
    /// Path to a JSON config file.
    #[arg(long, env = "ZYN_CONFIG_PATH")]
    pub config: Option<String>,

    /// Path to an optional env file for local overrides.
    #[arg(long, env = "ZYN_ENV_PATH")]
    pub env_path: Option<String>,

    /// Override the host in the config file.
    #[arg(short = 'H', long, env = "ZYN_HOST")]
    pub host: Option<String>,

    /// Override the port in the config file.
    #[arg(short = 'P', long, env = "ZYN_PORT")]
    pub port: Option<u16>,

    /// Override the database path in the config file.
    #[arg(short = 'D', long, env = "ZYN_DB_PATH")]
    pub db_path: Option<String>,

    /// Override the corpus directory in the config file.
    #[arg(short = 'C', long, env = "ZYN_CORPUS_DIR")]
    pub corpus_dir: Option<String>,

    /// Override the output format in the config file.
    #[arg(short = 'F', long, env = "ZYN_FORMAT", value_enum)]
    pub output_format: Option<OutputFormat>,

    /// Override the transport protocol.
    #[arg(long, env = "ZYN_PROTOCOL", value_enum)]
    pub protocol: Option<ProtocolMode>,

    /// Choose the ingestion source type.
    #[arg(long, env = "ZYN_INGESTION", value_enum)]
    pub ingestion: Option<IngestionMode>,

    /// Add a distribution channel the client may use. Repeat to enable more than one.
    #[arg(long, env = "ZYN_DISTRIBUTION", value_enum, action = ArgAction::Append)]
    pub distribution: Vec<DistributionChannel>,

    /// S3 bucket name when ingestion is set to S3.
    #[arg(long, env = "ZYN_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    /// Optional S3 prefix when ingestion is set to S3.
    #[arg(long, env = "ZYN_S3_PREFIX")]
    pub s3_prefix: Option<String>,

    /// Optional one-shot query.
    #[arg(long, env = "ZYN_QUERY")]
    pub query: Option<String>,

    /// Enable periodic cleanup of missing/deleted files from the index.
    #[arg(long, env = "ZYN_CLEANUP_ENABLE")]
    pub enable_periodic_cleanup: Option<bool>,

    /// How frequently (seconds) to run periodic cleanup of missing/deleted files.
    #[arg(long, env = "ZYN_CLEANUP_INTERVAL")]
    pub cleanup_interval_seconds: Option<u64>,
}

pub fn load_app_config() -> Result<AppConfig, Box<dyn std::error::Error>> {
    let cli = CliArgs::parse();
    let config_path = cli
        .config
        .clone()
        .or_else(|| std::env::var("ZYN_CONFIG_PATH").ok())
        .unwrap_or_else(|| "zynsearch.config.json".to_string());

    let mut config = if Path::new(&config_path).exists() {
        load_json_config(&config_path)?
    } else {
        AppConfig::default()
    };

    if let Some(env_path) = cli.env_path.clone().or_else(|| std::env::var("ZYN_ENV_PATH").ok()) {
        config.env_path = Some(env_path);
    }

    apply_env_file_overrides(&mut config)?;
    apply_cli_overrides(&mut config, &cli, config_path);
    config.distribution.normalize();
    config.runtime.protocol = config.distribution.to_protocol_mode();

    Ok(config)
}

fn load_json_config(path: &str) -> Result<AppConfig, Box<dyn std::error::Error>> {
    let raw = fs::read_to_string(path)?;
    let mut config: AppConfig = serde_json::from_str(&raw)?;
    config.config_path = path.to_string();
    Ok(config)
}

fn apply_env_file_overrides(config: &mut AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let Some(env_path) = config.env_path.clone() else {
        return Ok(());
    };

    if !Path::new(&env_path).exists() {
        return Ok(());
    }

    let raw = fs::read_to_string(&env_path)?;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let Some((key, value)) = line.split_once('=') else {
            continue;
        };

        let key = key.trim();
        let value = value.trim().trim_matches('"');

        match key {
            "ZYN_HOST" => config.runtime.host = value.to_string(),
            "ZYN_PORT" => {
                if let Ok(port) = value.parse::<u16>() {
                    config.runtime.port = port;
                }
            }
            "ZYN_DB_PATH" => config.storage.db_path = value.to_string(),
            "ZYN_CORPUS_DIR" => config.ingestion.corpus_dir = value.to_string(),
            "ZYN_QUERY" => config.runtime.query = Some(value.to_string()),
            "ZYN_FORMAT" => {
                if let Ok(format) = serde_json::from_str::<OutputFormat>(&format!("\"{}\"", value)) {
                    config.runtime.output_format = format;
                }
            }
            "ZYN_PROTOCOL" => {
                if let Ok(protocol) = serde_json::from_str::<ProtocolMode>(&format!("\"{}\"", value)) {
                    config.runtime.protocol = protocol;
                }
            }
            "ZYN_INGESTION" => {
                if let Ok(ingestion) = serde_json::from_str::<IngestionMode>(&format!("\"{}\"", value)) {
                    config.ingestion.mode = ingestion;
                }
            }
            "ZYN_DISTRIBUTION" => {
                if let Ok(channel) = serde_json::from_str::<DistributionChannel>(&format!("\"{}\"", value)) {
                    if !config.distribution.channels.contains(&channel) {
                        config.distribution.channels.push(channel);
                    }
                }
            }
            "ZYN_S3_BUCKET" => config.ingestion.s3_bucket = Some(value.to_string()),
            "ZYN_S3_PREFIX" => config.ingestion.s3_prefix = Some(value.to_string()),
            "ZYN_CLEANUP_ENABLE" => {
                if let Ok(enable) = value.parse::<bool>() {
                    config.cleanup.enable_periodic_cleanup = enable;
                }
            }
            "ZYN_CLEANUP_INTERVAL" => {
                if let Ok(interval) = value.parse::<u64>() {
                    config.cleanup.period_seconds = interval;
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn apply_cli_overrides(config: &mut AppConfig, cli: &CliArgs, config_path: String) {
    config.config_path = config_path;

    if let Some(host) = cli.host.clone() {
        config.runtime.host = host;
    }
    if let Some(port) = cli.port {
        config.runtime.port = port;
    }
    if let Some(db_path) = cli.db_path.clone() {
        config.storage.db_path = db_path;
    }
    if let Some(corpus_dir) = cli.corpus_dir.clone() {
        config.ingestion.corpus_dir = corpus_dir;
    }
    if let Some(output_format) = cli.output_format {
        config.runtime.output_format = output_format;
    }
    if let Some(protocol) = cli.protocol {
        config.runtime.protocol = protocol;
    }
    if let Some(ingestion) = cli.ingestion {
        config.ingestion.mode = ingestion;
    }
    if !cli.distribution.is_empty() {
        config.distribution.channels = cli.distribution.clone();
    }
    if let Some(s3_bucket) = cli.s3_bucket.clone() {
        config.ingestion.s3_bucket = Some(s3_bucket);
    }
    if let Some(s3_prefix) = cli.s3_prefix.clone() {
        config.ingestion.s3_prefix = Some(s3_prefix);
    }
    if let Some(query) = cli.query.clone() {
        config.runtime.query = Some(query);
    }
    if let Some(enable) = cli.enable_periodic_cleanup {
        config.cleanup.enable_periodic_cleanup = enable;
    }
    if let Some(interval) = cli.cleanup_interval_seconds {
        config.cleanup.period_seconds = interval;
    }
}
