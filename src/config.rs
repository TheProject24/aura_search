use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::{fs, path::{Path, PathBuf}};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub db_path: String,
    pub corpus_dir: String,
    pub query: Option<String>,
    pub output_format: OutputFormat,
    pub protocol: ProtocolMode,
    pub ingestion: IngestionMode,
    pub env_path: Option<String>,
    pub config_path: String,
    pub s3_bucket: Option<String>,
    pub s3_prefix: Option<String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 7777,
            db_path: "index.bin".to_string(),
            corpus_dir: "./".to_string(),
            query: None,
            output_format: OutputFormat::Text,
            protocol: ProtocolMode::Tcp,
            ingestion: IngestionMode::LocalDir,
            env_path: None,
            config_path: "zynsearch.config.json".to_string(),
            s3_bucket: None,
            s3_prefix: None,
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

    /// S3 bucket name when ingestion is set to S3.
    #[arg(long, env = "ZYN_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    /// Optional S3 prefix when ingestion is set to S3.
    #[arg(long, env = "ZYN_S3_PREFIX")]
    pub s3_prefix: Option<String>,

    /// Optional one-shot query.
    #[arg(long, env = "ZYN_QUERY")]
    pub query: Option<String>,
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
            "ZYN_HOST" => config.host = value.to_string(),
            "ZYN_PORT" => {
                if let Ok(port) = value.parse::<u16>() {
                    config.port = port;
                }
            }
            "ZYN_DB_PATH" => config.db_path = value.to_string(),
            "ZYN_CORPUS_DIR" => config.corpus_dir = value.to_string(),
            "ZYN_QUERY" => config.query = Some(value.to_string()),
            "ZYN_FORMAT" => {
                if let Ok(format) = serde_json::from_str::<OutputFormat>(&format!("\"{}\"", value)) {
                    config.output_format = format;
                }
            }
            "ZYN_PROTOCOL" => {
                if let Ok(protocol) = serde_json::from_str::<ProtocolMode>(&format!("\"{}\"", value)) {
                    config.protocol = protocol;
                }
            }
            "ZYN_INGESTION" => {
                if let Ok(ingestion) = serde_json::from_str::<IngestionMode>(&format!("\"{}\"", value)) {
                    config.ingestion = ingestion;
                }
            }
            "ZYN_S3_BUCKET" => config.s3_bucket = Some(value.to_string()),
            "ZYN_S3_PREFIX" => config.s3_prefix = Some(value.to_string()),
            _ => {}
        }
    }

    Ok(())
}

fn apply_cli_overrides(config: &mut AppConfig, cli: &CliArgs, config_path: String) {
    config.config_path = config_path;

    if let Some(host) = cli.host.clone() {
        config.host = host;
    }
    if let Some(port) = cli.port {
        config.port = port;
    }
    if let Some(db_path) = cli.db_path.clone() {
        config.db_path = db_path;
    }
    if let Some(corpus_dir) = cli.corpus_dir.clone() {
        config.corpus_dir = corpus_dir;
    }
    if let Some(output_format) = cli.output_format {
        config.output_format = output_format;
    }
    if let Some(protocol) = cli.protocol {
        config.protocol = protocol;
    }
    if let Some(ingestion) = cli.ingestion {
        config.ingestion = ingestion;
    }
    if let Some(s3_bucket) = cli.s3_bucket.clone() {
        config.s3_bucket = Some(s3_bucket);
    }
    if let Some(s3_prefix) = cli.s3_prefix.clone() {
        config.s3_prefix = Some(s3_prefix);
    }
    if let Some(query) = cli.query.clone() {
        config.query = Some(query);
    }
}

pub fn load_default_config_file(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if path.exists() {
        return Ok(());
    }

    let default_config = AppConfig::default();
    let contents = serde_json::to_string_pretty(&default_config)?;
    fs::write(path, contents)?;
    Ok(())
}

pub fn config_path_from_cli(cli: &CliArgs) -> PathBuf {
    cli.config
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("zynsearch.config.json"))
}
