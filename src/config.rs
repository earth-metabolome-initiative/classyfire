use anyhow::bail;
use clap::{Args, Parser, Subcommand};
use std::env;
use std::path::PathBuf;

pub const DEFAULT_DB_PATH: &str = "/mnt/bfd/classyfire/classyfire.sqlite";
pub const DEFAULT_BASE_URL: &str = "http://classyfire.wishartlab.com";
pub const DEFAULT_USER_AGENT: &str = "classyfire-get-downloader/0.1";
pub const DEFAULT_IMPORT_CHUNK_SIZE: usize = 100000;
pub const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_GET_SLEEP_SECONDS: u64 = 5;
pub const DEFAULT_THROTTLE_BACKOFF_SECONDS: u64 = 300;
pub const DEFAULT_STATUS_INTERVAL_SECONDS: u64 = 1;

#[derive(Debug, Parser)]
#[command(name = "classyfire-crawler")]
#[command(about = "Lean GET-only ClassyFire downloader")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    ImportPubchem(ImportPubchemArgs),
    RunGet(DbArgs),
    Stats(DbArgs),
    ExportLabels(ExportLabelsArgs),
    RebuildCounters(DbArgs),
}

#[derive(Debug, Args, Clone)]
pub struct DbArgs {
    #[arg(long, env = "CLASSYFIRE_DB", default_value = DEFAULT_DB_PATH)]
    pub db: PathBuf,
}

#[derive(Debug, Args, Clone)]
pub struct ImportPubchemArgs {
    #[command(flatten)]
    pub db_args: DbArgs,
    #[arg(long)]
    pub input: PathBuf,
}

#[derive(Debug, Args, Clone)]
pub struct ExportLabelsArgs {
    #[command(flatten)]
    pub db_args: DbArgs,
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub db: PathBuf,
    pub base_url: String,
    pub user_agent: String,
    pub timeout_seconds: u64,
    pub get_sleep_seconds: u64,
    pub throttle_backoff_seconds: u64,
    pub status_interval_seconds: u64,
}

impl RuntimeConfig {
    pub fn from_env(db: PathBuf) -> Self {
        Self {
            db,
            base_url: env_string("CLASSYFIRE_BASE_URL", DEFAULT_BASE_URL),
            user_agent: env_string("CLASSYFIRE_USER_AGENT", DEFAULT_USER_AGENT),
            timeout_seconds: env_u64("CLASSYFIRE_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS),
            get_sleep_seconds: env_u64("CLASSYFIRE_GET_SLEEP_SECONDS", DEFAULT_GET_SLEEP_SECONDS),
            throttle_backoff_seconds: env_u64(
                "CLASSYFIRE_THROTTLE_BACKOFF_SECONDS",
                DEFAULT_THROTTLE_BACKOFF_SECONDS,
            ),
            status_interval_seconds: env_u64(
                "CLASSYFIRE_STATUS_INTERVAL_SECONDS",
                DEFAULT_STATUS_INTERVAL_SECONDS,
            ),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.get_sleep_seconds == 0 {
            bail!("CLASSYFIRE_GET_SLEEP_SECONDS must be greater than zero");
        }
        if self.status_interval_seconds == 0 {
            bail!("CLASSYFIRE_STATUS_INTERVAL_SECONDS must be greater than zero");
        }
        Ok(())
    }
}

fn env_string(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}
