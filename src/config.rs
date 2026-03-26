use anyhow::bail;
use clap::{Args, Parser, Subcommand};
use std::env;
use std::path::PathBuf;

pub const DEFAULT_BASE_URL: &str = "http://classyfire.wishartlab.com";
pub const DEFAULT_USER_AGENT: &str = "classyfire-get-downloader/0.1";
pub const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_GET_SLEEP_SECONDS: u64 = 5;
pub const DEFAULT_THROTTLE_BACKOFF_SECONDS: u64 = 300;
pub const DEFAULT_STATUS_INTERVAL_SECONDS: u64 = 1;
pub const DEFAULT_SUCCESS_SHARD_MAX_RECORDS: u64 = 100_000;
pub const DEFAULT_SUCCESS_SHARD_MAX_BYTES: u64 = 128 * 1024 * 1024;
pub const DEFAULT_NTFY_BASE_URL: &str = "https://ntfy.sh";

#[derive(Debug, Parser)]
#[command(name = "classyfire-crawler")]
#[command(about = "Lean GET-only ClassyFire downloader")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Run(StreamArgs),
    NotifyZenodoRelease(NotifyZenodoReleaseArgs),
}

#[derive(Debug, Args, Clone)]
pub struct StreamArgs {
    #[arg(long)]
    pub input: PathBuf,
    #[arg(long)]
    pub output_dir: PathBuf,
    #[arg(long, default_value_t = DEFAULT_SUCCESS_SHARD_MAX_RECORDS)]
    pub success_shard_max_records: u64,
    #[arg(long, default_value_t = DEFAULT_SUCCESS_SHARD_MAX_BYTES)]
    pub success_shard_max_bytes: u64,
}

#[derive(Debug, Args, Clone)]
pub struct NotifyZenodoReleaseArgs {
    #[arg(long)]
    pub output_dir: PathBuf,
    #[arg(long)]
    pub record_url: String,
}

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub input: PathBuf,
    pub output_dir: PathBuf,
    pub base_url: String,
    pub user_agent: String,
    pub timeout_seconds: u64,
    pub get_sleep_seconds: u64,
    pub throttle_backoff_seconds: u64,
    pub status_interval_seconds: u64,
    pub success_shard_max_records: u64,
    pub success_shard_max_bytes: u64,
    pub ntfy_base_url: String,
}

#[derive(Debug, Clone)]
pub struct NotifyZenodoReleaseConfig {
    pub output_dir: PathBuf,
    pub record_url: String,
    pub user_agent: String,
    pub timeout_seconds: u64,
    pub ntfy_base_url: String,
}

impl StreamConfig {
    pub fn from_env(args: StreamArgs) -> Self {
        Self {
            input: args.input,
            output_dir: args.output_dir,
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
            success_shard_max_records: args.success_shard_max_records,
            success_shard_max_bytes: args.success_shard_max_bytes,
            ntfy_base_url: env_string("CLASSYFIRE_NTFY_BASE_URL", DEFAULT_NTFY_BASE_URL),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.get_sleep_seconds == 0 {
            bail!("CLASSYFIRE_GET_SLEEP_SECONDS must be greater than zero");
        }
        if self.status_interval_seconds == 0 {
            bail!("CLASSYFIRE_STATUS_INTERVAL_SECONDS must be greater than zero");
        }
        if self.success_shard_max_records == 0 {
            bail!("--success-shard-max-records must be greater than zero");
        }
        if self.success_shard_max_bytes == 0 {
            bail!("--success-shard-max-bytes must be greater than zero");
        }
        Ok(())
    }
}

impl NotifyZenodoReleaseConfig {
    pub fn from_env(args: NotifyZenodoReleaseArgs) -> Self {
        Self {
            output_dir: args.output_dir,
            record_url: args.record_url,
            user_agent: env_string("CLASSYFIRE_USER_AGENT", DEFAULT_USER_AGENT),
            timeout_seconds: env_u64("CLASSYFIRE_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS),
            ntfy_base_url: env_string("CLASSYFIRE_NTFY_BASE_URL", DEFAULT_NTFY_BASE_URL),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.record_url.trim().is_empty() {
            bail!("--record-url must not be empty");
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct EnvGuard {
        saved: Vec<(&'static str, Option<OsString>)>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, &str)]) -> Self {
            let saved = vars
                .iter()
                .map(|(key, _)| (*key, env::var_os(key)))
                .collect::<Vec<_>>();
            for (key, value) in vars {
                env::set_var(key, value);
            }
            Self { saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.saved.drain(..) {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
    }

    #[test]
    fn parses_run_cli_args() {
        let cli = Cli::try_parse_from([
            "classyfire-crawler",
            "run",
            "--input",
            "input.tsv.zst",
            "--output-dir",
            "run-dir",
        ])
        .unwrap();

        match cli.command {
            Commands::Run(args) => {
                assert_eq!(args.input, PathBuf::from("input.tsv.zst"));
                assert_eq!(args.output_dir, PathBuf::from("run-dir"));
                assert_eq!(
                    args.success_shard_max_records,
                    DEFAULT_SUCCESS_SHARD_MAX_RECORDS
                );
                assert_eq!(
                    args.success_shard_max_bytes,
                    DEFAULT_SUCCESS_SHARD_MAX_BYTES
                );
            }
            Commands::NotifyZenodoRelease(_) => panic!("expected run command"),
        }
    }

    #[test]
    fn parses_notify_zenodo_release_cli_args() {
        let cli = Cli::try_parse_from([
            "classyfire-crawler",
            "notify-zenodo-release",
            "--output-dir",
            "run-dir",
            "--record-url",
            "https://zenodo.org/records/123",
        ])
        .unwrap();

        match cli.command {
            Commands::NotifyZenodoRelease(args) => {
                assert_eq!(args.output_dir, PathBuf::from("run-dir"));
                assert_eq!(args.record_url, "https://zenodo.org/records/123");
            }
            Commands::Run(_) => panic!("expected notify-zenodo-release command"),
        }
    }

    #[test]
    fn stream_config_uses_environment_overrides() {
        let _lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _env = EnvGuard::set(&[
            ("CLASSYFIRE_BASE_URL", "http://127.0.0.1:9999"),
            ("CLASSYFIRE_USER_AGENT", "classyfire-test/0.1"),
            ("CLASSYFIRE_TIMEOUT_SECONDS", "9"),
            ("CLASSYFIRE_GET_SLEEP_SECONDS", "7"),
            ("CLASSYFIRE_THROTTLE_BACKOFF_SECONDS", "11"),
            ("CLASSYFIRE_STATUS_INTERVAL_SECONDS", "13"),
            ("CLASSYFIRE_NTFY_BASE_URL", "https://ntfy.example"),
        ]);

        let config = StreamConfig::from_env(StreamArgs {
            input: PathBuf::from("input.tsv"),
            output_dir: PathBuf::from("out"),
            success_shard_max_records: 42,
            success_shard_max_bytes: 99,
        });

        assert_eq!(config.base_url, "http://127.0.0.1:9999");
        assert_eq!(config.user_agent, "classyfire-test/0.1");
        assert_eq!(config.timeout_seconds, 9);
        assert_eq!(config.get_sleep_seconds, 7);
        assert_eq!(config.throttle_backoff_seconds, 11);
        assert_eq!(config.status_interval_seconds, 13);
        assert_eq!(config.success_shard_max_records, 42);
        assert_eq!(config.success_shard_max_bytes, 99);
        assert_eq!(config.ntfy_base_url, "https://ntfy.example");
    }

    #[test]
    fn notify_release_config_uses_environment_overrides() {
        let _lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _env = EnvGuard::set(&[
            ("CLASSYFIRE_USER_AGENT", "classyfire-test/0.1"),
            ("CLASSYFIRE_TIMEOUT_SECONDS", "9"),
            ("CLASSYFIRE_NTFY_BASE_URL", "https://ntfy.example"),
        ]);

        let config = NotifyZenodoReleaseConfig::from_env(NotifyZenodoReleaseArgs {
            output_dir: PathBuf::from("out"),
            record_url: "https://zenodo.org/records/123".to_owned(),
        });

        assert_eq!(config.output_dir, PathBuf::from("out"));
        assert_eq!(config.record_url, "https://zenodo.org/records/123");
        assert_eq!(config.user_agent, "classyfire-test/0.1");
        assert_eq!(config.timeout_seconds, 9);
        assert_eq!(config.ntfy_base_url, "https://ntfy.example");
    }

    #[test]
    fn validate_rejects_zero_get_sleep() {
        let mut config = StreamConfig::from_env(StreamArgs {
            input: PathBuf::from("input.tsv"),
            output_dir: PathBuf::from("out"),
            success_shard_max_records: 1,
            success_shard_max_bytes: 1,
        });
        config.get_sleep_seconds = 0;
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("CLASSYFIRE_GET_SLEEP_SECONDS"));
    }

    #[test]
    fn validate_rejects_zero_status_interval() {
        let mut config = StreamConfig::from_env(StreamArgs {
            input: PathBuf::from("input.tsv"),
            output_dir: PathBuf::from("out"),
            success_shard_max_records: 1,
            success_shard_max_bytes: 1,
        });
        config.status_interval_seconds = 0;
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("CLASSYFIRE_STATUS_INTERVAL_SECONDS"));
    }

    #[test]
    fn validate_rejects_zero_shard_limits() {
        let mut config = StreamConfig::from_env(StreamArgs {
            input: PathBuf::from("input.tsv"),
            output_dir: PathBuf::from("out"),
            success_shard_max_records: 1,
            success_shard_max_bytes: 1,
        });
        config.success_shard_max_records = 0;
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("--success-shard-max-records"));

        config.success_shard_max_records = 1;
        config.success_shard_max_bytes = 0;
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("--success-shard-max-bytes"));
    }

    #[test]
    fn notify_release_validate_rejects_empty_record_url() {
        let config = NotifyZenodoReleaseConfig::from_env(NotifyZenodoReleaseArgs {
            output_dir: PathBuf::from("out"),
            record_url: "   ".to_owned(),
        });
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("--record-url"));
    }
}
