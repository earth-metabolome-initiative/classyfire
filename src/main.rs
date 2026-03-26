use anyhow::Result;
use clap::Parser;
use classyfire_crawler::config::{Cli, Commands, NotifyZenodoReleaseConfig, StreamConfig};
use classyfire_crawler::streaming::{notify_zenodo_release, run_streaming};

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    match cli.command {
        Commands::Run(args) => {
            run_streaming(StreamConfig::from_env(args))?;
        }
        Commands::NotifyZenodoRelease(args) => {
            notify_zenodo_release(NotifyZenodoReleaseConfig::from_env(args))?;
        }
    }
    Ok(())
}
