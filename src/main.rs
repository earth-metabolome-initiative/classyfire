use anyhow::Result;
use clap::Parser;
use classyfire_crawler::config::{Cli, Commands, StreamConfig};
use classyfire_crawler::streaming::run_streaming;

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    match cli.command {
        Commands::Run(args) => {
            run_streaming(StreamConfig::from_env(args))?;
        }
    }
    Ok(())
}
