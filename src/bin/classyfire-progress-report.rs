use anyhow::Result;
use clap::{ArgGroup, Parser};
use classyfire_crawler::config::DEFAULT_USER_AGENT;
use classyfire_crawler::progress::{
    build_local_progress_summary, load_or_download_zenodo_progress_summary, load_progress_summary,
    render_progress_svg, DEFAULT_ZENODO_DOI,
};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "classyfire-progress-report")]
#[command(about = "Render a single SVG progress report for a local crawl or a Zenodo release")]
#[command(group(
    ArgGroup::new("source")
        .required(true)
        .args(["output_dir", "summary_json", "zenodo_doi"])
))]
struct Cli {
    #[arg(long)]
    output_dir: Option<PathBuf>,
    #[arg(long)]
    summary_json: Option<PathBuf>,
    #[arg(long, num_args = 0..=1, default_missing_value = DEFAULT_ZENODO_DOI)]
    zenodo_doi: Option<String>,
    #[arg(long, default_value = "classyfire-progress.svg")]
    output: PathBuf,
    #[arg(long)]
    pubchem_total_rows: Option<u64>,
    #[arg(long)]
    cache_dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let (summary, source_label) = load_summary(&cli)?;

    render_progress_svg(&cli.output, &summary, &source_label)?;
    println!("{}", cli.output.display());
    Ok(())
}

fn load_summary(cli: &Cli) -> Result<(classyfire_crawler::progress::ProgressSummary, String)> {
    if let Some(output_dir) = &cli.output_dir {
        return Ok((
            build_local_progress_summary(output_dir, cli.pubchem_total_rows)?,
            format!("Local crawl state from {}", output_dir.display()),
        ));
    }

    if let Some(summary_json) = &cli.summary_json {
        return Ok((
            load_progress_summary(summary_json)?,
            format!("Existing summary from {}", summary_json.display()),
        ));
    }

    let doi = cli
        .zenodo_doi
        .as_deref()
        .unwrap_or(DEFAULT_ZENODO_DOI)
        .to_owned();
    let cache_dir = cli
        .cache_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("classyfire-progress-report"));
    Ok((
        load_or_download_zenodo_progress_summary(
            &doi,
            &cache_dir,
            cli.pubchem_total_rows,
            DEFAULT_USER_AGENT,
        )?,
        format!("Latest Zenodo release resolved from DOI {doi}"),
    ))
}
