use crate::db::StatsSnapshot;
use anyhow::{anyhow, Context, Result};
use reqwest::blocking::{Body, Client};
use serde_json::Value;
use std::fs::File;
use std::path::Path;
use std::time::Duration;

const ZENODO_API: &str = "https://zenodo.org/api";
const REPOSITORY_URL: &str = "https://github.com/LucaCappelletti94/classyfire";
const CLASSYFIRE_URL: &str = "http://classyfire.wishartlab.com";

pub fn publish(
    token: &str,
    deposit_id: &str,
    parquet_path: &Path,
    stats: &StatsSnapshot,
) -> Result<String> {
    let client = zenodo_client()?;

    eprintln!("[zenodo] creating new version...");
    let new_version: Value = client
        .post(format!(
            "{ZENODO_API}/deposit/depositions/{deposit_id}/actions/newversion"
        ))
        .bearer_auth(token)
        .send()
        .context("failed to create new version")?
        .error_for_status()
        .context("Zenodo rejected new version request")?
        .json()
        .context("failed to parse new version response")?;

    let draft_url = new_version["links"]["latest_draft"]
        .as_str()
        .ok_or_else(|| anyhow!("missing latest_draft link in new version response"))?;

    let draft: Value = client
        .get(draft_url)
        .bearer_auth(token)
        .send()
        .context("failed to fetch latest draft")?
        .error_for_status()
        .context("Zenodo rejected latest draft fetch")?
        .json()
        .context("failed to parse draft response")?;

    let bucket_url = draft["links"]["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket link in draft"))?;
    let draft_id = draft["id"]
        .as_u64()
        .ok_or_else(|| anyhow!("missing draft id in draft response"))?;

    if let Some(files) = draft["files"].as_array() {
        for file in files {
            if let Some(file_id) = file["id"].as_str() {
                client
                    .delete(format!(
                        "{ZENODO_API}/deposit/depositions/{draft_id}/files/{file_id}"
                    ))
                    .bearer_auth(token)
                    .send()
                    .context("failed to delete old draft file")?
                    .error_for_status()
                    .context("Zenodo rejected old draft file deletion")?;
            }
        }
    }

    let metadata = build_metadata(stats);
    client
        .put(format!("{ZENODO_API}/deposit/depositions/{draft_id}"))
        .bearer_auth(token)
        .json(&metadata)
        .send()
        .context("failed to update Zenodo metadata")?
        .error_for_status()
        .context("Zenodo rejected metadata update")?;

    let filename = parquet_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("classyfire-labels.parquet");
    let file_size = std::fs::metadata(parquet_path)
        .with_context(|| format!("cannot stat {}", parquet_path.display()))?
        .len();
    eprintln!(
        "[zenodo] uploading {filename} ({:.1} MB)...",
        file_size as f64 / 1_048_576.0
    );

    let file = File::open(parquet_path)
        .with_context(|| format!("failed to open {}", parquet_path.display()))?;
    client
        .put(format!("{bucket_url}/{filename}"))
        .bearer_auth(token)
        .header("Content-Type", "application/octet-stream")
        .body(Body::new(file))
        .send()
        .context("failed to upload parquet to Zenodo")?
        .error_for_status()
        .context("Zenodo rejected parquet upload")?;

    let published: Value = client
        .post(format!(
            "{ZENODO_API}/deposit/depositions/{draft_id}/actions/publish"
        ))
        .bearer_auth(token)
        .send()
        .context("failed to publish Zenodo draft")?
        .error_for_status()
        .context("Zenodo rejected publish request")?
        .json()
        .context("failed to parse publish response")?;

    let doi = published["doi"].as_str().unwrap_or("unknown").to_owned();
    eprintln!("[zenodo] published: DOI {doi}");
    Ok(doi)
}

fn zenodo_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(300))
        .build()
        .context("failed to build Zenodo HTTP client")
}

fn build_metadata(stats: &StatsSnapshot) -> Value {
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let terminal = stats.done_count + stats.miss_count;
    let pct = terminal as f64 / stats.total_molecules.max(1) as f64 * 100.0;

    serde_json::json!({
        "metadata": {
            "title": format!("ClassyFire PubChem Labels ({today})"),
            "upload_type": "dataset",
            "publication_date": today,
            "description": format!(
                "<p>Open dataset of recovered <a href=\"{CLASSYFIRE_URL}\">ClassyFire</a> labels \
                 for unique PubChem InChIKeys.</p>\
                 <p>This snapshot contains {done} labeled molecules exported from a local crawl \
                 over {total} unique PubChem InChIKeys. The crawl has also observed {miss} keys \
                 with no classification and {error} unresolved transient failures. \
                 Terminal outcomes so far cover {terminal}/{total} keys ({pct:.1}%).</p>\
                 <p>The Parquet file contains one row per labeled InChIKey with the normalized \
                 InChI, the main ClassyFire taxonomy labels, and the full raw entity JSON as a \
                 UTF-8 string column.</p>\
                 <p>Format: Apache Parquet with Zstandard compression. Updated weekly. Source code: \
                 <a href=\"{REPOSITORY_URL}\">classyfire</a>.</p>",
                done = stats.done_count,
                total = stats.total_molecules,
                miss = stats.miss_count,
                error = stats.error_count,
                terminal = terminal,
                pct = pct,
            ),
            "creators": [
                {
                    "name": "Cappelletti, Luca",
                    "orcid": "0000-0002-1269-2038"
                }
            ],
            "keywords": [
                "ClassyFire",
                "PubChem",
                "InChIKey",
                "cheminformatics",
                "chemical classification",
                "open data",
                "machine learning dataset",
                "parquet"
            ],
            "license": "MIT",
            "access_right": "open",
            "related_identifiers": [
                {
                    "identifier": REPOSITORY_URL,
                    "relation": "isCompiledBy",
                    "resource_type": "software",
                    "scheme": "url"
                },
                {
                    "identifier": CLASSYFIRE_URL,
                    "relation": "isDerivedFrom",
                    "scheme": "url"
                }
            ],
            "notes": format!(
                "Snapshot: {today}. Labeled rows exported: {}. Misses: {}. Errors: {}. Total unique InChIKeys: {}.",
                stats.done_count,
                stats.miss_count,
                stats.error_count,
                stats.total_molecules,
            )
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_mentions_counts_and_format() {
        let stats = StatsSnapshot {
            total_molecules: 100,
            new_count: 80,
            done_count: 10,
            miss_count: 5,
            error_count: 5,
        };
        let metadata = build_metadata(&stats);
        let description = metadata["metadata"]["description"].as_str().unwrap();
        let notes = metadata["metadata"]["notes"].as_str().unwrap();

        assert!(description.contains("10 labeled molecules"));
        assert!(description.contains("5 keys with no classification"));
        assert!(description.contains("Apache Parquet"));
        assert!(notes.contains("Labeled rows exported: 10"));
    }
}
