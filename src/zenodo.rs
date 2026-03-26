use anyhow::{anyhow, Context, Result};
use reqwest::blocking::{Body, Client};
use serde_json::{json, Value};
use std::fs::File;
use std::path::Path;
use std::time::Duration;

const ZENODO_API: &str = "https://zenodo.org/api";
const REPOSITORY_URL: &str = "https://github.com/earth-metabolome-initiative/classyfire";
const CLASSYFIRE_URL: &str = "http://classyfire.wishartlab.com";

#[derive(Debug, Clone, Copy)]
pub struct PublishStats {
    pub success: u64,
    pub miss: u64,
    pub invalid: u64,
    pub failed: u64,
}

impl PublishStats {
    fn handled(self) -> u64 {
        self.success + self.miss + self.invalid + self.failed
    }
}

#[derive(Debug, Clone)]
pub struct PublishedRelease {
    pub doi: String,
    pub record_url: String,
}

#[derive(Clone)]
struct PublishConfig {
    api_base: String,
    deposit_id: String,
}

impl PublishConfig {
    fn production(deposit_id: &str) -> Self {
        Self {
            api_base: ZENODO_API.to_owned(),
            deposit_id: deposit_id.to_owned(),
        }
    }
}

pub fn publish(
    token: &str,
    deposit_id: &str,
    output_path: &Path,
    manifest_path: &Path,
    stats: PublishStats,
) -> Result<PublishedRelease> {
    publish_with_config(
        token,
        output_path,
        manifest_path,
        stats,
        &PublishConfig::production(deposit_id),
    )
}

fn publish_with_config(
    token: &str,
    output_path: &Path,
    manifest_path: &Path,
    stats: PublishStats,
    config: &PublishConfig,
) -> Result<PublishedRelease> {
    let client = zenodo_client()?;

    eprintln!("[zenodo] creating new version...");
    let new_version: Value = client
        .post(format!(
            "{}/deposit/depositions/{}/actions/newversion",
            config.api_base, config.deposit_id
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
    let draft_url = resolve_link(&config.api_base, draft_url);

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
    let bucket_url = resolve_link(&config.api_base, bucket_url);
    let draft_id = draft["id"]
        .as_u64()
        .ok_or_else(|| anyhow!("missing draft id in draft response"))?;

    if let Some(files) = draft["files"].as_array() {
        for file in files {
            if let Some(file_id) = file["id"].as_str() {
                let _ = client
                    .delete(format!(
                        "{}/deposit/depositions/{draft_id}/files/{file_id}",
                        config.api_base
                    ))
                    .bearer_auth(token)
                    .send();
            }
        }
    }

    client
        .put(format!(
            "{}/deposit/depositions/{draft_id}",
            config.api_base
        ))
        .bearer_auth(token)
        .json(&build_metadata(stats))
        .send()
        .context("failed to update Zenodo metadata")?
        .error_for_status()
        .context("Zenodo rejected metadata update")?;

    upload_file(&client, token, &bucket_url, output_path)?;
    upload_file(&client, token, &bucket_url, manifest_path)?;

    let published: Value = client
        .post(format!(
            "{}/deposit/depositions/{draft_id}/actions/publish",
            config.api_base
        ))
        .bearer_auth(token)
        .send()
        .context("failed to publish Zenodo draft")?
        .error_for_status()
        .context("Zenodo rejected publish request")?
        .json()
        .context("failed to parse publish response")?;

    let doi = published["doi"].as_str().unwrap_or("unknown").to_owned();
    let record_url = published["links"]["html"]
        .as_str()
        .or_else(|| published["links"]["record_html"].as_str())
        .map(str::to_owned)
        .or_else(|| {
            if doi == "unknown" {
                None
            } else {
                Some(format!("https://doi.org/{doi}"))
            }
        })
        .unwrap_or_else(|| "unknown".to_owned());
    Ok(PublishedRelease { doi, record_url })
}

fn zenodo_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(300))
        .build()
        .context("failed to build Zenodo HTTP client")
}

fn resolve_link(api_base: &str, link: &str) -> String {
    if link.starts_with("http://") || link.starts_with("https://") {
        return link.to_owned();
    }
    if link.starts_with('/') {
        format!("{}{}", api_base.trim_end_matches('/'), link)
    } else {
        format!("{}/{}", api_base.trim_end_matches('/'), link)
    }
}

fn build_metadata(stats: PublishStats) -> Value {
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();

    json!({
        "metadata": {
            "title": format!("ClassyFire PubChem Labels ({today})"),
            "upload_type": "dataset",
            "publication_date": today,
            "description": format!(
                "<p>Open dataset of recovered <a href=\"{CLASSYFIRE_URL}\">ClassyFire</a> labels for PubChem compounds.</p>\
                 <p>This snapshot contains {success} successful ClassyFire responses exported as a single merged <code>JSONL.zst</code> file. \
                 Rows that ended as misses ({miss}), invalid input ({invalid}), or failed requests ({failed}) are excluded from the release artifact.</p>\
                 <p>The release contains a merged <code>classyfire-labels.jsonl.zst</code> dataset and a machine-readable <code>manifest.json</code> describing the published snapshot.</p>\
                 <p>Handled rows so far: {handled}. Successful rows: {success}. Misses: {miss}. Invalid rows: {invalid}. Failed rows: {failed}.</p>\
                 <p>Format: JSON Lines compressed with Zstandard. Updated periodically from the streaming crawler.</p>\
                 <p>Source code: <a href=\"{REPOSITORY_URL}\">classyfire</a>.</p>",
                handled = stats.handled(),
                success = stats.success,
                miss = stats.miss,
                invalid = stats.invalid,
                failed = stats.failed,
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
                "chemical classification",
                "JSONL",
                "zstd",
                "open data",
                "cheminformatics"
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
                "Snapshot: {today}. Successful rows: {}. Misses: {}. Invalid rows: {}. Failed rows: {}.",
                stats.success,
                stats.miss,
                stats.invalid,
                stats.failed,
            )
        }
    })
}

fn upload_file(client: &Client, token: &str, bucket_url: &str, path: &Path) -> Result<()> {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("invalid filename for {}", path.display()))?;
    let file_size = std::fs::metadata(path)
        .with_context(|| format!("cannot stat {}", path.display()))?
        .len();
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;

    eprintln!(
        "[zenodo] uploading {filename} ({:.1} MB)...",
        file_size as f64 / 1_048_576.0
    );

    client
        .put(format!("{bucket_url}/{filename}"))
        .bearer_auth(token)
        .header("Content-Type", "application/octet-stream")
        .body(Body::new(file))
        .send()
        .with_context(|| format!("failed to upload {filename} to Zenodo"))?
        .error_for_status()
        .with_context(|| format!("Zenodo rejected upload for {filename}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::MockServer;
    use tempfile::tempdir;

    #[test]
    fn metadata_mentions_jsonl_and_counts() {
        let metadata = build_metadata(PublishStats {
            success: 7,
            miss: 2,
            invalid: 1,
            failed: 3,
        });
        let description = metadata["metadata"]["description"].as_str().unwrap();
        let notes = metadata["metadata"]["notes"].as_str().unwrap();
        assert!(description.contains("JSONL.zst"));
        assert!(description.contains("Successful rows: 7"));
        assert!(notes.contains("Misses: 2"));
    }

    #[test]
    fn publishes_output_and_manifest_to_mock_zenodo() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("classyfire-labels.jsonl.zst");
        let manifest_path = temp_dir.path().join("manifest.json");
        std::fs::write(&output_path, b"output-bytes").unwrap();
        std::fs::write(&manifest_path, br#"{"manifest_version":1}"#).unwrap();

        let server = MockServer::new([
            (
                "/deposit/depositions/999/actions/newversion",
                crate::test_support::MockResponse::json(
                    200,
                    r#"{"links":{"latest_draft":"/draft"}}"#,
                ),
            ),
            (
                "/draft",
                crate::test_support::MockResponse::json(
                    200,
                    r#"{"links":{"bucket":"/bucket"},"id":123,"files":[{"id":"old-file"}]}"#,
                ),
            ),
            (
                "/deposit/depositions/123/files/old-file",
                crate::test_support::MockResponse::text(204, ""),
            ),
            (
                "/deposit/depositions/123",
                crate::test_support::MockResponse::json(200, r#"{"updated":"metadata"}"#),
            ),
            (
                "/bucket/classyfire-labels.jsonl.zst",
                crate::test_support::MockResponse::json(200, r#"{"uploaded":"output"}"#),
            ),
            (
                "/bucket/manifest.json",
                crate::test_support::MockResponse::json(200, r#"{"uploaded":"manifest"}"#),
            ),
            (
                "/deposit/depositions/123/actions/publish",
                crate::test_support::MockResponse::json(
                    200,
                    r#"{"doi":"10.5281/zenodo.123","links":{"html":"https://zenodo.org/records/123"}}"#,
                ),
            ),
        ]);

        let published = publish_with_config(
            "token",
            &output_path,
            &manifest_path,
            PublishStats {
                success: 7,
                miss: 2,
                invalid: 1,
                failed: 3,
            },
            &PublishConfig {
                api_base: server.url(),
                deposit_id: "999".to_owned(),
            },
        )
        .unwrap();

        assert_eq!(published.doi, "10.5281/zenodo.123");
        assert_eq!(published.record_url, "https://zenodo.org/records/123");
        let requests = server.seen_requests();
        assert_eq!(requests.len(), 7);
        assert!(requests
            .iter()
            .any(|request| request.path == "/bucket/classyfire-labels.jsonl.zst"));
        assert!(requests
            .iter()
            .any(|request| request.path == "/bucket/manifest.json"));
    }
}
