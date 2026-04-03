use anyhow::{Context, Result};
use chrono::Local;
use std::path::Path;
use std::time::Duration;
use tokio::runtime::Builder;
use url::Url;
use zenodo_rs::{
    AccessRight, Auth, Creator, DepositMetadataUpdate, DepositionId, Endpoint, FileReplacePolicy,
    RelatedIdentifier, UploadSpec, UploadType, ZenodoClient,
};

const ZENODO_API: &str = "https://zenodo.org/api";
const REPOSITORY_URL: &str = "https://github.com/earth-metabolome-initiative/classyfire";
const CLASSYFIRE_URL: &str = "http://classyfire.wishartlab.com";
const ZENODO_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

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
    Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build Tokio runtime for Zenodo publish")?
        .block_on(publish_async(
            token,
            output_path,
            manifest_path,
            stats,
            config,
        ))
}

async fn publish_async(
    token: &str,
    output_path: &Path,
    manifest_path: &Path,
    stats: PublishStats,
    config: &PublishConfig,
) -> Result<PublishedRelease> {
    let client = zenodo_client(token, config)?;
    let deposition_id = parse_deposition_id(&config.deposit_id)?;
    let current = client
        .get_deposition(deposition_id)
        .await
        .with_context(|| {
            format!(
                "failed to fetch Zenodo deposition {}; CLASSYFIRE_ZENODO_DEPOSIT_ID must be a Zenodo deposition id, not a public DOI record id",
                config.deposit_id
            )
        })?;

    if current.is_published() {
        eprintln!("[zenodo] creating new version...");
    } else {
        eprintln!("[zenodo] using existing unpublished draft...");
    }

    let metadata = build_metadata(stats)?;
    let draft = if current.is_published() {
        client
            .ensure_editable_draft(deposition_id)
            .await
            .context("failed to create editable Zenodo draft")?
    } else {
        current
    };

    let draft = client
        .update_metadata(draft.id, &metadata)
        .await
        .context("failed to update Zenodo metadata")?;

    eprintln!("[zenodo] uploading release artifacts...");
    client
        .reconcile_files(
            &draft,
            FileReplacePolicy::ReplaceAll,
            build_upload_specs(output_path, manifest_path)?,
        )
        .await
        .context("failed to upload Zenodo release artifacts")?;

    let published = client
        .publish(draft.id)
        .await
        .context("failed to publish Zenodo draft")?;

    let doi = published
        .doi
        .as_ref()
        .map_or_else(|| "unknown".to_owned(), ToString::to_string);

    let record_url = match published.record_id {
        Some(record_id) => {
            let record = client
                .get_record(record_id)
                .await
                .context("failed to fetch published Zenodo record")?;
            record
                .links
                .html
                .as_ref()
                .or(record.links.self_html.as_ref())
                .or(record.links.doi.as_ref())
                .map(|url| url.to_string())
                .or_else(|| {
                    if doi == "unknown" {
                        None
                    } else {
                        Some(format!("https://doi.org/{doi}"))
                    }
                })
                .unwrap_or_else(|| "unknown".to_owned())
        }
        None => {
            if doi == "unknown" {
                "unknown".to_owned()
            } else {
                format!("https://doi.org/{doi}")
            }
        }
    };

    Ok(PublishedRelease { doi, record_url })
}

fn zenodo_client(token: &str, config: &PublishConfig) -> Result<ZenodoClient> {
    ZenodoClient::builder(Auth::new(token))
        .endpoint(Endpoint::Custom(
            Url::parse(&config.api_base)
                .with_context(|| format!("invalid Zenodo API base URL `{}`", config.api_base))?,
        ))
        .user_agent(ZENODO_USER_AGENT)
        .connect_timeout(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(300))
        .build()
        .context("failed to build Zenodo HTTP client")
}

fn parse_deposition_id(deposit_id: &str) -> Result<DepositionId> {
    deposit_id
        .parse::<u64>()
        .map(DepositionId)
        .with_context(|| format!("invalid Zenodo deposition id `{deposit_id}`"))
}

fn build_upload_specs(output_path: &Path, manifest_path: &Path) -> Result<Vec<UploadSpec>> {
    Ok(vec![
        UploadSpec::from_path(output_path)
            .with_context(|| format!("failed to prepare {}", output_path.display()))?,
        UploadSpec::from_path(manifest_path)
            .with_context(|| format!("failed to prepare {}", manifest_path.display()))?,
    ])
}

fn build_metadata(stats: PublishStats) -> Result<DepositMetadataUpdate> {
    let today = Local::now().date_naive();
    let today_text = today.format("%Y-%m-%d").to_string();

    DepositMetadataUpdate::builder()
        .title(format!("ClassyFire PubChem Labels ({today_text})"))
        .upload_type(UploadType::Dataset)
        .publication_date(today)
        .description_html(format!(
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
        ))
        .creator(
            Creator::builder()
                .name("Cappelletti, Luca")
                .orcid("0000-0002-1269-2038")
                .build()
                .context("failed to build Zenodo creator metadata")?,
        )
        .access_right(AccessRight::Open)
        .license("MIT")
        .keyword("ClassyFire")
        .keyword("PubChem")
        .keyword("InChIKey")
        .keyword("chemical classification")
        .keyword("JSONL")
        .keyword("zstd")
        .keyword("open data")
        .keyword("cheminformatics")
        .related_identifier(build_related_identifier(
            REPOSITORY_URL,
            "isCompiledBy",
            Some("software"),
            Some("url"),
        )?)
        .related_identifier(build_related_identifier(
            CLASSYFIRE_URL,
            "isDerivedFrom",
            None,
            Some("url"),
        )?)
        .notes(format!(
            "Snapshot: {today_text}. Successful rows: {}. Misses: {}. Invalid rows: {}. Failed rows: {}.",
            stats.success, stats.miss, stats.invalid, stats.failed,
        ))
        .build()
        .context("failed to build Zenodo metadata")
}

fn build_related_identifier(
    identifier: &str,
    relation: &str,
    resource_type: Option<&str>,
    scheme: Option<&str>,
) -> Result<RelatedIdentifier> {
    let builder = RelatedIdentifier::builder()
        .identifier(identifier)
        .relation(relation);
    let builder = if let Some(resource_type) = resource_type {
        builder.resource_type(resource_type)
    } else {
        builder
    };
    let builder = if let Some(scheme) = scheme {
        builder.scheme(scheme)
    } else {
        builder
    };
    builder
        .build()
        .context("failed to build Zenodo related identifier metadata")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockResponse, MockServer};
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn metadata_mentions_jsonl_and_counts() {
        let metadata = build_metadata(PublishStats {
            success: 7,
            miss: 2,
            invalid: 1,
            failed: 3,
        })
        .unwrap();
        assert!(metadata.description_html.contains("JSONL.zst"));
        assert!(metadata.description_html.contains("Successful rows: 7"));
        assert!(metadata.notes.as_deref().unwrap().contains("Misses: 2"));
    }

    #[test]
    fn publishes_output_and_manifest_to_mock_zenodo() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("classyfire-labels.jsonl.zst");
        let manifest_path = temp_dir.path().join("manifest.json");
        std::fs::write(&output_path, b"output-bytes").unwrap();
        std::fs::write(&manifest_path, br#"{"manifest_version":1}"#).unwrap();

        let server = MockServer::with_builder(|base| {
            vec![
                (
                    "GET /api/deposit/depositions/999".to_owned(),
                    MockResponse::json(
                        200,
                        json!({
                            "id": 999,
                            "submitted": true,
                            "state": "done",
                            "metadata": {},
                            "files": [],
                            "links": {
                                "self": format!("{base}/api/deposit/depositions/999"),
                            },
                        })
                        .to_string(),
                    ),
                ),
                (
                    "POST /api/deposit/depositions/999/actions/newversion".to_owned(),
                    MockResponse::json(
                        200,
                        json!({
                            "id": 999,
                            "submitted": true,
                            "state": "done",
                            "metadata": {},
                            "files": [],
                            "links": {
                                "latest_draft": format!("{base}/api/deposit/depositions/123"),
                            },
                        })
                        .to_string(),
                    ),
                ),
                (
                    "GET /api/deposit/depositions/123".to_owned(),
                    MockResponse::json(
                        200,
                        json!({
                            "id": 123,
                            "submitted": false,
                            "state": "inprogress",
                            "metadata": {},
                            "files": [{ "id": "old-file", "filename": "stale.jsonl.zst", "filesize": 1 }],
                            "links": {
                                "bucket": format!("{base}/api/files/bucket"),
                            },
                        })
                        .to_string(),
                    ),
                ),
                (
                    "PUT /api/deposit/depositions/123".to_owned(),
                    MockResponse::json(
                        200,
                        json!({
                            "id": 123,
                            "submitted": false,
                            "state": "inprogress",
                            "metadata": {},
                            "files": [{ "id": "old-file", "filename": "stale.jsonl.zst", "filesize": 1 }],
                            "links": {
                                "bucket": format!("{base}/api/files/bucket"),
                            },
                        })
                        .to_string(),
                    ),
                ),
                (
                    "DELETE /api/deposit/depositions/123/files/old-file".to_owned(),
                    MockResponse::text(204, ""),
                ),
                (
                    "PUT /api/files/bucket/classyfire-labels.jsonl.zst".to_owned(),
                    MockResponse::json(
                        200,
                        json!({ "key": "classyfire-labels.jsonl.zst", "size": 12 }).to_string(),
                    ),
                ),
                (
                    "PUT /api/files/bucket/manifest.json".to_owned(),
                    MockResponse::json(
                        200,
                        json!({ "key": "manifest.json", "size": 22 }).to_string(),
                    ),
                ),
                (
                    "POST /api/deposit/depositions/123/actions/publish".to_owned(),
                    MockResponse::json(
                        200,
                        json!({
                            "id": 123,
                            "record_id": 123,
                            "doi": "10.5281/zenodo.123",
                            "submitted": true,
                            "state": "done",
                            "metadata": {},
                            "files": [],
                            "links": {},
                        })
                        .to_string(),
                    ),
                ),
                (
                    "GET /api/records/123".to_owned(),
                    MockResponse::json(
                        200,
                        json!({
                            "id": 123,
                            "recid": "123",
                            "doi": "10.5281/zenodo.123",
                            "metadata": { "title": "ClassyFire PubChem Labels" },
                            "files": [],
                            "links": {
                                "html": "https://zenodo.org/records/123",
                            },
                        })
                        .to_string(),
                    ),
                ),
            ]
        });

        let config = PublishConfig {
            api_base: format!("{}/api", server.url()),
            deposit_id: "999".to_owned(),
        };

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
            &config,
        )
        .unwrap();

        assert_eq!(published.doi, "10.5281/zenodo.123");
        assert_eq!(published.record_url, "https://zenodo.org/records/123");
        let requests = server.seen_requests();
        assert_eq!(requests.len(), 11);
        assert_eq!(requests[0].path, "/api/deposit/depositions/999");
        assert_eq!(requests[1].path, "/api/deposit/depositions/999");
        assert_eq!(
            requests[2].path,
            "/api/deposit/depositions/999/actions/newversion"
        );
        assert!(requests
            .iter()
            .any(|request| request.path == "/api/files/bucket/classyfire-labels.jsonl.zst"));
        assert!(requests
            .iter()
            .any(|request| request.path == "/api/files/bucket/manifest.json"));
    }

    #[test]
    fn publishes_existing_unsubmitted_draft_without_new_version() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("classyfire-labels.jsonl.zst");
        let manifest_path = temp_dir.path().join("manifest.json");
        std::fs::write(&output_path, b"output-bytes").unwrap();
        std::fs::write(&manifest_path, br#"{"manifest_version":1}"#).unwrap();

        let server = MockServer::with_builder(|base| {
            vec![
                (
                    "GET /api/deposit/depositions/999".to_owned(),
                    MockResponse::json(
                        200,
                        json!({
                            "id": 999,
                            "submitted": false,
                            "state": "inprogress",
                            "metadata": {},
                            "files": [{ "id": "old-file", "filename": "stale.jsonl.zst", "filesize": 1 }],
                            "links": {
                                "bucket": format!("{base}/api/files/bucket"),
                            },
                        })
                        .to_string(),
                    ),
                ),
                (
                    "PUT /api/deposit/depositions/999".to_owned(),
                    MockResponse::json(
                        200,
                        json!({
                            "id": 999,
                            "submitted": false,
                            "state": "inprogress",
                            "metadata": {},
                            "files": [{ "id": "old-file", "filename": "stale.jsonl.zst", "filesize": 1 }],
                            "links": {
                                "bucket": format!("{base}/api/files/bucket"),
                            },
                        })
                        .to_string(),
                    ),
                ),
                (
                    "DELETE /api/deposit/depositions/999/files/old-file".to_owned(),
                    MockResponse::text(204, ""),
                ),
                (
                    "PUT /api/files/bucket/classyfire-labels.jsonl.zst".to_owned(),
                    MockResponse::json(
                        200,
                        json!({ "key": "classyfire-labels.jsonl.zst", "size": 12 }).to_string(),
                    ),
                ),
                (
                    "PUT /api/files/bucket/manifest.json".to_owned(),
                    MockResponse::json(
                        200,
                        json!({ "key": "manifest.json", "size": 22 }).to_string(),
                    ),
                ),
                (
                    "POST /api/deposit/depositions/999/actions/publish".to_owned(),
                    MockResponse::json(
                        200,
                        json!({
                            "id": 999,
                            "record_id": 999,
                            "doi": "10.5281/zenodo.999",
                            "submitted": true,
                            "state": "done",
                            "metadata": {},
                            "files": [],
                            "links": {},
                        })
                        .to_string(),
                    ),
                ),
                (
                    "GET /api/records/999".to_owned(),
                    MockResponse::json(
                        200,
                        json!({
                            "id": 999,
                            "recid": "999",
                            "doi": "10.5281/zenodo.999",
                            "metadata": { "title": "ClassyFire PubChem Labels" },
                            "files": [],
                            "links": {
                                "html": "https://zenodo.org/records/999",
                            },
                        })
                        .to_string(),
                    ),
                ),
            ]
        });

        let config = PublishConfig {
            api_base: format!("{}/api", server.url()),
            deposit_id: "999".to_owned(),
        };

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
            &config,
        )
        .unwrap();

        assert_eq!(published.doi, "10.5281/zenodo.999");
        assert_eq!(published.record_url, "https://zenodo.org/records/999");
        let requests = server.seen_requests();
        assert_eq!(requests.len(), 8);
        assert_eq!(requests[0].path, "/api/deposit/depositions/999");
        assert!(requests
            .iter()
            .all(|request| request.path != "/api/deposit/depositions/999/actions/newversion"));
    }
}
