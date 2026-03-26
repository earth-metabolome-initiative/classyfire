use anyhow::{bail, Context, Result};
use reqwest::blocking::Client;
use serde::de::IgnoredAny;
use serde::Deserialize;

const HTML_MARKERS: [&str; 4] = ["<!doctype html", "<html", "<head", "unsupported browser"];

#[derive(Debug, Clone)]
pub struct HttpBody {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: String,
}

#[derive(Debug, Clone)]
pub struct ApiClient {
    base_url: String,
    client: Client,
    timeout: std::time::Duration,
}

impl ApiClient {
    pub fn new(
        base_url: impl Into<String>,
        user_agent: &str,
        timeout_seconds: u64,
    ) -> Result<Self> {
        let client = Client::builder().user_agent(user_agent).build()?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            client,
            timeout: std::time::Duration::from_secs(timeout_seconds.max(1)),
        })
    }

    pub fn fetch_entity(&self, inchikey: &str) -> Result<HttpBody> {
        let response = self
            .client
            .get(format!("{}/entities/{}.json", self.base_url, inchikey))
            .timeout(self.timeout)
            .send()
            .context("failed GET /entities request")?;
        http_body_from_response(response)
    }
}

pub fn validate_entity_body(body: &HttpBody) -> Result<bool> {
    if body.status == 429 {
        bail!("entity request was throttled");
    }
    if body_looks_html(body.content_type.as_deref(), &body.body) {
        bail!(
            "entity request returned HTML ({})",
            summarize_html_body(&body.body)
        );
    }
    if body.status == 404 || body_looks_not_found(&body.body) {
        return Ok(false);
    }
    if body.status >= 400 {
        bail!("entity request failed with status {}", body.status);
    }
    if body.body.trim().is_empty() {
        return Ok(false);
    }

    let probe: ClassificationProbe =
        serde_json::from_str(&body.body).context("failed to parse entity response JSON")?;
    Ok(probe.has_classification())
}

fn http_body_from_response(response: reqwest::blocking::Response) -> Result<HttpBody> {
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response.text().context("failed reading response body")?;
    Ok(HttpBody {
        status,
        content_type,
        body,
    })
}

fn body_looks_html(content_type: Option<&str>, body: &str) -> bool {
    let normalized_content_type = content_type.unwrap_or_default().to_ascii_lowercase();
    if normalized_content_type.contains("text/html") {
        return true;
    }

    let body_prefix = body
        .chars()
        .take(256)
        .collect::<String>()
        .to_ascii_lowercase();
    HTML_MARKERS
        .iter()
        .any(|marker| body_prefix.contains(marker))
}

fn summarize_html_body(body: &str) -> String {
    if let Some(title) = extract_html_tag(body, "title") {
        let title = title.trim();
        if !title.is_empty() {
            return format!("title: {title}");
        }
    }

    let snippet = body
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(80)
        .collect::<String>();
    if snippet.is_empty() {
        "empty HTML body".to_owned()
    } else {
        format!("snippet: {snippet}")
    }
}

fn extract_html_tag(body: &str, tag: &str) -> Option<String> {
    let lower = body.to_ascii_lowercase();
    let start_marker = format!("<{tag}>");
    let end_marker = format!("</{tag}>");
    let start = lower.find(&start_marker)?;
    let content_start = start + start_marker.len();
    let end = lower[content_start..].find(&end_marker)? + content_start;
    Some(body[content_start..end].to_owned())
}

fn body_looks_not_found(body: &str) -> bool {
    body.to_ascii_lowercase()
        .contains("the page you were looking for doesn't exist")
}

#[derive(Debug, Deserialize)]
struct ClassificationProbe {
    kingdom: Option<IgnoredAny>,
    superclass: Option<IgnoredAny>,
    #[serde(rename = "class")]
    class_node: Option<IgnoredAny>,
    subclass: Option<IgnoredAny>,
    direct_parent: Option<IgnoredAny>,
}

impl ClassificationProbe {
    #[inline]
    fn has_classification(&self) -> bool {
        self.direct_parent.is_some()
            || self.kingdom.is_some()
            || self.superclass.is_some()
            || self.class_node.is_some()
            || self.subclass.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockResponse, MockServer};

    #[test]
    fn validates_classified_entity() {
        let body = HttpBody {
            status: 200,
            content_type: Some("application/json".to_owned()),
            body: r#"{"inchikey":"InChIKey=IJDNQMDRQITEOD-UHFFFAOYSA-N","kingdom":{"name":"Organic compounds","description":"x","chemont_id":"CHEMONTID:0000000","url":"u"},"superclass":null,"class":null,"subclass":null,"intermediate_nodes":[],"direct_parent":{"name":"Alkanes","description":"x","chemont_id":"CHEMONTID:0002500","url":"u"},"alternative_parents":[],"substituents":[],"external_descriptors":[],"ancestors":[],"predicted_chebi_terms":[],"predicted_lipidmaps_terms":[]}"#.to_owned(),
        };
        assert!(validate_entity_body(&body).unwrap());
    }

    #[test]
    fn treats_html_404_as_error_not_miss() {
        let body = HttpBody {
            status: 404,
            content_type: Some("text/html".to_owned()),
            body: "<html><head><title>Not Found</title></head><body>gateway</body></html>"
                .to_owned(),
        };
        let error = validate_entity_body(&body).unwrap_err().to_string();
        assert!(error.contains("returned HTML"));
    }

    #[test]
    fn fetches_plain_404_as_miss() {
        let server = MockServer::new([(
            "/entities/XLYOFNOQVPJJNP-UHFFFAOYSA-N.json",
            MockResponse::text(404, "missing"),
        )]);
        let client = ApiClient::new(server.url(), "classyfire-test/0.1", 1).unwrap();

        let body = client.fetch_entity("XLYOFNOQVPJJNP-UHFFFAOYSA-N").unwrap();

        assert!(!validate_entity_body(&body).unwrap());
        assert_eq!(
            server.seen_paths(),
            vec!["/entities/XLYOFNOQVPJJNP-UHFFFAOYSA-N.json".to_owned()]
        );
    }

    #[test]
    fn fetches_429_as_throttle_error() {
        let server = MockServer::new([(
            "/entities/OTMSDBZUPAUEDD-UHFFFAOYSA-N.json",
            MockResponse::text(429, "slow down"),
        )]);
        let client = ApiClient::new(server.url(), "classyfire-test/0.1", 1).unwrap();

        let body = client.fetch_entity("OTMSDBZUPAUEDD-UHFFFAOYSA-N").unwrap();
        let error = validate_entity_body(&body).unwrap_err().to_string();

        assert!(error.contains("throttled"));
    }

    #[test]
    fn fetches_html_body_as_error() {
        let server = MockServer::new([(
            "/entities/VNWKTOKETHGBQD-UHFFFAOYSA-N.json",
            MockResponse::html(200, "<html><body>gateway timeout</body></html>"),
        )]);
        let client = ApiClient::new(server.url(), "classyfire-test/0.1", 1).unwrap();

        let body = client.fetch_entity("VNWKTOKETHGBQD-UHFFFAOYSA-N").unwrap();
        let error = validate_entity_body(&body).unwrap_err().to_string();

        assert!(error.contains("returned HTML"));
    }

    #[test]
    fn rejects_malformed_json_from_server() {
        let server = MockServer::new([(
            "/entities/VNWKTOKETHGBQD-UHFFFAOYSA-N.json",
            MockResponse::json(200, "{"),
        )]);
        let client = ApiClient::new(server.url(), "classyfire-test/0.1", 1).unwrap();

        let body = client.fetch_entity("VNWKTOKETHGBQD-UHFFFAOYSA-N").unwrap();
        let error = validate_entity_body(&body).unwrap_err().to_string();

        assert!(error.contains("failed to parse entity response JSON"));
    }

    #[test]
    fn rejects_generic_server_error() {
        let body = HttpBody {
            status: 500,
            content_type: Some("application/json".to_owned()),
            body: r#"{"error":"boom"}"#.to_owned(),
        };

        let error = validate_entity_body(&body).unwrap_err().to_string();

        assert!(error.contains("failed with status 500"));
    }

    #[test]
    fn treats_empty_body_as_miss() {
        let body = HttpBody {
            status: 200,
            content_type: Some("application/json".to_owned()),
            body: "   ".to_owned(),
        };

        assert!(!validate_entity_body(&body).unwrap());
    }

    #[test]
    fn recognizes_classification_without_direct_parent() {
        let body = HttpBody {
            status: 200,
            content_type: Some("application/json".to_owned()),
            body: r#"{"superclass":{"name":"Benzenoids"}}"#.to_owned(),
        };

        assert!(validate_entity_body(&body).unwrap());
    }

    #[test]
    fn html_summary_falls_back_to_snippet_without_title() {
        let summary =
            summarize_html_body("<html><body>gateway timeout from upstream</body></html>");
        assert!(summary.starts_with("snippet:"));
    }

    #[test]
    fn html_summary_handles_empty_html() {
        assert_eq!(summarize_html_body("   "), "empty HTML body");
    }
}
