use crate::types::EntityResponse;
use anyhow::{bail, Context, Result};
use reqwest::blocking::Client;
use serde_json::Value;

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

pub fn validate_entity_body(body: &HttpBody) -> Result<Option<EntityResponse>> {
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
        return Ok(None);
    }
    if body.status >= 400 {
        bail!("entity request failed with status {}", body.status);
    }
    if body.body.trim().is_empty() {
        return Ok(None);
    }

    let value: Value =
        serde_json::from_str(&body.body).context("failed to parse entity response JSON")?;
    if value.is_null() {
        return Ok(None);
    }

    let entity: EntityResponse =
        serde_json::from_value(value).context("failed to deserialize entity response")?;
    if entity.has_classification() {
        Ok(Some(entity))
    } else {
        Ok(None)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_classified_entity() {
        let body = HttpBody {
            status: 200,
            content_type: Some("application/json".to_owned()),
            body: r#"{"inchikey":"InChIKey=IJDNQMDRQITEOD-UHFFFAOYSA-N","kingdom":{"name":"Organic compounds","description":"x","chemont_id":"CHEMONTID:0000000","url":"u"},"superclass":null,"class":null,"subclass":null,"intermediate_nodes":[],"direct_parent":{"name":"Alkanes","description":"x","chemont_id":"CHEMONTID:0002500","url":"u"},"alternative_parents":[],"substituents":[],"external_descriptors":[],"ancestors":[],"predicted_chebi_terms":[],"predicted_lipidmaps_terms":[]}"#.to_owned(),
        };
        let entity = validate_entity_body(&body).unwrap().unwrap();
        assert!(entity.has_classification());
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
}
