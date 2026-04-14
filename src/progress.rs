use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use plotters::coord::Shift;
use plotters::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::runtime::Builder;
use zenodo_rs::{ArtifactSelector, Auth, ZenodoClient};

pub const SUCCESS_BITMAP_NAME: &str = "state.success.bits";
pub const MISS_BITMAP_NAME: &str = "state.miss.bits";
pub const INVALID_BITMAP_NAME: &str = "state.invalid.bits";
pub const FAILED_BITMAP_NAME: &str = "state.failed.bits";
pub const CHECKPOINT_NAME: &str = "checkpoint.json";
pub const RELEASE_DATASET_FILENAME: &str = "classyfire-labels.jsonl.zst";
pub const RELEASE_DIR_NAME: &str = "releases";
pub const RELEASE_MANIFEST_FILENAME: &str = "manifest.json";
pub const RELEASE_PROGRESS_SUMMARY_FILENAME: &str = "progress-summary.json";
pub const SUCCESS_DIR_NAME: &str = "success";
pub const DEFAULT_ZENODO_DOI: &str = "10.5281/zenodo.19235916";

const SVG_WIDTH: u32 = 1600;
const HEADER_HEIGHT: u32 = 108;
const METRICS_ROW_HEIGHT: u32 = 292;
const ZENODO_TIMEOUT_SECONDS: u64 = 300;
const SUMMARY_CARD_COUNT: usize = 2;
const LAYER_PANEL_LIMIT: usize = 6;
const DIRECT_PARENT_PANEL_LIMIT: usize = 5;

const BACKGROUND: RGBColor = RGBColor(255, 255, 255);
const CARD_BACKGROUND: RGBColor = RGBColor(255, 255, 255);
const CARD_BORDER: RGBColor = RGBColor(215, 208, 196);
const TITLE_COLOR: RGBColor = RGBColor(27, 40, 59);
const BODY_COLOR: RGBColor = RGBColor(75, 74, 72);
const MUTED_COLOR: RGBColor = RGBColor(126, 122, 116);
const COVERAGE_COLOR: RGBColor = RGBColor(24, 127, 124);
const REMAINING_COLOR: RGBColor = RGBColor(214, 209, 201);
const SUCCESS_COLOR: RGBColor = RGBColor(72, 148, 108);
const MISS_COLOR: RGBColor = RGBColor(211, 164, 67);
const INVALID_COLOR: RGBColor = RGBColor(121, 128, 139);
const FAILED_COLOR: RGBColor = RGBColor(197, 92, 82);
const LAYER_COLORS: [RGBColor; 5] = [
    RGBColor(41, 104, 142),
    RGBColor(61, 131, 132),
    RGBColor(104, 148, 121),
    RGBColor(157, 150, 94),
    RGBColor(191, 118, 76),
];
const TOP_TAXONOMY_LAYERS: [TaxonomyLayer; 2] = [TaxonomyLayer::Kingdom, TaxonomyLayer::Superclass];
const BOTTOM_TAXONOMY_LAYERS: [TaxonomyLayer; 3] = [
    TaxonomyLayer::Class,
    TaxonomyLayer::Subclass,
    TaxonomyLayer::DirectParent,
];

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrawlCounts {
    pub success_rows: u64,
    pub miss_rows: u64,
    pub invalid_rows: u64,
    pub failed_rows: u64,
}

impl CrawlCounts {
    #[must_use]
    pub fn handled_rows(self) -> u64 {
        self.success_rows + self.miss_rows + self.invalid_rows + self.failed_rows
    }

    #[must_use]
    pub fn executed_request_rows(self) -> u64 {
        self.success_rows + self.miss_rows + self.failed_rows
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TaxonomyLayer {
    Kingdom,
    Superclass,
    Class,
    Subclass,
    DirectParent,
}

impl TaxonomyLayer {
    pub const ALL: [Self; 5] = [
        Self::Kingdom,
        Self::Superclass,
        Self::Class,
        Self::Subclass,
        Self::DirectParent,
    ];
    pub const COUNT: usize = Self::ALL.len();

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Kingdom => "Kingdom",
            Self::Superclass => "Superclass",
            Self::Class => "Class",
            Self::Subclass => "Subclass",
            Self::DirectParent => "Direct parent",
        }
    }

    #[must_use]
    fn color(self) -> RGBColor {
        match self {
            Self::Kingdom => LAYER_COLORS[0],
            Self::Superclass => LAYER_COLORS[1],
            Self::Class => LAYER_COLORS[2],
            Self::Subclass => LAYER_COLORS[3],
            Self::DirectParent => LAYER_COLORS[4],
        }
    }

    #[must_use]
    fn index(self) -> usize {
        match self {
            Self::Kingdom => 0,
            Self::Superclass => 1,
            Self::Class => 2,
            Self::Subclass => 3,
            Self::DirectParent => 4,
        }
    }

    #[must_use]
    fn panel_limit(self) -> usize {
        match self {
            Self::DirectParent => DIRECT_PARENT_PANEL_LIMIT,
            _ => LAYER_PANEL_LIMIT,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaxonomyClassCount {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chemont_id: Option<String>,
    pub label_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaxonomyLayerSummary {
    pub layer: TaxonomyLayer,
    pub assigned_records: u64,
    pub unique_class_count: u64,
    pub classes: Vec<TaxonomyClassCount>,
}

impl TaxonomyLayerSummary {
    #[must_use]
    pub fn shown_classes(&self, limit: usize) -> &[TaxonomyClassCount] {
        let shown_count = self.classes.len().min(limit);
        &self.classes[..shown_count]
    }

    #[must_use]
    pub fn has_tail(&self, shown_count: usize) -> bool {
        self.classes.len() > shown_count.min(self.classes.len())
    }

    #[must_use]
    pub fn tail_summary(&self, shown_count: usize) -> (usize, u64) {
        let tail = &self.classes[shown_count.min(self.classes.len())..];
        let remaining_classes = tail.len();
        let remaining_labels = tail
            .iter()
            .map(|class_count| class_count.label_count)
            .sum::<u64>();
        (remaining_classes, remaining_labels)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProgressSummary {
    pub generated_at: String,
    pub pubchem_total_rows: u64,
    pub counts: CrawlCounts,
    pub taxonomy_layers: Vec<TaxonomyLayerSummary>,
}

impl ProgressSummary {
    #[must_use]
    pub fn handled_rows(&self) -> u64 {
        self.counts.handled_rows()
    }

    #[must_use]
    pub fn executed_request_rows(&self) -> u64 {
        self.counts.executed_request_rows()
    }

    #[must_use]
    pub fn coverage_fraction(&self) -> f64 {
        safe_ratio(self.counts.success_rows, self.pubchem_total_rows)
    }

    #[must_use]
    pub fn request_success_fraction(&self) -> f64 {
        safe_ratio(self.counts.success_rows, self.executed_request_rows())
    }

    #[must_use]
    fn layer(&self, layer: TaxonomyLayer) -> Option<&TaxonomyLayerSummary> {
        self.taxonomy_layers
            .iter()
            .find(|summary| summary.layer == layer)
    }
}

#[derive(Debug, Clone)]
struct MetricSlice {
    label: String,
    value: u64,
    color: RGBColor,
}

#[derive(Debug, Clone)]
struct MetricPanel {
    title: String,
    subtitle: String,
    center_value: String,
    center_caption: String,
    footer_note: Option<String>,
    slices: Vec<MetricSlice>,
}

#[derive(Debug, Deserialize)]
struct CheckpointView {
    input_path: String,
}

#[derive(Debug, Deserialize)]
struct ReleaseManifest {
    #[serde(rename = "success_rows")]
    success: u64,
    #[serde(rename = "miss_rows")]
    miss: u64,
    #[serde(rename = "invalid_rows")]
    invalid: u64,
    #[serde(rename = "failed_rows")]
    failed: u64,
}

#[derive(Debug, Deserialize)]
struct SuccessRecordView {
    classyfire: TaxonomyFields,
}

#[derive(Debug, Deserialize)]
struct TaxonomyFields {
    kingdom: Option<TaxonomyNode>,
    superclass: Option<TaxonomyNode>,
    #[serde(rename = "class")]
    class_node: Option<TaxonomyNode>,
    subclass: Option<TaxonomyNode>,
    direct_parent: Option<TaxonomyNode>,
}

#[derive(Debug, Deserialize)]
struct TaxonomyNode {
    name: String,
    chemont_id: Option<String>,
}

#[derive(Debug)]
struct TaxonomyClassAccumulator {
    name: String,
    chemont_id: Option<String>,
    label_count: u64,
}

#[derive(Debug, Default)]
struct TaxonomyAccumulator {
    assigned_records: u64,
    classes: HashMap<String, TaxonomyClassAccumulator>,
}

type TaxonomyAccumulators = [TaxonomyAccumulator; TaxonomyLayer::COUNT];

impl TaxonomyAccumulator {
    fn register(&mut self, node: &TaxonomyNode) {
        let name = display_class_name(&node.name);
        let key = node
            .chemont_id
            .clone()
            .unwrap_or_else(|| name.to_ascii_lowercase());
        let class = self
            .classes
            .entry(key)
            .or_insert_with(|| TaxonomyClassAccumulator {
                name: name.clone(),
                chemont_id: node.chemont_id.clone(),
                label_count: 0,
            });
        class.label_count += 1;
        self.assigned_records += 1;
    }

    fn finalize(self, layer: TaxonomyLayer) -> TaxonomyLayerSummary {
        let mut classes = self
            .classes
            .into_values()
            .map(|class| TaxonomyClassCount {
                name: class.name,
                chemont_id: class.chemont_id,
                label_count: class.label_count,
            })
            .collect::<Vec<_>>();
        classes.sort_by(|left, right| {
            right
                .label_count
                .cmp(&left.label_count)
                .then_with(|| left.name.cmp(&right.name))
        });
        TaxonomyLayerSummary {
            layer,
            assigned_records: self.assigned_records,
            unique_class_count: classes.len() as u64,
            classes,
        }
    }
}

impl ReleaseManifest {
    #[must_use]
    fn counts(self) -> CrawlCounts {
        CrawlCounts {
            success_rows: self.success,
            miss_rows: self.miss,
            invalid_rows: self.invalid,
            failed_rows: self.failed,
        }
    }
}

pub fn build_local_progress_summary(
    output_dir: &Path,
    pubchem_total_rows: Option<u64>,
) -> Result<ProgressSummary> {
    let counts = load_counts_from_bitmaps(output_dir)?;
    let pubchem_total_rows = resolve_pubchem_total_rows(output_dir, pubchem_total_rows)?;
    build_progress_summary_from_paths(
        &list_success_shards(output_dir)?,
        pubchem_total_rows,
        counts,
    )
}

pub fn build_release_progress_summary(
    output_dir: &Path,
    pubchem_total_rows: u64,
    counts: CrawlCounts,
) -> Result<ProgressSummary> {
    build_progress_summary_from_paths(
        &list_success_shards(output_dir)?,
        pubchem_total_rows,
        counts,
    )
}

pub fn build_progress_summary_from_dataset(
    dataset_path: &Path,
    pubchem_total_rows: u64,
    counts: CrawlCounts,
) -> Result<ProgressSummary> {
    build_progress_summary_from_paths(&[dataset_path.to_path_buf()], pubchem_total_rows, counts)
}

pub fn resolve_pubchem_total_rows(output_dir: &Path, override_rows: Option<u64>) -> Result<u64> {
    if let Some(override_rows) = override_rows {
        if override_rows == 0 {
            bail!("--pubchem-total-rows must be greater than zero");
        }
        return Ok(override_rows);
    }

    let checkpoint_path = output_dir.join(CHECKPOINT_NAME);
    let checkpoint: CheckpointView = serde_json::from_slice(
        &fs::read(&checkpoint_path)
            .with_context(|| format!("failed reading {}", checkpoint_path.display()))?,
    )
    .with_context(|| format!("failed parsing {}", checkpoint_path.display()))?;

    let input_path = Path::new(&checkpoint.input_path);
    count_lines_in_path(input_path)
        .with_context(|| format!("failed counting PubChem rows in {}", input_path.display()))
}

pub fn load_progress_summary(path: &Path) -> Result<ProgressSummary> {
    serde_json::from_slice(
        &fs::read(path).with_context(|| format!("failed reading {}", path.display()))?,
    )
    .with_context(|| format!("failed parsing {}", path.display()))
}

pub fn write_progress_summary(path: &Path, summary: &ProgressSummary) -> Result<()> {
    let bytes =
        serde_json::to_vec_pretty(summary).context("failed serializing progress summary")?;
    fs::write(path, bytes).with_context(|| format!("failed writing {}", path.display()))
}

pub fn load_or_download_zenodo_progress_summary(
    doi: &str,
    cache_dir: &Path,
    pubchem_total_rows: Option<u64>,
    user_agent: &str,
) -> Result<ProgressSummary> {
    fs::create_dir_all(cache_dir)
        .with_context(|| format!("failed creating {}", cache_dir.display()))?;

    Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build Tokio runtime for Zenodo download")?
        .block_on(load_or_download_zenodo_progress_summary_async(
            doi,
            cache_dir,
            pubchem_total_rows,
            user_agent,
        ))
}

async fn load_or_download_zenodo_progress_summary_async(
    doi: &str,
    cache_dir: &Path,
    pubchem_total_rows: Option<u64>,
    user_agent: &str,
) -> Result<ProgressSummary> {
    let token = std::env::var("ZENODO_TOKEN").unwrap_or_default();
    let client = ZenodoClient::builder(Auth::new(token))
        .user_agent(user_agent)
        .connect_timeout(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(ZENODO_TIMEOUT_SECONDS))
        .build()
        .context("failed to build Zenodo download client")?;

    let summary_path = cache_dir.join(RELEASE_PROGRESS_SUMMARY_FILENAME);
    match download_release_artifact(
        &client,
        doi,
        RELEASE_PROGRESS_SUMMARY_FILENAME,
        &summary_path,
    )
    .await
    {
        Ok(_) => load_progress_summary(&summary_path),
        Err(summary_error) => {
            let Some(pubchem_total_rows) = pubchem_total_rows else {
                return Err(anyhow!(
                    "failed downloading {RELEASE_PROGRESS_SUMMARY_FILENAME} from {doi}: {summary_error:#}. Pass --pubchem-total-rows to enable dataset fallback"
                ));
            };

            let manifest_path = cache_dir.join(RELEASE_MANIFEST_FILENAME);
            let dataset_path = cache_dir.join(RELEASE_DATASET_FILENAME);
            download_release_artifact(&client, doi, RELEASE_MANIFEST_FILENAME, &manifest_path)
                .await
                .with_context(|| {
                    format!(
                        "failed downloading {RELEASE_MANIFEST_FILENAME} from {doi} after summary fallback"
                    )
                })?;
            download_release_artifact(&client, doi, RELEASE_DATASET_FILENAME, &dataset_path)
                .await
                .with_context(|| {
                    format!(
                        "failed downloading {RELEASE_DATASET_FILENAME} from {doi} after summary fallback"
                    )
                })?;

            let manifest: ReleaseManifest = serde_json::from_slice(
                &fs::read(&manifest_path)
                    .with_context(|| format!("failed reading {}", manifest_path.display()))?,
            )
            .with_context(|| format!("failed parsing {}", manifest_path.display()))?;

            build_progress_summary_from_dataset(
                &dataset_path,
                pubchem_total_rows,
                manifest.counts(),
            )
        }
    }
}

async fn download_release_artifact(
    client: &ZenodoClient,
    doi: &str,
    filename: &str,
    destination: &Path,
) -> Result<()> {
    let selector = ArtifactSelector::latest_file_by_doi(doi, filename)
        .with_context(|| format!("invalid Zenodo DOI `{doi}`"))?;
    client
        .download_artifact(&selector, destination)
        .await
        .with_context(|| format!("failed downloading {filename} from {doi}"))?;
    Ok(())
}

pub fn render_progress_svg(
    output_path: &Path,
    summary: &ProgressSummary,
    source_label: &str,
) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }

    let top_row_height = taxonomy_row_height(summary, &TOP_TAXONOMY_LAYERS);
    let bottom_row_height = taxonomy_row_height(summary, &BOTTOM_TAXONOMY_LAYERS);
    let svg_height = HEADER_HEIGHT + METRICS_ROW_HEIGHT + top_row_height + bottom_row_height;

    let root = SVGBackend::new(output_path, (SVG_WIDTH, svg_height)).into_drawing_area();
    root.fill(&BACKGROUND)
        .map_err(|error| anyhow!("failed painting SVG background: {error:?}"))?;

    let (header, body) = root.split_vertically(HEADER_HEIGHT);
    let (summary_row, taxonomy_body) = body.split_vertically(METRICS_ROW_HEIGHT);
    let (taxonomy_top_row, taxonomy_bottom_row) = taxonomy_body.split_vertically(top_row_height);
    let summary_cards = summary_row.split_evenly((1, SUMMARY_CARD_COUNT));
    let top_panels = taxonomy_top_row.split_evenly((1, 2));
    let bottom_panels = taxonomy_bottom_row.split_evenly((1, 3));

    draw_header(&header, summary, source_label)
        .map_err(|error| anyhow!("failed drawing SVG header: {error:?}"))?;
    let metric_panels = [build_coverage_panel(summary), build_request_panel(summary)];
    let metric_errors = ["coverage summary", "request summary"];
    for ((panel, metric), label) in summary_cards
        .iter()
        .zip(metric_panels.iter())
        .zip(metric_errors)
    {
        draw_metric_panel(panel, metric)
            .map_err(|error| anyhow!("failed drawing {label}: {error:?}"))?;
    }

    let layer_panels = [
        (&top_panels[0], TaxonomyLayer::Kingdom),
        (&top_panels[1], TaxonomyLayer::Superclass),
        (&bottom_panels[0], TaxonomyLayer::Class),
        (&bottom_panels[1], TaxonomyLayer::Subclass),
        (&bottom_panels[2], TaxonomyLayer::DirectParent),
    ];
    for (panel, layer) in layer_panels {
        if let Some(layer_summary) = summary.layer(layer) {
            draw_layer_panel(panel, layer_summary, layer.panel_limit())
                .map_err(|error| anyhow!("failed drawing {} panel: {error:?}", layer.label()))?;
        }
    }

    root.present()
        .map_err(|error| anyhow!("failed finalizing SVG output: {error:?}"))
}

fn taxonomy_row_height(summary: &ProgressSummary, layers: &[TaxonomyLayer]) -> u32 {
    let max_rows = layers
        .iter()
        .filter_map(|layer| summary.layer(*layer))
        .map(|breakdown| breakdown.shown_classes(breakdown.layer.panel_limit()).len() as u32)
        .max()
        .unwrap_or(0);
    let has_tail = layers.iter().any(|layer| {
        summary
            .layer(*layer)
            .is_some_and(|breakdown| breakdown.has_tail(layer.panel_limit()))
    });
    let tail_height = if has_tail { 34 } else { 0 };
    (188 + max_rows * 26 + tail_height + 26).max(320)
}

fn build_coverage_panel(summary: &ProgressSummary) -> MetricPanel {
    let pending_rows = summary
        .pubchem_total_rows
        .saturating_sub(summary.handled_rows());
    MetricPanel {
        title: "Collected vs PubChem".to_owned(),
        subtitle: format!(
            "{} successful labels from {} total PubChem rows",
            format_count(summary.counts.success_rows),
            format_count(summary.pubchem_total_rows)
        ),
        center_value: format_percent(summary.counts.success_rows, summary.pubchem_total_rows),
        center_caption: "collected".to_owned(),
        footer_note: None,
        slices: vec![
            MetricSlice {
                label: format!("Collected: {}", format_count(summary.counts.success_rows)),
                value: summary.counts.success_rows,
                color: COVERAGE_COLOR,
            },
            MetricSlice {
                label: format!("Pending: {}", format_count(pending_rows)),
                value: pending_rows,
                color: REMAINING_COLOR,
            },
            MetricSlice {
                label: format!("Miss: {}", format_count(summary.counts.miss_rows)),
                value: summary.counts.miss_rows,
                color: MISS_COLOR,
            },
            MetricSlice {
                label: format!("Failed: {}", format_count(summary.counts.failed_rows)),
                value: summary.counts.failed_rows,
                color: FAILED_COLOR,
            },
            MetricSlice {
                label: format!("Invalid: {}", format_count(summary.counts.invalid_rows)),
                value: summary.counts.invalid_rows,
                color: INVALID_COLOR,
            },
        ],
    }
}

fn build_request_panel(summary: &ProgressSummary) -> MetricPanel {
    let executed = summary.executed_request_rows();
    MetricPanel {
        title: "Executed Request Outcomes".to_owned(),
        subtitle: format!(
            "{} executed requests recorded in this source",
            format_count(executed)
        ),
        center_value: format_percent(summary.counts.success_rows, executed),
        center_caption: "successful requests".to_owned(),
        footer_note: Some(format!(
            "Invalid rows skipped before request: {}",
            format_count(summary.counts.invalid_rows)
        )),
        slices: vec![
            MetricSlice {
                label: format!("Success: {}", format_count(summary.counts.success_rows)),
                value: summary.counts.success_rows,
                color: SUCCESS_COLOR,
            },
            MetricSlice {
                label: format!("Miss: {}", format_count(summary.counts.miss_rows)),
                value: summary.counts.miss_rows,
                color: MISS_COLOR,
            },
            MetricSlice {
                label: format!("Failed: {}", format_count(summary.counts.failed_rows)),
                value: summary.counts.failed_rows,
                color: FAILED_COLOR,
            },
        ],
    }
}

fn build_progress_summary_from_paths(
    paths: &[PathBuf],
    pubchem_total_rows: u64,
    counts: CrawlCounts,
) -> Result<ProgressSummary> {
    if pubchem_total_rows == 0 {
        bail!("PubChem total rows must be greater than zero");
    }
    let taxonomy_layers = aggregate_taxonomy(paths)?;
    Ok(ProgressSummary {
        generated_at: Utc::now().to_rfc3339(),
        pubchem_total_rows,
        counts,
        taxonomy_layers,
    })
}

fn load_counts_from_bitmaps(output_dir: &Path) -> Result<CrawlCounts> {
    Ok(CrawlCounts {
        success_rows: count_set_bits(&output_dir.join(SUCCESS_BITMAP_NAME))?,
        miss_rows: count_set_bits(&output_dir.join(MISS_BITMAP_NAME))?,
        invalid_rows: count_set_bits(&output_dir.join(INVALID_BITMAP_NAME))?,
        failed_rows: count_set_bits(&output_dir.join(FAILED_BITMAP_NAME))?,
    })
}

fn count_set_bits(path: &Path) -> Result<u64> {
    match fs::read(path) {
        Ok(bytes) => Ok(bytes.iter().map(|byte| u64::from(byte.count_ones())).sum()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error).with_context(|| format!("failed reading {}", path.display())),
    }
}

fn list_success_shards(output_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut shards = fs::read_dir(output_dir.join(SUCCESS_DIR_NAME))
        .with_context(|| {
            format!(
                "failed reading {}",
                output_dir.join(SUCCESS_DIR_NAME).display()
            )
        })?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("zst"))
        .collect::<Vec<_>>();
    shards.sort();
    Ok(shards)
}

fn aggregate_taxonomy(paths: &[PathBuf]) -> Result<Vec<TaxonomyLayerSummary>> {
    let mut accumulators: TaxonomyAccumulators =
        std::array::from_fn(|_| TaxonomyAccumulator::default());

    for path in paths {
        match fs::metadata(path) {
            Ok(metadata) if metadata.len() == 0 => continue,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("failed stating {}", path.display()));
            }
        }
        let reader = open_record_reader(path)?;
        for (line_number, line) in reader.lines().enumerate() {
            let line = line.with_context(|| {
                format!(
                    "failed reading line {} from {}",
                    line_number + 1,
                    path.display()
                )
            })?;
            if line.trim().is_empty() {
                continue;
            }
            let record: SuccessRecordView = serde_json::from_str(&line).with_context(|| {
                format!(
                    "failed parsing line {} from {} as a success record",
                    line_number + 1,
                    path.display()
                )
            })?;
            record.classyfire.accumulate(&mut accumulators);
        }
    }

    Ok(TaxonomyLayer::ALL
        .into_iter()
        .map(|layer| std::mem::take(&mut accumulators[layer.index()]).finalize(layer))
        .collect())
}

impl TaxonomyFields {
    fn accumulate(&self, accumulators: &mut TaxonomyAccumulators) {
        for layer in TaxonomyLayer::ALL {
            if let Some(node) = layer.node(self) {
                accumulators[layer.index()].register(node);
            }
        }
    }
}

impl TaxonomyLayer {
    fn node(self, fields: &TaxonomyFields) -> Option<&TaxonomyNode> {
        match self {
            Self::Kingdom => fields.kingdom.as_ref(),
            Self::Superclass => fields.superclass.as_ref(),
            Self::Class => fields.class_node.as_ref(),
            Self::Subclass => fields.subclass.as_ref(),
            Self::DirectParent => fields.direct_parent.as_ref(),
        }
    }
}

fn open_record_reader(path: &Path) -> Result<Box<dyn BufRead>> {
    Ok(Box::new(BufReader::new(open_read_stream(path)?)))
}

fn count_lines_in_path(path: &Path) -> Result<u64> {
    let mut reader = open_read_stream(path)?;

    let mut lines = 0_u64;
    let mut buffer = vec![0_u8; 8 * 1024 * 1024];
    let mut last_byte = None;
    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed reading {}", path.display()))?;
        if bytes_read == 0 {
            break;
        }
        let chunk = &buffer[..bytes_read];
        lines += chunk.iter().filter(|byte| **byte == b'\n').count() as u64;
        last_byte = chunk.last().copied();
    }
    if last_byte.is_some() && last_byte != Some(b'\n') {
        lines += 1;
    }
    Ok(lines)
}

fn open_read_stream(path: &Path) -> Result<Box<dyn Read>> {
    let file = File::open(path).with_context(|| format!("failed opening {}", path.display()))?;
    if is_zstd_path(path) {
        let decoder = zstd::stream::read::Decoder::new(BufReader::new(file))
            .with_context(|| format!("failed opening zstd decoder for {}", path.display()))?;
        Ok(Box::new(decoder))
    } else {
        Ok(Box::new(file))
    }
}

fn is_zstd_path(path: &Path) -> bool {
    path.extension().and_then(|ext| ext.to_str()) == Some("zst")
}

fn draw_header<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    summary: &ProgressSummary,
    source_label: &str,
) -> std::result::Result<(), DrawingAreaErrorKind<DB::ErrorType>> {
    let panel = prepare_panel(area)?;
    let dims = panel.dim_in_pixel();
    let width = dims.0 as i32;
    let title_style = TextStyle::from(("sans-serif", 34).into_font()).color(&TITLE_COLOR);
    let description_style = TextStyle::from(("sans-serif", 17).into_font()).color(&BODY_COLOR);
    let meta_style = TextStyle::from(("sans-serif", 15).into_font()).color(&MUTED_COLOR);

    panel.draw(&Text::new(
        "ClassyFire Crawl Progress Dashboard",
        (24, 34),
        title_style,
    ))?;

    panel.draw(&Text::new(
        "Coverage, request outcomes, and taxonomy distribution for the current crawl snapshot.",
        (24, 60),
        description_style,
    ))?;

    let generated_text = format!("Summary timestamp: {}", summary.generated_at);
    let (generated_width, _) = panel.estimate_text_size(&generated_text, &meta_style)?;
    let generated_x = width - 24 - generated_width as i32;
    let source_text = truncate_to_width(
        &panel,
        &format!("Source: {source_label}"),
        &meta_style,
        (generated_x - 48).max(240) as u32,
    )?;
    panel.draw(&Text::new(source_text, (24, 80), meta_style.clone()))?;
    panel.draw(&Text::new(generated_text, (generated_x, 80), meta_style))?;
    Ok(())
}

fn draw_metric_panel<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    metric: &MetricPanel,
) -> std::result::Result<(), DrawingAreaErrorKind<DB::ErrorType>> {
    let panel = prepare_panel(area)?;
    let screen_panel = panel.use_screen_coord();
    panel.draw(&Text::new(
        metric.title.clone(),
        (24, 28),
        TextStyle::from(("sans-serif", 24).into_font()).color(&TITLE_COLOR),
    ))?;
    panel.draw(&Text::new(
        metric.subtitle.clone(),
        (24, 54),
        TextStyle::from(("sans-serif", 17).into_font()).color(&MUTED_COLOR),
    ))?;

    let footer_y = 80_i32;
    if let Some(footer_note) = &metric.footer_note {
        panel.draw(&Text::new(
            footer_note.clone(),
            (24, footer_y),
            TextStyle::from(("sans-serif", 14).into_font()).color(&MUTED_COLOR),
        ))?;
    }

    let total: u64 = metric.slices.iter().map(|slice| slice.value).sum();
    if total == 0 {
        panel.draw(&Text::new(
            "No rows available yet",
            (24, 112),
            TextStyle::from(("sans-serif", 20).into_font()).color(&TITLE_COLOR),
        ))?;
        return Ok(());
    }

    let dims = panel.dim_in_pixel();
    let center = (dims.0 as i32 - 180, dims.1 as i32 / 2 + 14);
    let radius = (f64::from(dims.1) * 0.34)
        .min(f64::from(dims.0) * 0.16)
        .max(78.0);
    let sizes: Vec<f64> = metric
        .slices
        .iter()
        .map(|slice| slice.value as f64)
        .collect();
    let colors: Vec<RGBColor> = metric.slices.iter().map(|slice| slice.color).collect();
    let labels = vec![""; metric.slices.len()];

    let (x_range, y_range) = panel.get_pixel_range();
    let center_abs = (x_range.end - 180, (y_range.start + y_range.end) / 2 + 14);

    let hole_radius = radius * 0.68;
    let mut pie = Pie::new(&center_abs, &radius, &sizes, &colors, &labels);
    pie.start_angle(-90.0);
    pie.donut_hole(hole_radius);
    screen_panel.draw(&pie)?;

    let value_style = TextStyle::from(("sans-serif", 28).into_font()).color(&TITLE_COLOR);
    let caption_style = TextStyle::from(("sans-serif", 14).into_font()).color(&MUTED_COLOR);
    let caption_lines = wrap_text_to_width(
        &panel,
        &metric.center_caption,
        &caption_style,
        ((hole_radius * 2.0) as u32).saturating_sub(18),
        2,
    )?;
    let value_y = if caption_lines.len() > 1 {
        center.1 - 24
    } else {
        center.1 - 18
    };
    draw_centered_text(
        &panel,
        &metric.center_value,
        center.0,
        value_y,
        &value_style,
    )?;
    let caption_start_y = if caption_lines.len() > 1 {
        center.1 + 2
    } else {
        center.1 + 12
    };
    for (index, line) in caption_lines.iter().enumerate() {
        draw_centered_text(
            &panel,
            line,
            center.0,
            caption_start_y + index as i32 * 14,
            &caption_style,
        )?;
    }

    let mut current_y = if metric.footer_note.is_some() {
        106
    } else {
        92
    };
    for slice in &metric.slices {
        draw_legend_entry(&panel, (24, current_y), slice.color, slice.label.clone())?;
        current_y += 24;
    }

    Ok(())
}

fn draw_layer_panel<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    layer: &TaxonomyLayerSummary,
    top_limit: usize,
) -> std::result::Result<(), DrawingAreaErrorKind<DB::ErrorType>> {
    let panel = prepare_panel(area)?;
    panel.draw(&Text::new(
        format!("{} Breakdown", layer.layer.label()),
        (22, 28),
        TextStyle::from(("sans-serif", 24).into_font()).color(&TITLE_COLOR),
    ))?;
    panel.draw(&Text::new(
        format!(
            "{} label assignments | {} distinct classes",
            format_count(layer.assigned_records),
            format_count(layer.unique_class_count)
        ),
        (22, 56),
        TextStyle::from(("sans-serif", 17).into_font()).color(&MUTED_COLOR),
    ))?;

    if layer.classes.is_empty() {
        panel.draw(&Text::new(
            "No labels collected yet",
            (22, 110),
            TextStyle::from(("sans-serif", 19).into_font()).color(&TITLE_COLOR),
        ))?;
        return Ok(());
    }

    let shown = layer.shown_classes(top_limit);
    panel.draw(&Text::new(
        format!(
            "Top {} classes listed below; the strip encodes the full distribution.",
            shown.len()
        ),
        (22, 78),
        TextStyle::from(("sans-serif", 15).into_font()).color(&MUTED_COLOR),
    ))?;

    draw_distribution_strip(&panel, layer, layer.layer.color())?;

    let dims = panel.dim_in_pixel();
    let count_style = TextStyle::from(("sans-serif", 15).into_font()).color(&BODY_COLOR);
    let label_style = TextStyle::from(("sans-serif", 15).into_font()).color(&BODY_COLOR);
    let count_anchor_x = dims.0 as i32 - 24;
    let mut row_y = 162_i32;
    for (index, class_count) in shown.iter().enumerate() {
        let bucket_color = ranked_color(layer.layer.color(), index, shown.len());
        let percent = format_percent(class_count.label_count, layer.assigned_records);
        let count_text = format!("{} | {}", percent, format_count(class_count.label_count));
        let (count_width, _) = panel.estimate_text_size(&count_text, &count_style)?;
        let count_x = count_anchor_x - count_width as i32;
        let label_max_width = (count_x - 56).max(80) as u32;
        let label_text =
            truncate_to_width(&panel, &class_count.name, &label_style, label_max_width)?;
        panel.draw(&Rectangle::new(
            [(24, row_y), (38, row_y + 14)],
            bucket_color.filled(),
        ))?;
        panel.draw(&Text::new(label_text, (46, row_y - 2), label_style.clone()))?;
        panel.draw(&Text::new(
            count_text,
            (count_x, row_y - 2),
            count_style.clone(),
        ))?;
        row_y += 26;
    }

    let (tail_classes, tail_count) = layer.tail_summary(shown.len());
    if tail_count > 0 {
        panel.draw(&Text::new(
            format!(
                "Tail: {} classes | {} | {}",
                format_count(tail_classes as u64),
                format_percent(tail_count, layer.assigned_records),
                format_count(tail_count)
            ),
            (24, row_y + 10),
            TextStyle::from(("sans-serif", 15).into_font()).color(&MUTED_COLOR),
        ))?;
    }

    Ok(())
}

fn draw_distribution_strip<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    layer: &TaxonomyLayerSummary,
    color: RGBColor,
) -> std::result::Result<(), DrawingAreaErrorKind<DB::ErrorType>> {
    let left = 24_i32;
    let top = 100_i32;
    let right = area.dim_in_pixel().0 as i32 - 24;
    let bottom = 136_i32;
    let width = right - left;
    let total = layer.assigned_records.max(1);

    area.draw(&Rectangle::new(
        [(left, top), (right, bottom)],
        ShapeStyle::from(&CARD_BORDER).stroke_width(1),
    ))?;

    let mut cumulative = 0_u64;
    let last_index = layer.classes.len().saturating_sub(1);
    for (index, class_count) in layer.classes.iter().enumerate() {
        let start_x = left + ((cumulative as f64 / total as f64) * f64::from(width)).round() as i32;
        cumulative += class_count.label_count;
        let end_x = if index == last_index {
            right
        } else {
            left + ((cumulative as f64 / total as f64) * f64::from(width)).round() as i32
        };
        if end_x <= start_x {
            continue;
        }
        area.draw(&Rectangle::new(
            [(start_x, top), (end_x, bottom)],
            ranked_color(color, index, layer.classes.len()).filled(),
        ))?;
    }

    Ok(())
}

fn draw_legend_entry<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    origin: (i32, i32),
    color: RGBColor,
    text: String,
) -> std::result::Result<(), DrawingAreaErrorKind<DB::ErrorType>> {
    area.draw(&Rectangle::new(
        [origin, (origin.0 + 14, origin.1 + 14)],
        color.filled(),
    ))?;
    area.draw(&Text::new(
        text,
        (origin.0 + 22, origin.1 - 2),
        TextStyle::from(("sans-serif", 15).into_font()).color(&BODY_COLOR),
    ))?;
    Ok(())
}

fn ranked_color(base: RGBColor, index: usize, total: usize) -> RGBAColor {
    let total = total.max(1);
    let fade = index as f64 / total as f64;
    let opacity = (0.92 - fade * 0.55).clamp(0.28, 0.92);
    base.mix(opacity)
}

fn prepare_panel<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
) -> std::result::Result<DrawingArea<DB, Shift>, DrawingAreaErrorKind<DB::ErrorType>> {
    area.fill(&CARD_BACKGROUND)?;
    let (width, height) = area.dim_in_pixel();
    area.draw(&Rectangle::new(
        [(0, 0), (width as i32 - 1, height as i32 - 1)],
        ShapeStyle::from(&CARD_BORDER).stroke_width(1),
    ))?;
    Ok(area.clone())
}

fn draw_centered_text<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    text: &str,
    center_x: i32,
    y: i32,
    style: &TextStyle<'_>,
) -> std::result::Result<(), DrawingAreaErrorKind<DB::ErrorType>> {
    let (text_width, _) = area.estimate_text_size(text, style)?;
    area.draw(&Text::new(
        text.to_owned(),
        (center_x - text_width as i32 / 2, y),
        style.clone(),
    ))
}

fn truncate_to_width<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    label: &str,
    style: &TextStyle<'_>,
    max_width: u32,
) -> std::result::Result<String, DrawingAreaErrorKind<DB::ErrorType>> {
    let (label_width, _) = area.estimate_text_size(label, style)?;
    if label_width <= max_width {
        return Ok(label.to_owned());
    }

    let mut truncated = label.to_owned();
    while !truncated.is_empty() {
        truncated.pop();
        while truncated.ends_with(char::is_whitespace) {
            truncated.pop();
        }
        let candidate = format!("{truncated}…");
        let (candidate_width, _) = area.estimate_text_size(&candidate, style)?;
        if candidate_width <= max_width {
            return Ok(candidate);
        }
    }

    Ok("…".to_owned())
}

fn wrap_text_to_width<DB: DrawingBackend>(
    area: &DrawingArea<DB, Shift>,
    text: &str,
    style: &TextStyle<'_>,
    max_width: u32,
    max_lines: usize,
) -> std::result::Result<Vec<String>, DrawingAreaErrorKind<DB::ErrorType>> {
    if max_lines <= 1 {
        return Ok(vec![truncate_to_width(area, text, style, max_width)?]);
    }

    let (full_width, _) = area.estimate_text_size(text, style)?;
    if full_width <= max_width {
        return Ok(vec![text.to_owned()]);
    }

    let words = text.split_whitespace().collect::<Vec<_>>();
    if words.len() <= 1 {
        return Ok(vec![truncate_to_width(area, text, style, max_width)?]);
    }

    let mut lines = Vec::new();
    let mut current = String::new();
    let mut index = 0usize;
    while index < words.len() {
        let candidate = if current.is_empty() {
            words[index].to_owned()
        } else {
            format!("{current} {}", words[index])
        };
        let (candidate_width, _) = area.estimate_text_size(&candidate, style)?;
        if candidate_width <= max_width {
            current = candidate;
            index += 1;
            continue;
        }

        if current.is_empty() {
            current = truncate_to_width(area, words[index], style, max_width)?;
            index += 1;
        }
        lines.push(current);
        current = String::new();

        if lines.len() == max_lines - 1 {
            let remaining = words[index..].join(" ");
            lines.push(truncate_to_width(area, &remaining, style, max_width)?);
            return Ok(lines);
        }
    }

    if !current.is_empty() {
        lines.push(current);
    }

    Ok(lines)
}

fn display_class_name(name: &str) -> String {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        "<unnamed>".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn format_percent(numerator: u64, denominator: u64) -> String {
    format!("{:.2}%", safe_ratio(numerator, denominator) * 100.0)
}

fn safe_ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn format_count(value: u64) -> String {
    let raw = value.to_string();
    let mut formatted = String::with_capacity(raw.len() + raw.len() / 3);
    let mut remaining = raw.len();
    for character in raw.chars() {
        formatted.push(character);
        remaining -= 1;
        if remaining > 0 && remaining.is_multiple_of(3) {
            formatted.push(',');
        }
    }
    formatted
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn summarizes_local_output_dir() {
        let dir = tempdir().unwrap();
        let output_dir = dir.path().join("run");
        fs::create_dir_all(output_dir.join(SUCCESS_DIR_NAME)).unwrap();
        fs::create_dir_all(output_dir.join(RELEASE_DIR_NAME)).unwrap();

        let input_path = dir.path().join("pubchem.tsv");
        fs::write(
            &input_path,
            "1\tInChI=1S/CH4/h1H4\tVNWKTOKETHGBQD-UHFFFAOYSA-N\n2\tInChI=1S/H2O/h1H2\tXLYOFNOQVPJJNP-UHFFFAOYSA-N\n3\tInChI=1S/C2H6/c1-2/h1-2H3\tOTMSDBZUPAUEDD-UHFFFAOYSA-N\n4\tInChI=1S/O2/c1-2\tMYMOFIZGZYHOMD-UHFFFAOYSA-N\n5\tInChI=1S/CO2/c2-1-3\tCURLTUGMZLYLDI-UHFFFAOYSA-N\n",
        )
        .unwrap();
        fs::write(
            output_dir.join(CHECKPOINT_NAME),
            serde_json::to_vec_pretty(&serde_json::json!({
                "input_path": input_path,
                "input_size_bytes": 1,
                "input_mtime_epoch": 1,
                "current_row_index": 5,
                "current_success_shard_id": 1,
                "current_success_records": 2,
                "ntfy_topic": "topic",
            }))
            .unwrap(),
        )
        .unwrap();

        write_bitmap(&output_dir.join(SUCCESS_BITMAP_NAME), &[0, 1]);
        write_bitmap(&output_dir.join(MISS_BITMAP_NAME), &[2]);
        write_bitmap(&output_dir.join(INVALID_BITMAP_NAME), &[3]);
        write_bitmap(&output_dir.join(FAILED_BITMAP_NAME), &[4]);
        write_success_shard(
            &output_dir
                .join(SUCCESS_DIR_NAME)
                .join("success-000001.jsonl.zst"),
            &[
                serde_json::json!({
                    "classyfire": {
                        "kingdom": { "name": "Organic compounds", "chemont_id": "CHEMONTID:0000000" },
                        "superclass": { "name": "Hydrocarbons", "chemont_id": "CHEMONTID:0000001" },
                        "direct_parent": { "name": "Alkanes", "chemont_id": "CHEMONTID:0002500" }
                    }
                }),
                serde_json::json!({
                    "classyfire": {
                        "kingdom": { "name": "Organic compounds", "chemont_id": "CHEMONTID:0000000" },
                        "class": { "name": "Benzenoids", "chemont_id": "CHEMONTID:0000234" },
                        "subclass": { "name": "Benzene and substituted derivatives", "chemont_id": "CHEMONTID:0000456" },
                        "direct_parent": { "name": "Oxides", "chemont_id": "CHEMONTID:0000999" }
                    }
                }),
            ],
        );

        let summary = build_local_progress_summary(&output_dir, None).unwrap();

        assert_eq!(summary.pubchem_total_rows, 5);
        assert_eq!(summary.counts.success_rows, 2);
        assert_eq!(summary.counts.miss_rows, 1);
        assert_eq!(summary.counts.invalid_rows, 1);
        assert_eq!(summary.counts.failed_rows, 1);
        assert_eq!(summary.taxonomy_layers[0].unique_class_count, 1);
        assert_eq!(summary.taxonomy_layers[0].classes[0].label_count, 2);
        assert_eq!(summary.taxonomy_layers[1].assigned_records, 1);
        assert_eq!(summary.taxonomy_layers[2].classes[0].name, "Benzenoids");
        assert_eq!(summary.taxonomy_layers[3].classes[0].label_count, 1);
        assert_eq!(summary.taxonomy_layers[4].unique_class_count, 2);
    }

    #[test]
    fn summary_round_trips_through_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("progress-summary.json");
        let summary = ProgressSummary {
            generated_at: "2026-04-14T12:00:00Z".to_owned(),
            pubchem_total_rows: 10,
            counts: CrawlCounts {
                success_rows: 4,
                miss_rows: 3,
                invalid_rows: 2,
                failed_rows: 1,
            },
            taxonomy_layers: vec![TaxonomyLayerSummary {
                layer: TaxonomyLayer::Kingdom,
                assigned_records: 4,
                unique_class_count: 2,
                classes: vec![
                    TaxonomyClassCount {
                        name: "Organic compounds".to_owned(),
                        chemont_id: Some("CHEMONTID:1".to_owned()),
                        label_count: 3,
                    },
                    TaxonomyClassCount {
                        name: "Inorganic compounds".to_owned(),
                        chemont_id: Some("CHEMONTID:2".to_owned()),
                        label_count: 1,
                    },
                ],
            }],
        };

        write_progress_summary(&path, &summary).unwrap();
        let restored = load_progress_summary(&path).unwrap();

        assert_eq!(summary, restored);
    }

    #[test]
    fn renders_svg_report() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("report.svg");
        let summary = ProgressSummary {
            generated_at: "2026-04-14T12:00:00Z".to_owned(),
            pubchem_total_rows: 100,
            counts: CrawlCounts {
                success_rows: 12,
                miss_rows: 4,
                invalid_rows: 1,
                failed_rows: 3,
            },
            taxonomy_layers: TaxonomyLayer::ALL
                .into_iter()
                .enumerate()
                .map(|(index, layer)| TaxonomyLayerSummary {
                    layer,
                    assigned_records: 12 - index as u64,
                    unique_class_count: 3 + index as u64,
                    classes: vec![
                        TaxonomyClassCount {
                            name: format!("{} class A", layer.label()),
                            chemont_id: None,
                            label_count: 6 - index as u64,
                        },
                        TaxonomyClassCount {
                            name: format!("{} class B", layer.label()),
                            chemont_id: None,
                            label_count: 3,
                        },
                    ],
                })
                .collect(),
        };

        render_progress_svg(&path, &summary, "local crawl state").unwrap();

        let svg = fs::read_to_string(&path).unwrap();
        assert!(svg.contains("<svg"));
        assert!(svg.contains("Collected vs PubChem"));
        assert!(svg.contains("Executed Request Outcomes"));
        assert!(svg.contains("Direct parent Breakdown"));
    }

    #[test]
    fn dataset_summary_uses_manifest_counts() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join(RELEASE_DATASET_FILENAME);
        write_success_shard(
            &dataset_path,
            &[serde_json::json!({
                "classyfire": {
                    "kingdom": { "name": "Organic compounds", "chemont_id": "CHEMONTID:0000000" },
                    "direct_parent": { "name": "Alkanes", "chemont_id": "CHEMONTID:0002500" }
                }
            })],
        );

        let summary = build_progress_summary_from_dataset(
            &dataset_path,
            10,
            CrawlCounts {
                success_rows: 1,
                miss_rows: 2,
                invalid_rows: 3,
                failed_rows: 4,
            },
        )
        .unwrap();

        let as_json: Value = serde_json::to_value(&summary).unwrap();
        assert_eq!(as_json["counts"]["miss_rows"], 2);
        assert_eq!(as_json["taxonomy_layers"][4]["unique_class_count"], 1);
        assert_eq!(
            as_json["taxonomy_layers"][4]["classes"][0]["label_count"],
            1
        );
    }

    #[test]
    fn aggregate_taxonomy_skips_missing_paths() {
        let dir = tempdir().unwrap();
        let existing_path = dir.path().join("success-000001.jsonl.zst");
        let missing_path = dir.path().join("success-000002.jsonl.zst");
        write_success_shard(
            &existing_path,
            &[serde_json::json!({
                "classyfire": {
                    "kingdom": { "name": "Organic compounds", "chemont_id": "CHEMONTID:0000000" },
                    "direct_parent": { "name": "Alkanes", "chemont_id": "CHEMONTID:0002500" }
                }
            })],
        );

        let layers = aggregate_taxonomy(&[missing_path, existing_path]).unwrap();

        assert_eq!(layers[0].assigned_records, 1);
        assert_eq!(layers[0].classes[0].name, "Organic compounds");
        assert_eq!(layers[4].assigned_records, 1);
        assert_eq!(layers[4].classes[0].name, "Alkanes");
    }

    #[test]
    fn tail_summary_counts_hidden_classes() {
        let layer = TaxonomyLayerSummary {
            layer: TaxonomyLayer::DirectParent,
            assigned_records: 10,
            unique_class_count: 4,
            classes: vec![
                TaxonomyClassCount {
                    name: "A".to_owned(),
                    chemont_id: None,
                    label_count: 4,
                },
                TaxonomyClassCount {
                    name: "B".to_owned(),
                    chemont_id: None,
                    label_count: 3,
                },
                TaxonomyClassCount {
                    name: "C".to_owned(),
                    chemont_id: None,
                    label_count: 2,
                },
                TaxonomyClassCount {
                    name: "D".to_owned(),
                    chemont_id: None,
                    label_count: 1,
                },
            ],
        };

        let visible = layer.shown_classes(3);
        let tail = layer.tail_summary(visible.len());
        assert_eq!(visible.len(), 3);
        assert_eq!(visible[2].name, "C");
        assert_eq!(tail, (1, 1));
    }

    fn write_bitmap(path: &Path, rows: &[u64]) {
        let size = rows
            .iter()
            .copied()
            .max()
            .map_or(1, |row| row as usize / 8 + 1);
        let mut bytes = vec![0_u8; size];
        for row in rows {
            let byte_index = (*row / 8) as usize;
            let bit_mask = 1_u8 << (*row % 8);
            bytes[byte_index] |= bit_mask;
        }
        fs::write(path, bytes).unwrap();
    }

    fn write_success_shard(path: &Path, records: &[serde_json::Value]) {
        let file = File::create(path).unwrap();
        let mut encoder = zstd::stream::write::Encoder::new(file, 3).unwrap();
        for record in records {
            serde_json::to_writer(&mut encoder, record).unwrap();
            encoder.write_all(b"\n").unwrap();
        }
        encoder.finish().unwrap();
    }
}
