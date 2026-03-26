use crate::api::{validate_entity_body, ApiClient};
use crate::config::{NotifyZenodoReleaseConfig, StreamConfig, ZenodoConfig};
use crate::ui::Ui;
use crate::zenodo::{publish as publish_zenodo_release, PublishStats};
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Days, TimeZone, Utc};
use memmap2::{MmapMut, MmapOptions};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, sleep};
use std::time::{Duration, Instant, UNIX_EPOCH};
use uuid::Uuid;

const BITMAP_GROWTH_BITS: u64 = 1 << 20;
const SUCCESS_BITMAP_NAME: &str = "state.success.bits";
const MISS_BITMAP_NAME: &str = "state.miss.bits";
const INVALID_BITMAP_NAME: &str = "state.invalid.bits";
const FAILED_BITMAP_NAME: &str = "state.failed.bits";
const CHECKPOINT_NAME: &str = "checkpoint.json";
const RELEASE_DATASET_FILENAME: &str = "classyfire-labels.jsonl.zst";
const RELEASE_DIR_NAME: &str = "releases";
const RELEASE_MANIFEST_FILENAME: &str = "manifest.json";
const SUCCESS_DIR_NAME: &str = "success";

static CTRLC_HANDLER_INSTALLED: OnceLock<()> = OnceLock::new();
static CTRLC_TARGET: OnceLock<Mutex<Option<CtrlcTarget>>> = OnceLock::new();

#[derive(Clone)]
struct CtrlcTarget {
    running: Arc<AtomicBool>,
    ui: Arc<Ui>,
}

pub fn run_streaming(config: StreamConfig) -> Result<()> {
    config.validate()?;
    ensure_output_dirs(&config.output_dir)?;

    let input_meta = read_input_metadata(&config.input)?;
    let checkpoint_path = config.output_dir.join(CHECKPOINT_NAME);
    let mut checkpoint = load_or_init_checkpoint(&checkpoint_path, &config.input, &input_meta)?;
    let ntfy_topic = checkpoint.ensure_ntfy_topic();
    save_checkpoint(&checkpoint_path, &checkpoint)?;
    let counters = Arc::new(ProgressCounters::from_snapshot(
        StateBitmaps::open(&config.output_dir, checkpoint.current_row_index + 1)?
            .snapshot_counts()?,
    ));

    let running = Arc::new(AtomicBool::new(true));
    let get_limiter = Arc::new(GetRateLimiter::new(config.get_sleep_seconds));
    let ui = Arc::new(Ui::new());
    let ntfy_client = Arc::new(NtfyClient::new(
        &config.ntfy_base_url,
        ntfy_topic,
        &config.user_agent,
        config.timeout_seconds,
    )?);
    let ntfy_url = ntfy_client.subscription_url();
    ui.set_ntfy_url(&ntfy_url);

    eprintln!("ntfy status URL: {ntfy_url}");
    let startup_snapshot = counters.snapshot();
    if let Err(error) = ntfy_client.publish_started(
        &ntfy_url,
        &config.input,
        &config.output_dir,
        startup_snapshot,
    ) {
        ui.info(format!("ntfy startup notification failed: {error:#}"));
    } else {
        ui.info("published ntfy startup notification");
    }

    install_or_update_ctrlc_handler(Arc::clone(&running), Arc::clone(&ui))?;

    let worker_handle = {
        let running = Arc::clone(&running);
        let get_limiter = Arc::clone(&get_limiter);
        let ui = Arc::clone(&ui);
        let counters = Arc::clone(&counters);
        let ntfy_client = Arc::clone(&ntfy_client);
        let config = config.clone();
        thread::spawn(move || {
            run_stream_worker(running, get_limiter, ui, counters, ntfy_client, config)
        })
    };

    let reporter_handle = if ui.is_interactive() {
        let running = Arc::clone(&running);
        let get_limiter = Arc::clone(&get_limiter);
        let ui = Arc::clone(&ui);
        let status_interval_seconds = config.status_interval_seconds;
        Some(thread::spawn(move || {
            run_stream_reporter(running, get_limiter, ui, status_interval_seconds)
        }))
    } else {
        None
    };

    let ntfy_handle = {
        let running = Arc::clone(&running);
        let counters = Arc::clone(&counters);
        let ui = Arc::clone(&ui);
        let ntfy_client = Arc::clone(&ntfy_client);
        Some(thread::spawn(move || {
            run_ntfy_reporter(running, counters, ui, ntfy_client)
        }))
    };

    worker_handle
        .join()
        .map_err(|_| anyhow!("stream worker panicked"))??;
    running.store(false, Ordering::SeqCst);
    if let Some(reporter_handle) = reporter_handle {
        reporter_handle
            .join()
            .map_err(|_| anyhow!("stream reporter panicked"))??;
    }
    if let Some(ntfy_handle) = ntfy_handle {
        ntfy_handle
            .join()
            .map_err(|_| anyhow!("ntfy reporter panicked"))??;
    }
    Ok(())
}

pub fn notify_zenodo_release(config: NotifyZenodoReleaseConfig) -> Result<()> {
    config.validate()?;
    let checkpoint_path = config.output_dir.join(CHECKPOINT_NAME);
    let checkpoint = load_checkpoint_file(&checkpoint_path)?;
    let topic = checkpoint
        .ntfy_topic
        .ok_or_else(|| anyhow!("checkpoint does not contain an ntfy topic"))?;
    let ntfy_client = NtfyClient::new(
        &config.ntfy_base_url,
        topic,
        &config.user_agent,
        config.timeout_seconds,
    )?;

    eprintln!("ntfy status URL: {}", ntfy_client.subscription_url());
    ntfy_client.publish_zenodo_release_completed(&config.record_url)
}

fn run_stream_worker(
    running: Arc<AtomicBool>,
    get_limiter: Arc<GetRateLimiter>,
    ui: Arc<Ui>,
    counters: Arc<ProgressCounters>,
    ntfy_client: Arc<NtfyClient>,
    config: StreamConfig,
) -> Result<()> {
    ensure_output_dirs(&config.output_dir)?;
    ui.info(format!(
        "streaming {} into {}",
        config.input.display(),
        config.output_dir.display()
    ));

    let input_meta = read_input_metadata(&config.input)?;
    let checkpoint_path = config.output_dir.join(CHECKPOINT_NAME);
    let mut checkpoint = load_or_init_checkpoint(&checkpoint_path, &config.input, &input_meta)?;
    let mut bitmaps = StateBitmaps::open(&config.output_dir, checkpoint.current_row_index + 1)?;
    let mut writer = SuccessShardWriter::open(
        success_dir(&config.output_dir),
        checkpoint.current_success_shard_id,
        checkpoint.current_success_records,
        config.success_shard_max_records,
        config.success_shard_max_bytes,
    )?;
    save_checkpoint(&checkpoint_path, &checkpoint)?;
    let zenodo_config = config.zenodo_config();
    let mut last_publish = Instant::now();

    let client = ApiClient::new(&config.base_url, &config.user_agent, config.timeout_seconds)?;
    let mut reader = open_input_reader(&config.input)?;
    let mut line = String::new();
    let mut row_index = 0_u64;

    loop {
        if !running.load(Ordering::SeqCst) {
            break;
        }

        if last_publish.elapsed() >= Duration::from_secs(zenodo_config.publish_interval_seconds) {
            publish_current_snapshot_to_zenodo(
                &mut writer,
                PublishContext {
                    checkpoint_path: &checkpoint_path,
                    checkpoint: &mut checkpoint,
                    counters: &counters,
                    output_dir: &config.output_dir,
                    zenodo_config: &zenodo_config,
                    ntfy_client: &ntfy_client,
                    ui: &ui,
                },
            )?;
            last_publish = Instant::now();
        }

        line.clear();
        let bytes_read = reader
            .read_line(&mut line)
            .with_context(|| format!("failed reading {}", config.input.display()))?;
        if bytes_read == 0 {
            checkpoint.current_row_index = row_index;
            save_checkpoint(&checkpoint_path, &checkpoint)?;
            break;
        }

        let current_row = row_index;
        row_index += 1;
        if bitmaps.terminal_state(current_row)?.is_some() {
            continue;
        }

        let trimmed = line.trim_end_matches(['\n', '\r']);
        let (cid, inchi, inchikey) = match parse_pubchem_line(trimmed) {
            Ok(parts) => parts,
            Err(error) => {
                ui.note_error(&format!("row-{current_row}"), &format!("{error:#}"));
                persist_terminal_state(
                    &mut bitmaps,
                    &checkpoint_path,
                    &mut checkpoint,
                    &counters,
                    current_row,
                    RowState::InvalidInput,
                    &writer,
                )?;
                continue;
            }
        };

        if !is_valid_inchikey(&inchikey) {
            ui.note_error(&inchikey, "invalid inchikey format in PubChem input");
            persist_terminal_state(
                &mut bitmaps,
                &checkpoint_path,
                &mut checkpoint,
                &counters,
                current_row,
                RowState::InvalidInput,
                &writer,
            )?;
            continue;
        }

        ui.note_current_key(&inchikey);
        if !get_limiter.acquire(&running) {
            break;
        }

        match client
            .fetch_entity(&inchikey)
            .and_then(|body| validate_entity_body(&body).map(|is_classified| (body, is_classified)))
        {
            Ok((body, true)) => {
                let rotated = writer.write_record(&SuccessRecord::new(
                    current_row,
                    cid,
                    &inchi,
                    &inchikey,
                    &body.body,
                )?)?;
                if rotated {
                    ui.info(format!("opened success shard {}", writer.shard_id()));
                }
                ui.note_hit(&inchikey);
                persist_terminal_state(
                    &mut bitmaps,
                    &checkpoint_path,
                    &mut checkpoint,
                    &counters,
                    current_row,
                    RowState::Success,
                    &writer,
                )?;
            }
            Ok((_body, false)) => {
                ui.note_miss(&inchikey);
                persist_terminal_state(
                    &mut bitmaps,
                    &checkpoint_path,
                    &mut checkpoint,
                    &counters,
                    current_row,
                    RowState::Miss,
                    &writer,
                )?;
            }
            Err(error) => {
                let error_text = format!("{error:#}");
                if is_throttled_or_html(&error_text) {
                    get_limiter.backoff(config.throttle_backoff_seconds);
                    ui.note_backoff(
                        config.throttle_backoff_seconds,
                        classify_backoff_reason(&error_text),
                    );
                }
                ui.note_error(&inchikey, &error_text);
                persist_terminal_state(
                    &mut bitmaps,
                    &checkpoint_path,
                    &mut checkpoint,
                    &counters,
                    current_row,
                    RowState::Failed,
                    &writer,
                )?;
            }
        }
    }

    writer.finish()?;
    bitmaps.flush_all()?;
    save_checkpoint(&checkpoint_path, &checkpoint)?;
    Ok(())
}

fn run_ntfy_reporter(
    running: Arc<AtomicBool>,
    counters: Arc<ProgressCounters>,
    ui: Arc<Ui>,
    ntfy_client: Arc<NtfyClient>,
) -> Result<()> {
    loop {
        let now = Utc::now();
        let next_publish_at = next_ntfy_publish_after(now);
        let wait_for = (next_publish_at - now).to_std().unwrap_or(Duration::ZERO);
        if !sleep_until_stop_with_granularity(&running, wait_for, Duration::from_secs(1)) {
            break;
        }
        let snapshot = counters.snapshot();
        if let Err(error) = ntfy_client.publish_daily_status(snapshot) {
            ui.info(format!("ntfy publish failed: {error:#}"));
        } else {
            ui.info("published ntfy daily status");
        }
    }
    Ok(())
}

fn run_stream_reporter(
    running: Arc<AtomicBool>,
    get_limiter: Arc<GetRateLimiter>,
    ui: Arc<Ui>,
    status_interval_seconds: u64,
) -> Result<()> {
    let _terminal = ui.enter_terminal()?;
    while running.load(Ordering::SeqCst) {
        if ui.is_interactive() {
            ui.render_dashboard(get_limiter.seconds_until_ready())?;
        }
        if !sleep_until_stop(&running, status_interval_seconds) {
            break;
        }
    }
    Ok(())
}

fn install_or_update_ctrlc_handler(running: Arc<AtomicBool>, ui: Arc<Ui>) -> Result<()> {
    let target = CTRLC_TARGET.get_or_init(|| Mutex::new(None));
    *target.lock().expect("ctrl-c target mutex poisoned") = Some(CtrlcTarget { running, ui });

    if CTRLC_HANDLER_INSTALLED.get().is_none() {
        ctrlc::set_handler(move || {
            if let Some(target) = CTRLC_TARGET
                .get()
                .and_then(|state| state.lock().ok().and_then(|guard| guard.clone()))
            {
                if target.running.swap(false, Ordering::SeqCst) {
                    target.ui.info("stopping streaming runner");
                }
            }
        })
        .context("failed to install ctrl-c handler")?;
        let _ = CTRLC_HANDLER_INSTALLED.set(());
    }

    Ok(())
}

fn persist_terminal_state(
    bitmaps: &mut StateBitmaps,
    checkpoint_path: &Path,
    checkpoint: &mut Checkpoint,
    counters: &ProgressCounters,
    row_index: u64,
    state: RowState,
    writer: &SuccessShardWriter,
) -> Result<()> {
    bitmaps.set_state(row_index, state)?;
    bitmaps.flush_row(row_index, state)?;
    counters.increment(state);
    checkpoint.current_row_index = row_index + 1;
    checkpoint.current_success_shard_id = writer.shard_id();
    checkpoint.current_success_records = writer.records_in_shard();
    save_checkpoint(checkpoint_path, checkpoint)
}

struct PublishContext<'a> {
    checkpoint_path: &'a Path,
    checkpoint: &'a mut Checkpoint,
    counters: &'a ProgressCounters,
    output_dir: &'a Path,
    zenodo_config: &'a ZenodoConfig,
    ntfy_client: &'a NtfyClient,
    ui: &'a Ui,
}

fn publish_current_snapshot_to_zenodo(
    writer: &mut SuccessShardWriter,
    ctx: PublishContext<'_>,
) -> Result<()> {
    let snapshot = ctx.counters.snapshot();
    if snapshot.success == 0 {
        ctx.ui
            .info("skipping Zenodo publish: no successful rows yet");
        return Ok(());
    }

    let sealed_current = writer.seal_current()?;
    ctx.checkpoint.current_success_shard_id = writer.shard_id();
    ctx.checkpoint.current_success_records = writer.records_in_shard();
    save_checkpoint(ctx.checkpoint_path, ctx.checkpoint)?;

    let Some(release) = build_release_artifacts(ctx.output_dir, snapshot)? else {
        if sealed_current {
            ctx.ui
                .info("skipping Zenodo publish: no non-empty success shards found");
        }
        return Ok(());
    };

    let publish_result = publish_zenodo_release(
        &ctx.zenodo_config.token,
        &ctx.zenodo_config.deposit_id,
        &release.output_path,
        &release.manifest_path,
        PublishStats {
            success: snapshot.success,
            miss: snapshot.miss,
            invalid: snapshot.invalid,
            failed: snapshot.failed,
        },
    );

    match publish_result {
        Ok(published) => {
            ctx.ui
                .info(format!("published Zenodo release {}", published.record_url));
            if let Err(error) = ctx
                .ntfy_client
                .publish_zenodo_release_completed_with_counts(&published.record_url, snapshot)
            {
                ctx.ui
                    .info(format!("ntfy release notification failed: {error:#}"));
            }
            fs::remove_file(&release.output_path)
                .with_context(|| format!("failed removing {}", release.output_path.display()))?;
            fs::remove_file(&release.manifest_path)
                .with_context(|| format!("failed removing {}", release.manifest_path.display()))?;
        }
        Err(error) => {
            ctx.ui.info(format!("Zenodo publish failed: {error:#}"));
            if let Err(ntfy_error) = ctx
                .ntfy_client
                .publish_zenodo_release_failed(&error.to_string())
            {
                ctx.ui.info(format!(
                    "ntfy release failure notification failed: {ntfy_error:#}"
                ));
            }
        }
    }

    Ok(())
}

fn ensure_output_dirs(output_dir: &Path) -> Result<()> {
    fs::create_dir_all(success_dir(output_dir))
        .with_context(|| format!("failed creating {}", success_dir(output_dir).display()))?;
    fs::create_dir_all(release_dir(output_dir))
        .with_context(|| format!("failed creating {}", release_dir(output_dir).display()))?;
    Ok(())
}

fn success_dir(output_dir: &Path) -> PathBuf {
    output_dir.join(SUCCESS_DIR_NAME)
}

fn release_dir(output_dir: &Path) -> PathBuf {
    output_dir.join(RELEASE_DIR_NAME)
}

fn read_input_metadata(path: &Path) -> Result<InputMetadata> {
    let metadata =
        fs::metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    let mtime_epoch = metadata
        .modified()
        .context("missing file modified time")?
        .duration_since(UNIX_EPOCH)
        .context("modified time predates unix epoch")?
        .as_secs();
    Ok(InputMetadata {
        size_bytes: metadata.len(),
        mtime_epoch,
    })
}

fn open_input_reader(path: &Path) -> Result<Box<dyn BufRead>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    if path.extension().and_then(|ext| ext.to_str()) == Some("zst") {
        let decoder = zstd::stream::read::Decoder::new(BufReader::new(file))
            .with_context(|| format!("failed opening zstd decoder for {}", path.display()))?;
        Ok(Box::new(BufReader::new(decoder)))
    } else {
        Ok(Box::new(BufReader::with_capacity(8 * 1024 * 1024, file)))
    }
}

fn parse_pubchem_line(line: &str) -> Result<(i64, String, String)> {
    let mut parts = line.splitn(3, '\t');
    let cid = parts
        .next()
        .ok_or_else(|| anyhow!("missing CID"))?
        .trim()
        .parse::<i64>()
        .context("CID is not an integer")?;
    let inchi = parts
        .next()
        .ok_or_else(|| anyhow!("missing InChI"))?
        .trim()
        .to_owned();
    let inchikey = parts
        .next()
        .ok_or_else(|| anyhow!("missing InChIKey"))?
        .trim()
        .to_owned();

    if inchi.is_empty() || inchikey.is_empty() {
        bail!("empty InChI or InChIKey");
    }

    Ok((cid, inchi, inchikey))
}

fn load_or_init_checkpoint(
    checkpoint_path: &Path,
    input_path: &Path,
    input_meta: &InputMetadata,
) -> Result<Checkpoint> {
    if checkpoint_path.exists() {
        let checkpoint = load_checkpoint_file(checkpoint_path)?;
        if checkpoint.input_path != input_path.to_string_lossy() {
            bail!("checkpoint input path does not match the current input");
        }
        if checkpoint.input_size_bytes != input_meta.size_bytes {
            bail!("checkpoint input size does not match the current input");
        }
        if checkpoint.input_mtime_epoch != input_meta.mtime_epoch {
            bail!("checkpoint input mtime does not match the current input");
        }
        return Ok(checkpoint);
    }

    Ok(Checkpoint {
        input_path: input_path.to_string_lossy().into_owned(),
        input_size_bytes: input_meta.size_bytes,
        input_mtime_epoch: input_meta.mtime_epoch,
        current_row_index: 0,
        current_success_shard_id: 1,
        current_success_records: 0,
        ntfy_topic: None,
    })
}

fn load_checkpoint_file(checkpoint_path: &Path) -> Result<Checkpoint> {
    serde_json::from_slice(
        &fs::read(checkpoint_path)
            .with_context(|| format!("failed reading {}", checkpoint_path.display()))?,
    )
    .with_context(|| format!("failed parsing {}", checkpoint_path.display()))
}

fn save_checkpoint(path: &Path, checkpoint: &Checkpoint) -> Result<()> {
    let tmp_path = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(checkpoint).context("failed serializing checkpoint")?;
    fs::write(&tmp_path, bytes)
        .with_context(|| format!("failed writing {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed renaming {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowState {
    Success,
    Miss,
    InvalidInput,
    Failed,
}

struct StateBitmaps {
    success: MappedBitmap,
    miss: MappedBitmap,
    invalid: MappedBitmap,
    failed: MappedBitmap,
}

impl StateBitmaps {
    fn open(output_dir: &Path, min_bits: u64) -> Result<Self> {
        Ok(Self {
            success: MappedBitmap::open(&output_dir.join(SUCCESS_BITMAP_NAME), min_bits)?,
            miss: MappedBitmap::open(&output_dir.join(MISS_BITMAP_NAME), min_bits)?,
            invalid: MappedBitmap::open(&output_dir.join(INVALID_BITMAP_NAME), min_bits)?,
            failed: MappedBitmap::open(&output_dir.join(FAILED_BITMAP_NAME), min_bits)?,
        })
    }

    fn terminal_state(&mut self, row_index: u64) -> Result<Option<RowState>> {
        let success = self.success.get(row_index)?;
        let miss = self.miss.get(row_index)?;
        let invalid = self.invalid.get(row_index)?;
        let failed = self.failed.get(row_index)?;
        let count = success as u8 + miss as u8 + invalid as u8 + failed as u8;
        if count > 1 {
            bail!("row {row_index} has multiple terminal states");
        }
        Ok(if success {
            Some(RowState::Success)
        } else if miss {
            Some(RowState::Miss)
        } else if invalid {
            Some(RowState::InvalidInput)
        } else if failed {
            Some(RowState::Failed)
        } else {
            None
        })
    }

    fn set_state(&mut self, row_index: u64, state: RowState) -> Result<()> {
        if self.terminal_state(row_index)?.is_some() {
            bail!("row {row_index} already has a terminal state");
        }
        match state {
            RowState::Success => self.success.set(row_index),
            RowState::Miss => self.miss.set(row_index),
            RowState::InvalidInput => self.invalid.set(row_index),
            RowState::Failed => self.failed.set(row_index),
        }
    }

    fn flush_row(&self, row_index: u64, state: RowState) -> Result<()> {
        match state {
            RowState::Success => self.success.flush_row(row_index),
            RowState::Miss => self.miss.flush_row(row_index),
            RowState::InvalidInput => self.invalid.flush_row(row_index),
            RowState::Failed => self.failed.flush_row(row_index),
        }
    }

    fn flush_all(&self) -> Result<()> {
        self.success.flush_all()?;
        self.miss.flush_all()?;
        self.invalid.flush_all()?;
        self.failed.flush_all()?;
        Ok(())
    }

    fn snapshot_counts(&self) -> Result<ProgressSnapshot> {
        Ok(ProgressSnapshot {
            success: self.success.count_ones()?,
            miss: self.miss.count_ones()?,
            invalid: self.invalid.count_ones()?,
            failed: self.failed.count_ones()?,
        })
    }
}

struct MappedBitmap {
    file: File,
    map: MmapMut,
    capacity_bits: u64,
}

impl MappedBitmap {
    fn open(path: &Path, min_bits: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("failed opening {}", path.display()))?;
        let mut capacity_bits = fs::metadata(path)
            .with_context(|| format!("failed to stat {}", path.display()))?
            .len()
            * 8;
        if capacity_bits < min_bits.max(1) {
            capacity_bits = round_up_bits(min_bits.max(1));
            file.set_len(bytes_for_bits(capacity_bits))
                .with_context(|| format!("failed sizing {}", path.display()))?;
        }
        let map = unsafe {
            MmapOptions::new()
                .len(bytes_for_bits(capacity_bits) as usize)
                .map_mut(&file)
        }
        .with_context(|| format!("failed mapping {}", path.display()))?;
        Ok(Self {
            file,
            map,
            capacity_bits,
        })
    }

    fn get(&mut self, row_index: u64) -> Result<bool> {
        if row_index >= self.capacity_bits {
            return Ok(false);
        }
        let (byte_index, mask) = bit_position(row_index);
        Ok(self.map[byte_index] & mask != 0)
    }

    fn set(&mut self, row_index: u64) -> Result<()> {
        self.ensure_capacity(row_index + 1)?;
        let (byte_index, mask) = bit_position(row_index);
        self.map[byte_index] |= mask;
        Ok(())
    }

    fn ensure_capacity(&mut self, required_bits: u64) -> Result<()> {
        if required_bits <= self.capacity_bits {
            return Ok(());
        }
        self.capacity_bits = round_up_bits(required_bits);
        self.file
            .set_len(bytes_for_bits(self.capacity_bits))
            .context("failed resizing bitmap file")?;
        self.map = unsafe {
            MmapOptions::new()
                .len(bytes_for_bits(self.capacity_bits) as usize)
                .map_mut(&self.file)
        }
        .context("failed remapping bitmap file")?;
        Ok(())
    }

    fn flush_row(&self, row_index: u64) -> Result<()> {
        if row_index >= self.capacity_bits {
            return Ok(());
        }
        let (byte_index, _) = bit_position(row_index);
        self.map
            .flush_range(byte_index, 1)
            .context("failed flushing bitmap row")
    }

    fn flush_all(&self) -> Result<()> {
        self.map.flush().context("failed flushing bitmap")
    }

    fn count_ones(&self) -> Result<u64> {
        Ok(self.map.iter().map(|byte| byte.count_ones() as u64).sum())
    }
}

fn round_up_bits(required_bits: u64) -> u64 {
    let units = required_bits.div_ceil(BITMAP_GROWTH_BITS);
    units * BITMAP_GROWTH_BITS
}

fn bytes_for_bits(bits: u64) -> u64 {
    bits.div_ceil(8)
}

fn bit_position(row_index: u64) -> (usize, u8) {
    let byte_index = (row_index / 8) as usize;
    let bit_index = (row_index % 8) as u8;
    (byte_index, 1 << bit_index)
}

#[derive(Debug, Clone)]
struct InputMetadata {
    size_bytes: u64,
    mtime_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Checkpoint {
    input_path: String,
    input_size_bytes: u64,
    input_mtime_epoch: u64,
    current_row_index: u64,
    current_success_shard_id: u64,
    current_success_records: u64,
    #[serde(default)]
    ntfy_topic: Option<String>,
}

impl Checkpoint {
    fn ensure_ntfy_topic(&mut self) -> String {
        if let Some(topic) = &self.ntfy_topic {
            return topic.clone();
        }
        let topic = Uuid::new_v4().to_string();
        self.ntfy_topic = Some(topic.clone());
        topic
    }
}

#[derive(Debug, Serialize)]
struct SuccessRecord {
    row_index: u64,
    cid: i64,
    inchi: String,
    inchikey: String,
    fetched_at: String,
    classyfire: Box<RawValue>,
}

impl SuccessRecord {
    fn new(row_index: u64, cid: i64, inchi: &str, inchikey: &str, raw_body: &str) -> Result<Self> {
        let classyfire = serde_json::from_str::<Box<RawValue>>(raw_body)
            .context("failed parsing successful ClassyFire body as raw JSON")?;
        Ok(Self {
            row_index,
            cid,
            inchi: inchi.to_owned(),
            inchikey: inchikey.to_owned(),
            fetched_at: chrono::Utc::now().to_rfc3339(),
            classyfire,
        })
    }
}

struct SuccessShardWriter {
    output_dir: PathBuf,
    shard_id: u64,
    records_in_shard: u64,
    max_records: u64,
    max_bytes: u64,
    encoder: zstd::stream::write::Encoder<'static, CountingWriter<BufWriter<File>>>,
}

impl SuccessShardWriter {
    fn open(
        output_dir: PathBuf,
        shard_id: u64,
        records_in_shard: u64,
        max_records: u64,
        max_bytes: u64,
    ) -> Result<Self> {
        let shard_id = shard_id.max(1);
        let encoder = open_success_encoder(&success_shard_path(&output_dir, shard_id))?;
        Ok(Self {
            output_dir,
            shard_id,
            records_in_shard,
            max_records,
            max_bytes,
            encoder,
        })
    }

    fn write_record(&mut self, record: &SuccessRecord) -> Result<bool> {
        let rotated = self.rotate_if_needed()?;
        serde_json::to_writer(&mut self.encoder, record)
            .context("failed serializing success record")?;
        self.encoder
            .write_all(b"\n")
            .context("failed writing success record newline")?;
        self.encoder
            .flush()
            .context("failed flushing success shard")?;
        self.records_in_shard += 1;
        Ok(rotated)
    }

    fn rotate_if_needed(&mut self) -> Result<bool> {
        if self.records_in_shard < self.max_records && self.compressed_bytes() < self.max_bytes {
            return Ok(false);
        }
        let next_shard_id = self.shard_id + 1;
        let next_encoder =
            open_success_encoder(&success_shard_path(&self.output_dir, next_shard_id))?;
        let previous_encoder = std::mem::replace(&mut self.encoder, next_encoder);
        finish_success_encoder(previous_encoder)?;
        self.shard_id = next_shard_id;
        self.records_in_shard = 0;
        Ok(true)
    }

    fn seal_current(&mut self) -> Result<bool> {
        if self.records_in_shard == 0 && self.compressed_bytes() == 0 {
            return Ok(false);
        }
        let next_shard_id = self.shard_id + 1;
        let next_encoder =
            open_success_encoder(&success_shard_path(&self.output_dir, next_shard_id))?;
        let previous_encoder = std::mem::replace(&mut self.encoder, next_encoder);
        finish_success_encoder(previous_encoder)?;
        self.shard_id = next_shard_id;
        self.records_in_shard = 0;
        Ok(true)
    }

    fn finish(self) -> Result<()> {
        if self.records_in_shard == 0 && self.compressed_bytes() == 0 {
            drop(self.encoder);
            let path = success_shard_path(&self.output_dir, self.shard_id);
            remove_zero_length_file(&path)?;
            return Ok(());
        }
        finish_success_encoder(self.encoder)
    }

    fn shard_id(&self) -> u64 {
        self.shard_id
    }

    fn records_in_shard(&self) -> u64 {
        self.records_in_shard
    }

    fn compressed_bytes(&self) -> u64 {
        self.encoder.get_ref().bytes_written
    }
}

fn success_shard_path(output_dir: &Path, shard_id: u64) -> PathBuf {
    output_dir.join(format!("success-{shard_id:06}.jsonl.zst"))
}

struct ReleaseArtifacts {
    output_path: PathBuf,
    manifest_path: PathBuf,
}

fn build_release_artifacts(
    output_dir: &Path,
    snapshot: ProgressSnapshot,
) -> Result<Option<ReleaseArtifacts>> {
    let mut shards = fs::read_dir(success_dir(output_dir))
        .with_context(|| format!("failed reading {}", success_dir(output_dir).display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("zst"))
        .filter_map(|path| match fs::metadata(&path) {
            Ok(metadata) if metadata.len() > 0 => Some((path, metadata.len())),
            _ => None,
        })
        .collect::<Vec<_>>();
    shards.sort_by(|left, right| left.0.cmp(&right.0));

    if shards.is_empty() {
        return Ok(None);
    }

    let release_dir = release_dir(output_dir);
    let output_path = release_dir.join(RELEASE_DATASET_FILENAME);
    let manifest_path = release_dir.join(RELEASE_MANIFEST_FILENAME);
    let output_tmp = release_dir.join(format!("{RELEASE_DATASET_FILENAME}.tmp"));
    let manifest_tmp = release_dir.join(format!("{RELEASE_MANIFEST_FILENAME}.tmp"));

    {
        let mut output = File::create(&output_tmp)
            .with_context(|| format!("failed creating {}", output_tmp.display()))?;
        for (path, _) in &shards {
            let mut shard =
                File::open(path).with_context(|| format!("failed opening {}", path.display()))?;
            std::io::copy(&mut shard, &mut output)
                .with_context(|| format!("failed appending {}", path.display()))?;
        }
        output
            .sync_all()
            .with_context(|| format!("failed syncing {}", output_tmp.display()))?;
    }

    let manifest = serde_json::json!({
        "generated_at": Utc::now().to_rfc3339(),
        "format": "jsonl.zst",
        "success_rows": snapshot.success,
        "miss_rows": snapshot.miss,
        "invalid_rows": snapshot.invalid,
        "failed_rows": snapshot.failed,
        "shards": shards
            .iter()
            .map(|(path, bytes)| serde_json::json!({
                "filename": path.file_name().and_then(|name| name.to_str()).unwrap_or_default(),
                "bytes": bytes,
            }))
            .collect::<Vec<_>>(),
    });
    fs::write(
        &manifest_tmp,
        serde_json::to_vec_pretty(&manifest).context("failed serializing release manifest")?,
    )
    .with_context(|| format!("failed writing {}", manifest_tmp.display()))?;

    fs::rename(&output_tmp, &output_path).with_context(|| {
        format!(
            "failed renaming {} to {}",
            output_tmp.display(),
            output_path.display()
        )
    })?;
    fs::rename(&manifest_tmp, &manifest_path).with_context(|| {
        format!(
            "failed renaming {} to {}",
            manifest_tmp.display(),
            manifest_path.display()
        )
    })?;

    Ok(Some(ReleaseArtifacts {
        output_path,
        manifest_path,
    }))
}

fn open_success_encoder(
    path: &Path,
) -> Result<zstd::stream::write::Encoder<'static, CountingWriter<BufWriter<File>>>> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed opening {}", path.display()))?;
    let existing_bytes = file
        .metadata()
        .with_context(|| format!("failed to stat {}", path.display()))?
        .len();
    zstd::stream::write::Encoder::new(CountingWriter::new(BufWriter::new(file), existing_bytes), 3)
        .with_context(|| format!("failed creating zstd encoder for {}", path.display()))
}

fn finish_success_encoder(
    encoder: zstd::stream::write::Encoder<'static, CountingWriter<BufWriter<File>>>,
) -> Result<()> {
    let mut writer = encoder.finish().context("failed finishing success shard")?;
    writer
        .inner
        .flush()
        .context("failed flushing success shard buffer")?;
    writer
        .inner
        .get_ref()
        .sync_all()
        .context("failed syncing success shard")
}

fn remove_zero_length_file(path: &Path) -> Result<()> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.len() == 0 => {
            fs::remove_file(path).with_context(|| format!("failed removing {}", path.display()))?;
            Ok(())
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("failed stating {}", path.display())),
    }
}

struct CountingWriter<W> {
    inner: W,
    bytes_written: u64,
}

impl<W> CountingWriter<W> {
    fn new(inner: W, bytes_written: u64) -> Self {
        Self {
            inner,
            bytes_written,
        }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.bytes_written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn is_valid_inchikey(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 27
        && bytes[14] == b'-'
        && bytes[25] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 14 | 25) || byte.is_ascii_uppercase())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProgressSnapshot {
    success: u64,
    miss: u64,
    invalid: u64,
    failed: u64,
}

impl ProgressSnapshot {
    fn completed(self) -> u64 {
        self.success + self.miss + self.invalid
    }
}

struct ProgressCounters {
    success: AtomicU64,
    miss: AtomicU64,
    invalid: AtomicU64,
    failed: AtomicU64,
}

impl ProgressCounters {
    fn from_snapshot(snapshot: ProgressSnapshot) -> Self {
        Self {
            success: AtomicU64::new(snapshot.success),
            miss: AtomicU64::new(snapshot.miss),
            invalid: AtomicU64::new(snapshot.invalid),
            failed: AtomicU64::new(snapshot.failed),
        }
    }

    fn increment(&self, state: RowState) {
        match state {
            RowState::Success => {
                self.success.fetch_add(1, Ordering::SeqCst);
            }
            RowState::Miss => {
                self.miss.fetch_add(1, Ordering::SeqCst);
            }
            RowState::InvalidInput => {
                self.invalid.fetch_add(1, Ordering::SeqCst);
            }
            RowState::Failed => {
                self.failed.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    fn snapshot(&self) -> ProgressSnapshot {
        ProgressSnapshot {
            success: self.success.load(Ordering::SeqCst),
            miss: self.miss.load(Ordering::SeqCst),
            invalid: self.invalid.load(Ordering::SeqCst),
            failed: self.failed.load(Ordering::SeqCst),
        }
    }
}

struct NtfyClient {
    base_url: String,
    topic: String,
    client: Client,
}

impl NtfyClient {
    fn new(base_url: &str, topic: String, user_agent: &str, timeout_seconds: u64) -> Result<Self> {
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            topic,
            client: Client::builder()
                .user_agent(user_agent)
                .timeout(Duration::from_secs(timeout_seconds.max(1)))
                .build()
                .context("failed building ntfy client")?,
        })
    }

    fn subscription_url(&self) -> String {
        format!("{}/{}", self.base_url, self.topic)
    }

    fn publish_started(
        &self,
        subscription_url: &str,
        input_path: &Path,
        output_dir: &Path,
        snapshot: ProgressSnapshot,
    ) -> Result<()> {
        self.publish_message(
            "ClassyFire crawler started",
            &format!(
                "subscription_url: {}\ninput: {}\noutput_dir: {}\ncompleted: {}\nfailed: {}\nsuccess: {}\nmiss: {}\ninvalid_input: {}",
                subscription_url.trim(),
                input_path.display(),
                output_dir.display(),
                snapshot.completed(),
                snapshot.failed,
                snapshot.success,
                snapshot.miss,
                snapshot.invalid
            ),
            "failed POST ntfy startup notification",
        )
    }

    fn publish_daily_status(&self, snapshot: ProgressSnapshot) -> Result<()> {
        self.publish_message(
            &format!("ClassyFire daily status {}", Utc::now().format("%Y-%m-%d")),
            &format!(
                "completed: {}\nfailed: {}\nsuccess: {}\nmiss: {}\ninvalid_input: {}",
                snapshot.completed(),
                snapshot.failed,
                snapshot.success,
                snapshot.miss,
                snapshot.invalid
            ),
            "failed POST ntfy daily status",
        )
    }

    fn publish_zenodo_release_completed(&self, record_url: &str) -> Result<()> {
        self.publish_message(
            "ClassyFire Zenodo release completed",
            &format!("record_url: {}", record_url.trim()),
            "failed POST ntfy zenodo release notification",
        )
    }

    fn publish_zenodo_release_completed_with_counts(
        &self,
        record_url: &str,
        snapshot: ProgressSnapshot,
    ) -> Result<()> {
        self.publish_message(
            "ClassyFire Zenodo release completed",
            &format!(
                "record_url: {}\ncompleted: {}\nfailed: {}\nsuccess: {}\nmiss: {}\ninvalid_input: {}",
                record_url.trim(),
                snapshot.completed(),
                snapshot.failed,
                snapshot.success,
                snapshot.miss,
                snapshot.invalid
            ),
            "failed POST ntfy zenodo release notification",
        )
    }

    fn publish_zenodo_release_failed(&self, error: &str) -> Result<()> {
        self.publish_message(
            "ClassyFire Zenodo release failed",
            &format!("error: {}", error.trim()),
            "failed POST ntfy zenodo release failure notification",
        )
    }

    fn publish_message(&self, title: &str, body: &str, request_context: &str) -> Result<()> {
        let response = self
            .client
            .post(self.subscription_url())
            .header("Title", title)
            .body(body.to_owned())
            .send()
            .with_context(|| request_context.to_owned())?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            bail!("ntfy publish failed with status {status}: {body}");
        }
        Ok(())
    }
}

struct GetRateLimiter {
    interval: Duration,
    next_allowed: Mutex<Instant>,
}

impl GetRateLimiter {
    fn new(interval_seconds: u64) -> Self {
        Self {
            interval: Duration::from_secs(interval_seconds.max(1)),
            next_allowed: Mutex::new(Instant::now()),
        }
    }

    fn acquire(&self, running: &Arc<AtomicBool>) -> bool {
        loop {
            if !running.load(Ordering::SeqCst) {
                return false;
            }

            let wait_for = {
                let mut next_allowed = self
                    .next_allowed
                    .lock()
                    .expect("get rate limiter mutex poisoned");
                let now = Instant::now();
                if now >= *next_allowed {
                    *next_allowed = now + self.interval;
                    None
                } else {
                    Some(next_allowed.saturating_duration_since(now))
                }
            };

            match wait_for {
                None => return true,
                Some(duration) => {
                    let sleep_seconds = duration.as_secs().max(1);
                    if !sleep_until_stop(running, sleep_seconds) {
                        return false;
                    }
                }
            }
        }
    }

    fn backoff(&self, seconds: u64) {
        let mut next_allowed = self
            .next_allowed
            .lock()
            .expect("get rate limiter mutex poisoned");
        let candidate = Instant::now() + Duration::from_secs(seconds.max(1));
        if candidate > *next_allowed {
            *next_allowed = candidate;
        }
    }

    fn seconds_until_ready(&self) -> u64 {
        let next_allowed = self
            .next_allowed
            .lock()
            .expect("get rate limiter mutex poisoned");
        next_allowed
            .saturating_duration_since(Instant::now())
            .as_secs()
    }
}

fn sleep_until_stop(running: &Arc<AtomicBool>, seconds: u64) -> bool {
    sleep_until_stop_with_granularity(
        running,
        Duration::from_secs(seconds),
        Duration::from_secs(1),
    )
}

fn sleep_until_stop_with_granularity(
    running: &Arc<AtomicBool>,
    duration: Duration,
    granularity: Duration,
) -> bool {
    let mut remaining = duration;
    while remaining > Duration::ZERO {
        if !running.load(Ordering::SeqCst) {
            return false;
        }
        let step = remaining.min(granularity.max(Duration::from_millis(1)));
        sleep(step);
        remaining = remaining.saturating_sub(step);
    }
    running.load(Ordering::SeqCst)
}

fn next_ntfy_publish_after(now: DateTime<Utc>) -> DateTime<Utc> {
    let today = now.date_naive();
    let publish_today =
        Utc.from_utc_datetime(&today.and_hms_opt(18, 0, 0).expect("valid 18:00 utc time"));
    if now < publish_today {
        publish_today
    } else {
        let tomorrow = today
            .checked_add_days(Days::new(1))
            .expect("valid next day for ntfy publish");
        Utc.from_utc_datetime(
            &tomorrow
                .and_hms_opt(18, 0, 0)
                .expect("valid 18:00 utc time"),
        )
    }
}

#[inline]
fn is_throttled_or_html(error_text: &str) -> bool {
    let lower = error_text.to_ascii_lowercase();
    lower.contains("throttled") || lower.contains("returned html")
}

#[inline]
fn classify_backoff_reason(error_text: &str) -> &'static str {
    let lower = error_text.to_ascii_lowercase();
    if lower.contains("throttled") {
        "throttle"
    } else if lower.contains("returned html") {
        "html"
    } else {
        "error"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{MockResponse, MockServer};
    use serde_json::Value;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;

    fn test_limiter() -> Arc<GetRateLimiter> {
        Arc::new(GetRateLimiter {
            interval: Duration::ZERO,
            next_allowed: Mutex::new(Instant::now()),
        })
    }

    fn test_ntfy_client() -> Arc<NtfyClient> {
        Arc::new(
            NtfyClient::new(
                "http://127.0.0.1:9",
                "topic".to_owned(),
                "classyfire-test/0.1",
                1,
            )
            .unwrap(),
        )
    }

    fn test_config(input: PathBuf, output_dir: PathBuf, base_url: String) -> StreamConfig {
        StreamConfig {
            input,
            output_dir,
            base_url,
            user_agent: "classyfire-test/0.1".to_owned(),
            timeout_seconds: 1,
            get_sleep_seconds: 1,
            throttle_backoff_seconds: 0,
            status_interval_seconds: 1,
            success_shard_max_records: 100,
            success_shard_max_bytes: u64::MAX,
            ntfy_base_url: "https://ntfy.test".to_owned(),
            zenodo_token: "token".to_owned(),
            zenodo_deposit_id: "deposit".to_owned(),
            zenodo_publish_interval_seconds: 7 * 24 * 60 * 60,
        }
    }

    fn write_zstd_input(path: &Path, contents: &str) {
        let file = File::create(path).unwrap();
        let mut encoder = zstd::stream::write::Encoder::new(file, 3).unwrap();
        encoder.write_all(contents.as_bytes()).unwrap();
        encoder.finish().unwrap();
    }

    fn read_success_records(path: &Path) -> Vec<Value> {
        let file = File::open(path).unwrap();
        let decoder = zstd::stream::read::Decoder::new(BufReader::new(file)).unwrap();
        BufReader::new(decoder)
            .lines()
            .map(|line| serde_json::from_str::<Value>(&line.unwrap()).unwrap())
            .collect()
    }

    fn bitmap_has_row(path: &Path, row_index: u64) -> bool {
        let bytes = fs::read(path).unwrap();
        let byte_index = (row_index / 8) as usize;
        let mask = 1 << (row_index % 8);
        bytes.get(byte_index).is_some_and(|byte| byte & mask != 0)
    }

    fn load_checkpoint(path: &Path) -> Checkpoint {
        serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
    }

    #[test]
    fn bitmap_tracks_terminal_states_and_grows() {
        let dir = tempdir().unwrap();
        let mut bitmaps = StateBitmaps::open(dir.path(), 1).unwrap();
        assert_eq!(bitmaps.terminal_state(0).unwrap(), None);
        bitmaps.set_state(2_000_000, RowState::Failed).unwrap();
        assert_eq!(
            bitmaps.terminal_state(2_000_000).unwrap(),
            Some(RowState::Failed)
        );
    }

    #[test]
    fn success_writer_rotates_shards() {
        let dir = tempdir().unwrap();
        let success_dir = dir.path().join("success");
        fs::create_dir_all(&success_dir).unwrap();
        let mut writer = SuccessShardWriter::open(success_dir.clone(), 1, 0, 1, u64::MAX).unwrap();
        writer
            .write_record(
                &SuccessRecord::new(
                    0,
                    1,
                    "InChI=1S/CH4/h1H4",
                    "VNWKTOKETHGBQD-UHFFFAOYSA-N",
                    r#"{"kingdom":{"name":"Organic compounds"}}"#,
                )
                .unwrap(),
            )
            .unwrap();
        writer
            .write_record(
                &SuccessRecord::new(
                    1,
                    2,
                    "InChI=1S/H2O/h1H2",
                    "XLYOFNOQVPJJNP-UHFFFAOYSA-N",
                    r#"{"direct_parent":{"name":"Oxides"}}"#,
                )
                .unwrap(),
            )
            .unwrap();
        writer.finish().unwrap();

        assert!(success_dir.join("success-000001.jsonl.zst").exists());
        assert!(success_dir.join("success-000002.jsonl.zst").exists());
    }

    #[test]
    fn validates_inchikey_shape() {
        assert!(is_valid_inchikey("VNWKTOKETHGBQD-UHFFFAOYSA-N"));
        assert!(!is_valid_inchikey("bad"));
        assert!(!is_valid_inchikey("VNWKTOKETHGBQD-UHFFFAOYSA-1"));
    }

    #[test]
    fn parses_pubchem_lines() {
        let line = "123\tInChI=1S/CH4/h1H4\tVNWKTOKETHGBQD-UHFFFAOYSA-N";
        let (cid, inchi, inchikey) = parse_pubchem_line(line).unwrap();
        assert_eq!(cid, 123);
        assert_eq!(inchi, "InChI=1S/CH4/h1H4");
        assert_eq!(inchikey, "VNWKTOKETHGBQD-UHFFFAOYSA-N");
    }

    #[test]
    fn worker_processes_mixed_rows_from_zstd_input() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("pubchem.tsv.zst");
        let output_dir = dir.path().join("run");
        write_zstd_input(
            &input_path,
            &[
                "1\tInChI=1S/CH4/h1H4\tVNWKTOKETHGBQD-UHFFFAOYSA-N",
                "2\tInChI=1S/H2O/h1H2\tXLYOFNOQVPJJNP-UHFFFAOYSA-N",
                "bad-row",
                "4\tInChI=1S/CO2/c2-1-3\tbad",
                "5\tInChI=1S/C2H6/c1-2/h1-2H3\tOTMSDBZUPAUEDD-UHFFFAOYSA-N",
            ]
            .join("\n"),
        );

        let server = MockServer::new([
            (
                "/entities/VNWKTOKETHGBQD-UHFFFAOYSA-N.json",
                MockResponse::json(200, r#"{"direct_parent":{"name":"Alkanes"}}"#),
            ),
            (
                "/entities/XLYOFNOQVPJJNP-UHFFFAOYSA-N.json",
                MockResponse::text(404, "missing"),
            ),
            (
                "/entities/OTMSDBZUPAUEDD-UHFFFAOYSA-N.json",
                MockResponse::text(429, "slow down"),
            ),
        ]);

        let config = test_config(input_path, output_dir.clone(), server.url());
        run_stream_worker(
            Arc::new(AtomicBool::new(true)),
            test_limiter(),
            Arc::new(Ui::new()),
            Arc::new(ProgressCounters::from_snapshot(ProgressSnapshot {
                success: 0,
                miss: 0,
                invalid: 0,
                failed: 0,
            })),
            test_ntfy_client(),
            config,
        )
        .unwrap();

        assert_eq!(
            server.seen_paths(),
            vec![
                "/entities/VNWKTOKETHGBQD-UHFFFAOYSA-N.json".to_owned(),
                "/entities/XLYOFNOQVPJJNP-UHFFFAOYSA-N.json".to_owned(),
                "/entities/OTMSDBZUPAUEDD-UHFFFAOYSA-N.json".to_owned(),
            ]
        );

        assert!(bitmap_has_row(&output_dir.join(SUCCESS_BITMAP_NAME), 0));
        assert!(bitmap_has_row(&output_dir.join(MISS_BITMAP_NAME), 1));
        assert!(bitmap_has_row(&output_dir.join(INVALID_BITMAP_NAME), 2));
        assert!(bitmap_has_row(&output_dir.join(INVALID_BITMAP_NAME), 3));
        assert!(bitmap_has_row(&output_dir.join(FAILED_BITMAP_NAME), 4));

        let checkpoint = load_checkpoint(&output_dir.join(CHECKPOINT_NAME));
        assert_eq!(checkpoint.current_row_index, 5);
        assert_eq!(checkpoint.current_success_shard_id, 1);
        assert_eq!(checkpoint.current_success_records, 1);

        let records = read_success_records(
            &output_dir
                .join(SUCCESS_DIR_NAME)
                .join("success-000001.jsonl.zst"),
        );
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["row_index"], 0);
        assert_eq!(records[0]["cid"], 1);
        assert_eq!(records[0]["inchikey"], "VNWKTOKETHGBQD-UHFFFAOYSA-N");
        assert_eq!(records[0]["classyfire"]["direct_parent"]["name"], "Alkanes");
    }

    #[test]
    fn worker_resumes_from_checkpoint_and_skips_processed_rows() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("pubchem.tsv");
        let output_dir = dir.path().join("run");
        fs::write(
            &input_path,
            [
                "1\tInChI=1S/CH4/h1H4\tVNWKTOKETHGBQD-UHFFFAOYSA-N",
                "2\tInChI=1S/H2O/h1H2\tXLYOFNOQVPJJNP-UHFFFAOYSA-N",
            ]
            .join("\n"),
        )
        .unwrap();

        ensure_output_dirs(&output_dir).unwrap();
        let mut bitmaps = StateBitmaps::open(&output_dir, 2).unwrap();
        bitmaps.set_state(0, RowState::Success).unwrap();
        bitmaps.flush_all().unwrap();

        let mut writer =
            SuccessShardWriter::open(success_dir(&output_dir), 1, 0, 100, u64::MAX).unwrap();
        writer
            .write_record(
                &SuccessRecord::new(
                    0,
                    1,
                    "InChI=1S/CH4/h1H4",
                    "VNWKTOKETHGBQD-UHFFFAOYSA-N",
                    r#"{"direct_parent":{"name":"Alkanes"}}"#,
                )
                .unwrap(),
            )
            .unwrap();
        writer.finish().unwrap();

        let input_meta = read_input_metadata(&input_path).unwrap();
        save_checkpoint(
            &output_dir.join(CHECKPOINT_NAME),
            &Checkpoint {
                input_path: input_path.to_string_lossy().into_owned(),
                input_size_bytes: input_meta.size_bytes,
                input_mtime_epoch: input_meta.mtime_epoch,
                current_row_index: 1,
                current_success_shard_id: 1,
                current_success_records: 1,
                ntfy_topic: Some("topic".to_owned()),
            },
        )
        .unwrap();

        let server = MockServer::new([(
            "/entities/XLYOFNOQVPJJNP-UHFFFAOYSA-N.json",
            MockResponse::json(200, r#"{"direct_parent":{"name":"Oxides"}}"#),
        )]);
        let config = test_config(input_path, output_dir.clone(), server.url());

        run_stream_worker(
            Arc::new(AtomicBool::new(true)),
            test_limiter(),
            Arc::new(Ui::new()),
            Arc::new(ProgressCounters::from_snapshot(ProgressSnapshot {
                success: 1,
                miss: 0,
                invalid: 0,
                failed: 0,
            })),
            test_ntfy_client(),
            config,
        )
        .unwrap();

        assert_eq!(
            server.seen_paths(),
            vec!["/entities/XLYOFNOQVPJJNP-UHFFFAOYSA-N.json".to_owned()]
        );

        let checkpoint = load_checkpoint(&output_dir.join(CHECKPOINT_NAME));
        assert_eq!(checkpoint.current_row_index, 2);
        assert_eq!(checkpoint.current_success_shard_id, 1);
        assert_eq!(checkpoint.current_success_records, 2);

        let records = read_success_records(
            &output_dir
                .join(SUCCESS_DIR_NAME)
                .join("success-000001.jsonl.zst"),
        );
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["cid"], 1);
        assert_eq!(records[1]["cid"], 2);
        assert_eq!(records[1]["classyfire"]["direct_parent"]["name"], "Oxides");
    }

    #[test]
    fn build_release_artifacts_merges_non_empty_success_shards() {
        let dir = tempdir().unwrap();
        let output_dir = dir.path().join("run");
        ensure_output_dirs(&output_dir).unwrap();

        let mut writer =
            SuccessShardWriter::open(success_dir(&output_dir), 1, 0, 1, u64::MAX).unwrap();
        writer
            .write_record(
                &SuccessRecord::new(
                    0,
                    1,
                    "InChI=1S/CH4/h1H4",
                    "VNWKTOKETHGBQD-UHFFFAOYSA-N",
                    r#"{"direct_parent":{"name":"Alkanes"}}"#,
                )
                .unwrap(),
            )
            .unwrap();
        writer
            .write_record(
                &SuccessRecord::new(
                    1,
                    2,
                    "InChI=1S/H2O/h1H2",
                    "XLYOFNOQVPJJNP-UHFFFAOYSA-N",
                    r#"{"direct_parent":{"name":"Oxides"}}"#,
                )
                .unwrap(),
            )
            .unwrap();
        writer.seal_current().unwrap();
        writer.finish().unwrap();

        let release = build_release_artifacts(
            &output_dir,
            ProgressSnapshot {
                success: 2,
                miss: 3,
                invalid: 4,
                failed: 5,
            },
        )
        .unwrap()
        .unwrap();

        let records = read_success_records(&release.output_path);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["cid"], 1);
        assert_eq!(records[1]["cid"], 2);

        let manifest: Value =
            serde_json::from_slice(&fs::read(&release.manifest_path).unwrap()).unwrap();
        assert_eq!(manifest["format"], "jsonl.zst");
        assert_eq!(manifest["success_rows"], 2);
        assert_eq!(manifest["miss_rows"], 3);
        assert_eq!(manifest["invalid_rows"], 4);
        assert_eq!(manifest["failed_rows"], 5);
        assert_eq!(manifest["shards"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn run_streaming_handles_empty_input() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("empty.tsv");
        let output_dir = dir.path().join("run");
        fs::write(&input_path, "").unwrap();

        run_streaming(test_config(
            input_path,
            output_dir.clone(),
            "http://127.0.0.1:9".to_owned(),
        ))
        .unwrap();

        let checkpoint = load_checkpoint(&output_dir.join(CHECKPOINT_NAME));
        assert_eq!(checkpoint.current_row_index, 0);
        assert_eq!(checkpoint.current_success_records, 0);
    }

    #[test]
    fn checkpoint_rejects_path_mismatch() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.tsv");
        fs::write(
            &input_path,
            "1\tInChI=1S/CH4/h1H4\tVNWKTOKETHGBQD-UHFFFAOYSA-N\n",
        )
        .unwrap();
        let checkpoint_path = dir.path().join(CHECKPOINT_NAME);
        let input_meta = read_input_metadata(&input_path).unwrap();

        save_checkpoint(
            &checkpoint_path,
            &Checkpoint {
                input_path: "other.tsv".to_owned(),
                input_size_bytes: input_meta.size_bytes,
                input_mtime_epoch: input_meta.mtime_epoch,
                current_row_index: 0,
                current_success_shard_id: 1,
                current_success_records: 0,
                ntfy_topic: Some("topic".to_owned()),
            },
        )
        .unwrap();

        let error = load_or_init_checkpoint(&checkpoint_path, &input_path, &input_meta)
            .unwrap_err()
            .to_string();
        assert!(error.contains("input path"));
    }

    #[test]
    fn checkpoint_rejects_size_and_mtime_mismatch() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.tsv");
        fs::write(
            &input_path,
            "1\tInChI=1S/CH4/h1H4\tVNWKTOKETHGBQD-UHFFFAOYSA-N\n",
        )
        .unwrap();
        let checkpoint_path = dir.path().join(CHECKPOINT_NAME);
        let input_meta = read_input_metadata(&input_path).unwrap();

        save_checkpoint(
            &checkpoint_path,
            &Checkpoint {
                input_path: input_path.to_string_lossy().into_owned(),
                input_size_bytes: input_meta.size_bytes + 1,
                input_mtime_epoch: input_meta.mtime_epoch,
                current_row_index: 0,
                current_success_shard_id: 1,
                current_success_records: 0,
                ntfy_topic: Some("topic".to_owned()),
            },
        )
        .unwrap();
        let error = load_or_init_checkpoint(&checkpoint_path, &input_path, &input_meta)
            .unwrap_err()
            .to_string();
        assert!(error.contains("input size"));

        save_checkpoint(
            &checkpoint_path,
            &Checkpoint {
                input_path: input_path.to_string_lossy().into_owned(),
                input_size_bytes: input_meta.size_bytes,
                input_mtime_epoch: input_meta.mtime_epoch + 1,
                current_row_index: 0,
                current_success_shard_id: 1,
                current_success_records: 0,
                ntfy_topic: Some("topic".to_owned()),
            },
        )
        .unwrap();
        let error = load_or_init_checkpoint(&checkpoint_path, &input_path, &input_meta)
            .unwrap_err()
            .to_string();
        assert!(error.contains("input mtime"));
    }

    #[test]
    fn bitmap_detects_corruption_and_duplicate_states() {
        let dir = tempdir().unwrap();
        let mut bitmaps = StateBitmaps::open(dir.path(), 1).unwrap();
        bitmaps.set_state(0, RowState::Success).unwrap();
        let error = bitmaps
            .set_state(0, RowState::Miss)
            .unwrap_err()
            .to_string();
        assert!(error.contains("already has a terminal state"));

        bitmaps.success.set(1).unwrap();
        bitmaps.miss.set(1).unwrap();
        let error = bitmaps.terminal_state(1).unwrap_err().to_string();
        assert!(error.contains("multiple terminal states"));
    }

    #[test]
    fn reporter_returns_immediately_when_stopped() {
        let ui = Arc::new(Ui::new());
        let limiter = test_limiter();
        let running = Arc::new(AtomicBool::new(false));

        run_stream_reporter(running, limiter, ui, 1).unwrap();
    }

    #[test]
    fn sleep_and_backoff_helpers_cover_stop_and_other_reason() {
        let running = Arc::new(AtomicBool::new(false));
        assert!(!sleep_until_stop(&running, 1));
        assert!(!sleep_until_stop_with_granularity(
            &running,
            Duration::from_millis(10),
            Duration::from_millis(1),
        ));

        let limiter = GetRateLimiter {
            interval: Duration::from_secs(1),
            next_allowed: Mutex::new(Instant::now()),
        };
        limiter.backoff(2);
        assert!(limiter.seconds_until_ready() >= 1);
        assert!(!is_throttled_or_html("plain transport error"));
        assert_eq!(classify_backoff_reason("plain transport error"), "error");
    }

    #[test]
    fn next_ntfy_publish_uses_same_day_or_next_day_18_utc() {
        let same_day = next_ntfy_publish_after(
            Utc.with_ymd_and_hms(2026, 3, 26, 12, 0, 0)
                .single()
                .unwrap(),
        );
        assert_eq!(
            same_day,
            Utc.with_ymd_and_hms(2026, 3, 26, 18, 0, 0)
                .single()
                .unwrap()
        );

        let next_day = next_ntfy_publish_after(
            Utc.with_ymd_and_hms(2026, 3, 26, 18, 30, 0)
                .single()
                .unwrap(),
        );
        assert_eq!(
            next_day,
            Utc.with_ymd_and_hms(2026, 3, 27, 18, 0, 0)
                .single()
                .unwrap()
        );
    }

    #[test]
    fn checkpoint_generates_and_reuses_ntfy_topic() {
        let mut checkpoint = Checkpoint {
            input_path: "input.tsv".to_owned(),
            input_size_bytes: 1,
            input_mtime_epoch: 1,
            current_row_index: 0,
            current_success_shard_id: 1,
            current_success_records: 0,
            ntfy_topic: None,
        };
        let first = checkpoint.ensure_ntfy_topic();
        assert_eq!(Uuid::parse_str(&first).unwrap().get_version_num(), 4);
        let second = checkpoint.ensure_ntfy_topic();
        assert_eq!(first, second);
    }

    #[test]
    fn ntfy_client_publishes_daily_status_to_topic() {
        let server = MockServer::new([("/topic-123", MockResponse::text(200, "ok"))]);
        let client = NtfyClient::new(
            &server.url(),
            "topic-123".to_owned(),
            "classyfire-test/0.1",
            1,
        )
        .unwrap();

        client
            .publish_daily_status(ProgressSnapshot {
                success: 10,
                miss: 5,
                invalid: 2,
                failed: 3,
            })
            .unwrap();

        let requests = server.seen_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/topic-123");
        assert!(requests[0].headers.contains_key("title"));
        assert!(requests[0].body.contains("completed: 17"));
        assert!(requests[0].body.contains("failed: 3"));
    }

    #[test]
    fn ntfy_client_publishes_startup_notification_with_subscription_url() {
        let server = MockServer::new([("/topic-123", MockResponse::text(200, "ok"))]);
        let client = NtfyClient::new(
            &server.url(),
            "topic-123".to_owned(),
            "classyfire-test/0.1",
            1,
        )
        .unwrap();

        client
            .publish_started(
                &client.subscription_url(),
                Path::new("./input.tsv"),
                Path::new("./run"),
                ProgressSnapshot {
                    success: 10,
                    miss: 5,
                    invalid: 2,
                    failed: 3,
                },
            )
            .unwrap();

        let requests = server.seen_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/topic-123");
        assert_eq!(
            requests[0].headers.get("title").map(String::as_str),
            Some("ClassyFire crawler started")
        );
        assert!(requests[0]
            .body
            .contains(&format!("subscription_url: {}", client.subscription_url())));
        assert!(requests[0].body.contains("completed: 17"));
        assert!(requests[0].body.contains("failed: 3"));
    }

    #[test]
    fn ntfy_client_publishes_zenodo_release_completion() {
        let server = MockServer::new([("/topic-123", MockResponse::text(200, "ok"))]);
        let client = NtfyClient::new(
            &server.url(),
            "topic-123".to_owned(),
            "classyfire-test/0.1",
            1,
        )
        .unwrap();

        client
            .publish_zenodo_release_completed("https://zenodo.org/records/123")
            .unwrap();

        let requests = server.seen_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/topic-123");
        assert_eq!(
            requests[0].headers.get("title").map(String::as_str),
            Some("ClassyFire Zenodo release completed")
        );
        assert!(requests[0]
            .body
            .contains("record_url: https://zenodo.org/records/123"));
    }

    #[test]
    fn notify_zenodo_release_reuses_checkpoint_topic() {
        let dir = tempdir().unwrap();
        let output_dir = dir.path().join("run");
        ensure_output_dirs(&output_dir).unwrap();
        save_checkpoint(
            &output_dir.join(CHECKPOINT_NAME),
            &Checkpoint {
                input_path: "input.tsv".to_owned(),
                input_size_bytes: 1,
                input_mtime_epoch: 1,
                current_row_index: 0,
                current_success_shard_id: 1,
                current_success_records: 0,
                ntfy_topic: Some("topic-from-checkpoint".to_owned()),
            },
        )
        .unwrap();

        let server = MockServer::new([("/topic-from-checkpoint", MockResponse::text(200, "ok"))]);
        notify_zenodo_release(NotifyZenodoReleaseConfig {
            output_dir,
            record_url: "https://zenodo.org/records/999".to_owned(),
            user_agent: "classyfire-test/0.1".to_owned(),
            timeout_seconds: 1,
            ntfy_base_url: server.url(),
        })
        .unwrap();

        let requests = server.seen_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/topic-from-checkpoint");
        assert!(requests[0]
            .body
            .contains("record_url: https://zenodo.org/records/999"));
    }

    #[test]
    fn run_streaming_sends_startup_ntfy_notification() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("empty.tsv");
        let output_dir = dir.path().join("run");
        fs::write(&input_path, "").unwrap();

        let server = MockServer::new([("/", MockResponse::text(404, "missing route"))]);
        let config = test_config(
            input_path.clone(),
            output_dir.clone(),
            "http://127.0.0.1:9".to_owned(),
        );
        let config = StreamConfig {
            ntfy_base_url: server.url(),
            ..config
        };

        run_streaming(config).unwrap();

        let requests = server.seen_requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].headers.contains_key("title"));
        assert_eq!(
            requests[0].headers.get("title").map(String::as_str),
            Some("ClassyFire crawler started")
        );
        assert!(requests[0].body.contains("subscription_url: http://"));
        assert!(requests[0]
            .body
            .contains(&format!("input: {}", input_path.display())));
        assert!(requests[0]
            .body
            .contains(&format!("output_dir: {}", output_dir.display())));
    }
}
