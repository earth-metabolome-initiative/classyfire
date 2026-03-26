use crate::api::{validate_entity_body, ApiClient};
use crate::config::{RuntimeConfig, ZenodoConfig};
use crate::db::{
    collect_runner_snapshot, collect_stats, establish_pool, mark_molecule_done,
    mark_molecule_error, mark_molecule_miss, recreate_runtime_indexes, run_migrations,
    select_next_molecule,
};
use crate::export::export_parquet;
use crate::ui::Ui;
use crate::zenodo;
use anyhow::{Context, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};

pub fn run_daemon(config: RuntimeConfig) -> Result<()> {
    config.validate()?;
    let pool = establish_pool(&config.db)?;
    {
        let mut conn = pool.get()?;
        run_migrations(&mut conn)?;
        recreate_runtime_indexes(&mut conn)?;
    }

    let running = Arc::new(AtomicBool::new(true));
    let get_limiter = Arc::new(GetRateLimiter::new(config.get_sleep_seconds));
    let ui = Arc::new(Ui::new());

    {
        let stop = Arc::clone(&running);
        let ui = Arc::clone(&ui);
        ctrlc::set_handler(move || {
            if stop.swap(false, Ordering::SeqCst) {
                ui.info("stopping runner");
            }
        })
        .context("failed to install ctrl-c handler")?;
    }

    let worker_handle = {
        let running = Arc::clone(&running);
        let get_limiter = Arc::clone(&get_limiter);
        let ui = Arc::clone(&ui);
        let config = config.clone();
        let pool = pool.clone();
        thread::spawn(move || run_get_worker(running, get_limiter, ui, config, pool))
    };

    let reporter_handle = {
        let running = Arc::clone(&running);
        let get_limiter = Arc::clone(&get_limiter);
        let ui = Arc::clone(&ui);
        let config = config.clone();
        let pool = pool.clone();
        thread::spawn(move || run_reporter(running, get_limiter, ui, config, pool))
    };

    let publisher_handle = config.zenodo_config().map(|zenodo_config| {
        let running = Arc::clone(&running);
        let ui = Arc::clone(&ui);
        let pool = pool.clone();
        thread::spawn(move || run_zenodo_publisher(running, ui, pool, zenodo_config))
    });

    if config.zenodo_config().is_some() {
        ui.info("zenodo publisher enabled");
    } else {
        ui.info("zenodo publisher disabled");
    }

    worker_handle
        .join()
        .map_err(|_| anyhow::anyhow!("GET worker panicked"))??;
    reporter_handle
        .join()
        .map_err(|_| anyhow::anyhow!("status worker panicked"))??;
    if let Some(handle) = publisher_handle {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("Zenodo publisher panicked"))??;
    }
    Ok(())
}

fn run_get_worker(
    running: Arc<AtomicBool>,
    get_limiter: Arc<GetRateLimiter>,
    ui: Arc<Ui>,
    config: RuntimeConfig,
    pool: crate::db::DbPool,
) -> Result<()> {
    let client = ApiClient::new(&config.base_url, &config.user_agent, config.timeout_seconds)?;
    while running.load(Ordering::SeqCst) {
        let molecule = {
            let mut conn = pool.get()?;
            select_next_molecule(&mut conn)?
        };

        let Some(molecule) = molecule else {
            if !sleep_until_stop(&running, 5) {
                break;
            }
            continue;
        };

        ui.note_current_key(&molecule.inchikey, molecule.attempts + 1);
        if !get_limiter.acquire(&running) {
            return Ok(());
        }

        let mut conn = pool.get()?;
        match client
            .fetch_entity(&molecule.inchikey)
            .and_then(|body| validate_entity_body(&body).map(|value| (body, value)))
        {
            Ok((body, Some(entity))) => {
                let labels = entity.taxonomy_labels();
                mark_molecule_done(
                    &mut conn,
                    &molecule,
                    body.body.as_bytes(),
                    &labels,
                    now_ts(),
                )?;
                ui.note_hit(&molecule.inchikey);
            }
            Ok((_body, None)) => {
                mark_molecule_miss(&mut conn, &molecule, now_ts())?;
                ui.note_miss(&molecule.inchikey);
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
                mark_molecule_error(&mut conn, &molecule, &error_text, now_ts())?;
                ui.note_error(&molecule.inchikey, &error_text);
            }
        }
    }
    Ok(())
}

fn run_reporter(
    running: Arc<AtomicBool>,
    get_limiter: Arc<GetRateLimiter>,
    ui: Arc<Ui>,
    config: RuntimeConfig,
    pool: crate::db::DbPool,
) -> Result<()> {
    let _terminal = ui.enter_terminal()?;
    while running.load(Ordering::SeqCst) {
        let snapshot = {
            let mut conn = pool.get()?;
            collect_runner_snapshot(&mut conn, 5)?
        };

        if ui.is_interactive() {
            ui.render_dashboard(&snapshot, get_limiter.seconds_until_ready())?;
        } else {
            eprintln!(
                "status: total={} new={} done={} miss={} error={}",
                snapshot.stats.total_molecules,
                snapshot.stats.new_count,
                snapshot.stats.done_count,
                snapshot.stats.miss_count,
                snapshot.stats.error_count
            );
        }

        if !sleep_until_stop(&running, config.status_interval_seconds) {
            break;
        }
    }
    Ok(())
}

fn run_zenodo_publisher(
    running: Arc<AtomicBool>,
    ui: Arc<Ui>,
    pool: crate::db::DbPool,
    config: ZenodoConfig,
) -> Result<()> {
    let interval = Duration::from_secs(config.publish_interval_seconds.max(1));
    let mut last_publish = Instant::now();

    while running.load(Ordering::SeqCst) {
        if last_publish.elapsed() < interval {
            let remaining = interval
                .saturating_sub(last_publish.elapsed())
                .as_secs()
                .min(60);
            if !sleep_until_stop(&running, remaining.max(1)) {
                break;
            }
            continue;
        }

        ui.info("zenodo publish started");
        match publish_zenodo_snapshot(&pool, &config) {
            Ok(doi) => ui.info(format!("zenodo published {doi}")),
            Err(error) => ui.info(format!("zenodo publish failed: {error:#}")),
        }
        last_publish = Instant::now();
    }

    Ok(())
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

fn publish_zenodo_snapshot(pool: &crate::db::DbPool, config: &ZenodoConfig) -> Result<String> {
    let parquet_path = zenodo_parquet_path();
    let stats = {
        let mut conn = pool.get()?;
        let stats = collect_stats(&mut conn)?;
        if stats.done_count == 0 {
            return Err(anyhow::anyhow!(
                "no labeled rows available for Zenodo export"
            ));
        }
        export_parquet(&mut conn, &parquet_path)?;
        stats
    };

    let publish_result = zenodo::publish(&config.token, &config.deposit_id, &parquet_path, &stats);
    let _ = std::fs::remove_file(&parquet_path);
    publish_result
}

fn zenodo_parquet_path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "classyfire-labels-{}.parquet",
        chrono::Utc::now().format("%Y%m%dT%H%M%S")
    ))
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
    let mut remaining = seconds;
    while remaining > 0 {
        if !running.load(Ordering::SeqCst) {
            return false;
        }
        sleep(Duration::from_secs(1));
        remaining -= 1;
    }
    running.load(Ordering::SeqCst)
}

#[inline]
fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_backoff_reasons() {
        assert_eq!(
            classify_backoff_reason("entity request was throttled"),
            "throttle"
        );
        assert_eq!(
            classify_backoff_reason("entity request returned HTML"),
            "html"
        );
    }
}
