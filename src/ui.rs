use crate::db::{CounterEntry, RunnerSnapshot};
use anyhow::Result;
use chrono::Local;
use crossterm::cursor::{Hide, MoveTo, Show};
use crossterm::queue;
use crossterm::style::{Attribute, Color, Print, ResetColor, SetAttribute, SetForegroundColor};
use crossterm::terminal::{
    size, BeginSynchronizedUpdate, Clear, ClearType, DisableLineWrap, EnableLineWrap,
    EndSynchronizedUpdate, EnterAlternateScreen, LeaveAlternateScreen,
};
use std::collections::VecDeque;
use std::io::{self, IsTerminal, Write};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const MAX_EVENTS: usize = 8;
const MAX_ERRORS: usize = 8;

#[derive(Clone)]
pub struct Ui {
    inner: Arc<UiInner>,
}

struct UiInner {
    interactive: bool,
    state: Mutex<DashboardState>,
}

struct DashboardState {
    started_at: Instant,
    current_inchikey: Option<String>,
    current_attempt: i32,
    last_result: Option<String>,
    get_backoff_reason: Option<String>,
    get_backoff_at: Option<String>,
    session_requests: u64,
    session_hits: u64,
    session_misses: u64,
    session_errors: u64,
    recent_events: VecDeque<String>,
    recent_errors: VecDeque<String>,
}

pub struct TerminalGuard {
    active: bool,
}

impl Ui {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(UiInner {
                interactive: io::stderr().is_terminal(),
                state: Mutex::new(DashboardState {
                    started_at: Instant::now(),
                    current_inchikey: None,
                    current_attempt: 0,
                    last_result: None,
                    get_backoff_reason: None,
                    get_backoff_at: None,
                    session_requests: 0,
                    session_hits: 0,
                    session_misses: 0,
                    session_errors: 0,
                    recent_events: VecDeque::new(),
                    recent_errors: VecDeque::new(),
                }),
            }),
        }
    }

    pub fn is_interactive(&self) -> bool {
        self.inner.interactive
    }

    pub fn enter_terminal(&self) -> Result<Option<TerminalGuard>> {
        if !self.is_interactive() {
            return Ok(None);
        }
        let mut stderr = io::stderr();
        crossterm::execute!(stderr, EnterAlternateScreen, DisableLineWrap, Hide)?;
        Ok(Some(TerminalGuard { active: true }))
    }

    pub fn info(&self, message: impl Into<String>) {
        let message = message.into();
        if !self.is_interactive() {
            eprintln!("{message}");
            return;
        }
        let mut state = self.inner.state.lock().expect("ui state mutex poisoned");
        push_ring(&mut state.recent_events, MAX_EVENTS, message);
    }

    pub fn note_current_key(&self, inchikey: &str, attempt: i32) {
        let mut state = self.inner.state.lock().expect("ui state mutex poisoned");
        state.current_inchikey = Some(inchikey.to_owned());
        state.current_attempt = attempt;
        state.session_requests += 1;
    }

    pub fn note_hit(&self, inchikey: &str) {
        let mut state = self.inner.state.lock().expect("ui state mutex poisoned");
        state.session_hits += 1;
        state.last_result = Some(format!("hit {inchikey}"));
        push_ring(
            &mut state.recent_events,
            MAX_EVENTS,
            format!("hit {inchikey}"),
        );
    }

    pub fn note_miss(&self, inchikey: &str) {
        let mut state = self.inner.state.lock().expect("ui state mutex poisoned");
        state.session_misses += 1;
        state.last_result = Some(format!("miss {inchikey}"));
        push_ring(
            &mut state.recent_events,
            MAX_EVENTS,
            format!("miss {inchikey}"),
        );
    }

    pub fn note_error(&self, inchikey: &str, error: &str) {
        let mut state = self.inner.state.lock().expect("ui state mutex poisoned");
        state.session_errors += 1;
        state.last_result = Some(format!("error {inchikey}: {}", compact_error(error)));
        push_ring(
            &mut state.recent_errors,
            MAX_ERRORS,
            format!("error {inchikey}: {error}"),
        );
    }

    pub fn note_backoff(&self, seconds: u64, reason: &str) {
        let mut state = self.inner.state.lock().expect("ui state mutex poisoned");
        state.get_backoff_reason = Some(reason.to_owned());
        state.get_backoff_at = Some(timestamp());
        push_ring(
            &mut state.recent_events,
            MAX_EVENTS,
            format!("GET backoff {seconds}s after {reason}"),
        );
    }

    pub fn render_dashboard(
        &self,
        snapshot: &RunnerSnapshot,
        get_ready_in_seconds: u64,
    ) -> Result<()> {
        if !self.is_interactive() {
            return Ok(());
        }

        let state = self.inner.state.lock().expect("ui state mutex poisoned");
        let uptime_seconds = state.started_at.elapsed().as_secs().max(1);
        let rate = state.session_requests as f64 / (uptime_seconds as f64 / 60.0);
        let (width, height) = size().unwrap_or((120, 36));

        let mut lines = vec![
            format!(
                "ClassyFire GET downloader  {}",
                Local::now().format("%Y-%m-%d %H:%M:%S")
            ),
            format!("uptime={}s | req_rate={:.2}/min", uptime_seconds, rate),
            format!(
                "current: {} attempt={} | get_gate={}{}",
                state.current_inchikey.as_deref().unwrap_or("idle"),
                state.current_attempt,
                get_ready_in_seconds,
                format_backoff(&state, get_ready_in_seconds),
            ),
            state.last_result.as_ref().map_or_else(
                || "last result: none".to_owned(),
                |value| format!("last result: {value}"),
            ),
            format!(
                "session: requests={} hits={} misses={} errors={}",
                state.session_requests,
                state.session_hits,
                state.session_misses,
                state.session_errors
            ),
            format!(
                "db counts: total={} new={} done={} miss={} error={}",
                snapshot.stats.total_molecules,
                snapshot.stats.new_count,
                snapshot.stats.done_count,
                snapshot.stats.miss_count,
                snapshot.stats.error_count
            ),
            "top kingdoms:".to_owned(),
        ];

        push_counter_lines(&mut lines, &snapshot.top_kingdoms, 5);
        lines.push("top superclasses:".to_owned());
        push_counter_lines(&mut lines, &snapshot.top_superclasses, 5);
        lines.push("top classes:".to_owned());
        push_counter_lines(&mut lines, &snapshot.top_classes, 5);
        lines.push("recent events:".to_owned());
        push_recent_lines(&mut lines, &state.recent_events, MAX_EVENTS, "  (none)");
        lines.push("recent errors:".to_owned());
        push_recent_lines(&mut lines, &state.recent_errors, MAX_ERRORS, "  (none)");

        let mut stderr = io::stderr();
        queue!(
            stderr,
            BeginSynchronizedUpdate,
            MoveTo(0, 0),
            Clear(ClearType::All)
        )?;

        for (row, line) in lines.into_iter().enumerate().take(height as usize) {
            if row > 0 {
                write!(stderr, "\r\n")?;
            }
            write_styled_line(&mut stderr, &line, width as usize)?;
        }

        queue!(stderr, EndSynchronizedUpdate)?;
        stderr.flush()?;
        Ok(())
    }
}

impl Default for Ui {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut stderr = io::stderr();
        let _ = crossterm::execute!(stderr, Show, EnableLineWrap, LeaveAlternateScreen);
    }
}

fn format_backoff(state: &DashboardState, get_ready_in_seconds: u64) -> String {
    if get_ready_in_seconds == 0 {
        return String::new();
    }
    match (&state.get_backoff_reason, &state.get_backoff_at) {
        (Some(reason), Some(at)) => format!(" reason={reason} since={at}"),
        (Some(reason), None) => format!(" reason={reason}"),
        _ => String::new(),
    }
}

fn compact_error(error: &str) -> String {
    let lower = error.to_ascii_lowercase();
    if lower.contains("throttled") {
        return "throttled".to_owned();
    }
    if lower.contains("returned html") {
        return "html".to_owned();
    }
    if lower.contains("timed out") {
        return "timeout".to_owned();
    }
    if lower.contains("failed get /entities") {
        return "transport".to_owned();
    }
    "error".to_owned()
}

fn push_counter_lines(lines: &mut Vec<String>, counters: &[CounterEntry], max_lines: usize) {
    if counters.is_empty() {
        lines.push("  (none)".to_owned());
        return;
    }
    for counter in counters.iter().take(max_lines) {
        lines.push(format!("  {} = {}", counter.label, counter.count));
    }
}

fn push_recent_lines(
    lines: &mut Vec<String>,
    events: &VecDeque<String>,
    max_lines: usize,
    empty_line: &str,
) {
    if events.is_empty() {
        lines.push(empty_line.to_owned());
        return;
    }
    let start = events.len().saturating_sub(max_lines);
    for event in events.iter().skip(start) {
        lines.push(format!("  {event}"));
    }
}

fn push_ring(ring: &mut VecDeque<String>, max_len: usize, message: String) {
    if ring.len() >= max_len {
        ring.pop_front();
    }
    ring.push_back(format!("[{}] {message}", timestamp()));
}

fn ellipsize_to_width(value: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let available = width.saturating_sub(1);
    let char_count = value.chars().count();
    if char_count <= available {
        return value.to_owned();
    }
    if available <= 1 {
        return "…".to_owned();
    }
    let keep = available - 1;
    let mut truncated = value.chars().take(keep).collect::<String>();
    truncated.push('…');
    truncated
}

fn write_styled_line(stderr: &mut io::Stderr, line: &str, width: usize) -> Result<()> {
    let line = ellipsize_to_width(line, width);
    if line.starts_with("ClassyFire GET downloader") {
        queue!(
            stderr,
            SetForegroundColor(Color::DarkCyan),
            SetAttribute(Attribute::Bold),
            Print(line),
            ResetColor,
            SetAttribute(Attribute::Reset)
        )?;
        return Ok(());
    }
    if is_section_header(&line) {
        queue!(
            stderr,
            SetForegroundColor(Color::DarkBlue),
            SetAttribute(Attribute::Bold),
            Print(line),
            ResetColor,
            SetAttribute(Attribute::Reset)
        )?;
        return Ok(());
    }
    if is_error_event(&line) {
        queue!(
            stderr,
            SetForegroundColor(Color::DarkRed),
            Print(line),
            ResetColor
        )?;
        return Ok(());
    }
    if line.starts_with("  [") {
        queue!(
            stderr,
            SetForegroundColor(Color::DarkGreen),
            Print(line),
            ResetColor
        )?;
        return Ok(());
    }
    queue!(stderr, Print(line))?;
    Ok(())
}

fn is_section_header(line: &str) -> bool {
    matches!(
        line,
        "top kingdoms:"
            | "top superclasses:"
            | "top classes:"
            | "recent events:"
            | "recent errors:"
    )
}

fn is_error_event(line: &str) -> bool {
    line.starts_with("  [") && line.contains("error")
}

fn timestamp() -> String {
    Local::now().format("%H:%M:%S").to_string()
}

#[cfg(test)]
mod tests {
    use super::ellipsize_to_width;

    #[test]
    fn ellipsize_reserves_last_column() {
        assert_eq!(ellipsize_to_width("abcdefghij", 10), "abcdefgh…");
    }
}
