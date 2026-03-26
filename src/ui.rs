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
    ntfy_url: Option<String>,
    current_inchikey: Option<String>,
    last_result: Option<String>,
    get_backoff_reason: Option<String>,
    get_backoff_at: Option<String>,
    recent_events: VecDeque<String>,
    recent_errors: VecDeque<String>,
}

pub struct TerminalGuard;

impl Ui {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(UiInner {
                interactive: io::stderr().is_terminal(),
                state: Mutex::new(DashboardState {
                    started_at: Instant::now(),
                    ntfy_url: None,
                    current_inchikey: None,
                    last_result: None,
                    get_backoff_reason: None,
                    get_backoff_at: None,
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
        Ok(Some(TerminalGuard))
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

    pub fn set_ntfy_url(&self, url: &str) {
        let mut state = self.inner.state.lock().expect("ui state mutex poisoned");
        state.ntfy_url = Some(url.to_owned());
    }

    pub fn note_current_key(&self, inchikey: &str) {
        let mut state = self.inner.state.lock().expect("ui state mutex poisoned");
        state.current_inchikey = Some(inchikey.to_owned());
    }

    pub fn note_hit(&self, inchikey: &str) {
        let mut state = self.inner.state.lock().expect("ui state mutex poisoned");
        state.last_result = Some(format!("hit {inchikey}"));
        push_ring(
            &mut state.recent_events,
            MAX_EVENTS,
            format!("hit {inchikey}"),
        );
    }

    pub fn note_miss(&self, inchikey: &str) {
        let mut state = self.inner.state.lock().expect("ui state mutex poisoned");
        state.last_result = Some(format!("miss {inchikey}"));
        push_ring(
            &mut state.recent_events,
            MAX_EVENTS,
            format!("miss {inchikey}"),
        );
    }

    pub fn note_error(&self, inchikey: &str, error: &str) {
        let mut state = self.inner.state.lock().expect("ui state mutex poisoned");
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

    pub fn render_dashboard(&self, get_ready_in_seconds: u64) -> Result<()> {
        if !self.is_interactive() {
            return Ok(());
        }

        let state = self.inner.state.lock().expect("ui state mutex poisoned");
        let (width, height) = size().unwrap_or((120, 36));
        let lines = build_dashboard_lines(
            &state,
            get_ready_in_seconds,
            &Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        );

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

fn build_dashboard_lines(
    state: &DashboardState,
    get_ready_in_seconds: u64,
    now: &str,
) -> Vec<String> {
    let mut lines = vec![
        format!("ClassyFire GET downloader  {now}"),
        format!("uptime={}s", state.started_at.elapsed().as_secs().max(1)),
        state.ntfy_url.as_ref().map_or_else(
            || "ntfy: (not initialized)".to_owned(),
            |value| format!("ntfy: {value}"),
        ),
        format!(
            "current: {} | get_gate={}{}",
            state.current_inchikey.as_deref().unwrap_or("idle"),
            get_ready_in_seconds,
            format_backoff(state, get_ready_in_seconds),
        ),
        state.last_result.as_ref().map_or_else(
            || "last result: none".to_owned(),
            |value| format!("last result: {value}"),
        ),
        "recent events:".to_owned(),
    ];

    push_recent_lines(&mut lines, &state.recent_events, MAX_EVENTS, "  (none)");
    lines.push("recent errors:".to_owned());
    push_recent_lines(&mut lines, &state.recent_errors, MAX_ERRORS, "  (none)");
    lines
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
    matches!(line, "recent events:" | "recent errors:")
}

fn is_error_event(line: &str) -> bool {
    line.starts_with("  [") && line.contains("error")
}

fn timestamp() -> String {
    Local::now().format("%H:%M:%S").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn ellipsize_reserves_last_column() {
        assert_eq!(ellipsize_to_width("abcdefghij", 10), "abcdefgh…");
    }

    #[test]
    fn dashboard_lines_include_backoff_and_recent_entries() {
        let mut recent_events = VecDeque::new();
        recent_events.push_back("[12:00:00] hit VNWKTOKETHGBQD-UHFFFAOYSA-N".to_owned());
        let mut recent_errors = VecDeque::new();
        recent_errors.push_back("[12:00:01] error XLYOFNOQVPJJNP-UHFFFAOYSA-N: timeout".to_owned());
        let state = DashboardState {
            started_at: Instant::now() - Duration::from_secs(5),
            ntfy_url: Some("https://ntfy.sh/topic-123".to_owned()),
            current_inchikey: Some("VNWKTOKETHGBQD-UHFFFAOYSA-N".to_owned()),
            last_result: Some("hit VNWKTOKETHGBQD-UHFFFAOYSA-N".to_owned()),
            get_backoff_reason: Some("throttle".to_owned()),
            get_backoff_at: Some("12:00:02".to_owned()),
            recent_events,
            recent_errors,
        };

        let lines = build_dashboard_lines(&state, 30, "2026-03-26 18:10:00");

        assert_eq!(lines[0], "ClassyFire GET downloader  2026-03-26 18:10:00");
        assert_eq!(lines[2], "ntfy: https://ntfy.sh/topic-123");
        assert!(lines[3].contains("get_gate=30"));
        assert!(lines[3].contains("reason=throttle since=12:00:02"));
        assert!(lines.iter().any(|line| line.contains("recent events:")));
        assert!(lines.iter().any(|line| line.contains("recent errors:")));
        assert!(lines.iter().any(|line| line.contains("timeout")));
    }

    #[test]
    fn compact_error_classifies_known_cases() {
        assert_eq!(compact_error("entity request was throttled"), "throttled");
        assert_eq!(compact_error("entity request returned HTML"), "html");
        assert_eq!(compact_error("operation timed out"), "timeout");
        assert_eq!(compact_error("failed GET /entities request"), "transport");
        assert_eq!(compact_error("something else"), "error");
    }

    #[test]
    fn push_ring_keeps_only_the_most_recent_entries() {
        let mut ring = VecDeque::new();
        push_ring(&mut ring, 2, "first".to_owned());
        push_ring(&mut ring, 2, "second".to_owned());
        push_ring(&mut ring, 2, "third".to_owned());

        assert_eq!(ring.len(), 2);
        assert!(ring[0].contains("second"));
        assert!(ring[1].contains("third"));
    }
}
