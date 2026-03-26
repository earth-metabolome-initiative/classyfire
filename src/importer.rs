use crate::db::{insert_molecule_chunk, load_import_state, save_import_state, ImportStats};
use anyhow::{anyhow, bail, Context, Result};
use diesel::sqlite::SqliteConnection;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::File;
use std::io::{BufRead, BufReader, IsTerminal, Seek, SeekFrom};
use std::path::Path;
use std::time::UNIX_EPOCH;

const PROGRESS_UPDATE_BYTES: i64 = 16 * 1024 * 1024;

struct ImportSourceState {
    source_size_bytes: i64,
    source_mtime_epoch: i64,
    source_path: String,
    resume: Option<crate::models::ImportState>,
}

pub fn import_has_pending_work(conn: &mut SqliteConnection, input: &Path) -> Result<bool> {
    let source = inspect_import_source(conn, input)?;
    Ok(source
        .resume
        .as_ref()
        .is_none_or(|state| state.last_committed_offset < source.source_size_bytes))
}

pub fn import_pubchem_dump(
    conn: &mut SqliteConnection,
    input: &Path,
    chunk_size: usize,
    now: i64,
) -> Result<ImportStats> {
    let source = inspect_import_source(conn, input)?;

    let mut file =
        File::open(input).with_context(|| format!("failed to open {}", input.display()))?;
    let start_offset = source
        .resume
        .as_ref()
        .map_or(0, |state| state.last_committed_offset);
    if start_offset > 0 {
        file.seek(SeekFrom::Start(start_offset as u64))
            .with_context(|| format!("failed seeking {}", input.display()))?;
    }

    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut chunk = Vec::with_capacity(chunk_size);
    let mut stats = ImportStats {
        resumed_from_line: source
            .resume
            .as_ref()
            .map_or(0, |state| state.last_committed_line) as u64,
        ..ImportStats::default()
    };
    let progress_bar = build_progress_bar(source.source_size_bytes as u64, start_offset as u64);

    let mut current_line = source
        .resume
        .as_ref()
        .map_or(0, |state| state.last_committed_line);
    let mut current_offset = start_offset;
    let mut last_progress_offset = start_offset;
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader
            .read_line(&mut line)
            .with_context(|| format!("failed reading {}", input.display()))?;
        if bytes_read == 0 {
            break;
        }

        current_offset += i64::try_from(bytes_read).context("offset overflow")?;
        current_line += 1;

        let trimmed = line.trim_end_matches(['\n', '\r']);
        let (cid, inchi, inchikey) = parse_pubchem_line(trimmed)
            .with_context(|| format!("failed parsing line {current_line}"))?;
        chunk.push((cid, inchi, inchikey));
        stats.rows_read += 1;

        if current_offset - last_progress_offset >= PROGRESS_UPDATE_BYTES {
            update_progress_bar(
                &progress_bar,
                current_offset as u64,
                current_line as u64,
                stats.rows_insert_attempted + chunk.len() as u64,
            );
            last_progress_offset = current_offset;
        }

        if chunk.len() >= chunk_size {
            insert_molecule_chunk(conn, &chunk, now)?;
            save_import_state(
                conn,
                &source.source_path,
                source.source_size_bytes,
                source.source_mtime_epoch,
                current_line,
                current_offset,
                now,
            )?;
            stats.rows_insert_attempted += chunk.len() as u64;
            chunk.clear();
            update_progress_bar(
                &progress_bar,
                current_offset as u64,
                current_line as u64,
                stats.rows_insert_attempted,
            );
            last_progress_offset = current_offset;
        }
    }

    if !chunk.is_empty() {
        insert_molecule_chunk(conn, &chunk, now)?;
        save_import_state(
            conn,
            &source.source_path,
            source.source_size_bytes,
            source.source_mtime_epoch,
            current_line,
            current_offset,
            now,
        )?;
        stats.rows_insert_attempted += chunk.len() as u64;
        update_progress_bar(
            &progress_bar,
            current_offset as u64,
            current_line as u64,
            stats.rows_insert_attempted,
        );
    }

    finish_progress_bar(
        progress_bar,
        current_offset as u64,
        current_line as u64,
        stats.rows_insert_attempted,
    );

    Ok(stats)
}

fn inspect_import_source(conn: &mut SqliteConnection, input: &Path) -> Result<ImportSourceState> {
    let metadata =
        std::fs::metadata(input).with_context(|| format!("failed to stat {}", input.display()))?;
    let source_size_bytes = i64::try_from(metadata.len()).context("source file too large")?;
    let source_mtime_epoch = i64::try_from(
        metadata
            .modified()
            .context("missing file modified time")?
            .duration_since(UNIX_EPOCH)
            .context("modified time predates unix epoch")?
            .as_secs(),
    )
    .context("source mtime overflow")?;
    let source_path = input.to_string_lossy().into_owned();

    let existing_state = load_import_state(conn, &source_path)?;
    let resume = match existing_state {
        Some(state)
            if state.source_size_bytes == source_size_bytes
                && state.source_mtime_epoch == source_mtime_epoch =>
        {
            Some(state)
        }
        Some(_) => bail!("existing import checkpoint does not match the current source file"),
        None => None,
    };

    Ok(ImportSourceState {
        source_size_bytes,
        source_mtime_epoch,
        source_path,
        resume,
    })
}

pub fn parse_pubchem_line(line: &str) -> Result<(i64, String, String)> {
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

fn build_progress_bar(total_bytes: u64, start_offset: u64) -> Option<ProgressBar> {
    if !std::io::stderr().is_terminal() {
        return None;
    }

    let progress_bar = ProgressBar::new(total_bytes);
    progress_bar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta}) {msg}",
        )
        .expect("valid progress bar template")
        .progress_chars("#>-"),
    );
    progress_bar.set_position(start_offset);
    progress_bar.set_message("starting import");
    Some(progress_bar)
}

fn update_progress_bar(
    progress_bar: &Option<ProgressBar>,
    current_offset: u64,
    current_line: u64,
    rows_insert_attempted: u64,
) {
    if let Some(progress_bar) = progress_bar {
        progress_bar.set_position(current_offset);
        progress_bar.set_message(format!(
            "line={current_line} inserted={rows_insert_attempted}"
        ));
    }
}

fn finish_progress_bar(
    progress_bar: Option<ProgressBar>,
    current_offset: u64,
    current_line: u64,
    rows_insert_attempted: u64,
) {
    if let Some(progress_bar) = progress_bar {
        progress_bar.set_position(current_offset);
        progress_bar.finish_with_message(format!(
            "done: line={current_line} inserted={rows_insert_attempted}"
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::run_migrations;
    use diesel::Connection;
    use tempfile::tempdir;

    #[test]
    fn parses_pubchem_lines() {
        let line = "123\tInChI=1S/CH4/h1H4\tVNWKTOKETHGBQD-UHFFFAOYSA-N";
        let (cid, inchi, inchikey) = parse_pubchem_line(line).unwrap();
        assert_eq!(cid, 123);
        assert_eq!(inchi, "InChI=1S/CH4/h1H4");
        assert_eq!(inchikey, "VNWKTOKETHGBQD-UHFFFAOYSA-N");
    }

    #[test]
    fn import_has_pending_work_detects_completed_checkpoint() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.sqlite");
        let input_path = dir.path().join("CID-InChI-Key.txt");
        std::fs::write(
            &input_path,
            "1\tInChI=1S/CH4/h1H4\tVNWKTOKETHGBQD-UHFFFAOYSA-N\n",
        )
        .unwrap();

        let mut conn = SqliteConnection::establish(db_path.to_str().unwrap()).unwrap();
        run_migrations(&mut conn).unwrap();

        assert!(import_has_pending_work(&mut conn, &input_path).unwrap());

        let metadata = std::fs::metadata(&input_path).unwrap();
        let source_size_bytes = i64::try_from(metadata.len()).unwrap();
        let source_mtime_epoch = i64::try_from(
            metadata
                .modified()
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap();
        save_import_state(
            &mut conn,
            &input_path.to_string_lossy(),
            source_size_bytes,
            source_mtime_epoch,
            1,
            source_size_bytes,
            1_700_000_000,
        )
        .unwrap();

        assert!(!import_has_pending_work(&mut conn, &input_path).unwrap());
    }
}
