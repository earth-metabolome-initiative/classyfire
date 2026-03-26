use crate::compression::{compress_json_bytes, decompress_json_bytes};
use crate::models::{
    ImportState, Molecule, NewImportState, NewMolecule, NewStateCount, NewTaxonomyCount,
    StateCount, TaxonomyCount,
};
use crate::schema::{cid_map, import_state, molecules, state_counts, taxonomy_counts};
use crate::types::EntityResponse;
use anyhow::{anyhow, Context, Result};
use diesel::connection::SimpleConnection;
use diesel::dsl::count_star;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, CustomizeConnection, Pool, PooledConnection};
use diesel::upsert::excluded;
use diesel::{insert_or_ignore_into, QueryDsl, RunQueryDsl};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

pub const STATE_NEW: &str = "new";
pub const STATE_DONE: &str = "done";
pub const STATE_MISS: &str = "miss";
pub const STATE_ERROR: &str = "error";

const RUNTIME_PRAGMAS: &str = "PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA locking_mode = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA temp_store = MEMORY;
PRAGMA wal_autocheckpoint = 10000;
PRAGMA mmap_size = 34359738368;
PRAGMA cache_size = -262144;";

const IMPORT_PRAGMAS: &str = "PRAGMA foreign_keys = OFF;
PRAGMA synchronous = OFF;
PRAGMA locking_mode = NORMAL;
PRAGMA busy_timeout = 60000;";

pub type DbPool = Pool<ConnectionManager<SqliteConnection>>;
pub type DbConn = PooledConnection<ConnectionManager<SqliteConnection>>;

#[derive(Debug, Clone, Default)]
pub struct ImportStats {
    pub rows_read: u64,
    pub rows_insert_attempted: u64,
    pub resumed_from_line: u64,
}

#[derive(Debug, Clone)]
pub struct CounterEntry {
    pub label: String,
    pub count: i64,
}

#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub total_molecules: i64,
    pub new_count: i64,
    pub done_count: i64,
    pub miss_count: i64,
    pub error_count: i64,
}

#[derive(Debug, Clone)]
pub struct RunnerSnapshot {
    pub stats: StatsSnapshot,
    pub top_kingdoms: Vec<CounterEntry>,
    pub top_superclasses: Vec<CounterEntry>,
    pub top_classes: Vec<CounterEntry>,
}

#[derive(Debug)]
pub struct PoolConnectionOptions;

impl CustomizeConnection<SqliteConnection, diesel::r2d2::Error> for PoolConnectionOptions {
    fn on_acquire(
        &self,
        conn: &mut SqliteConnection,
    ) -> std::result::Result<(), diesel::r2d2::Error> {
        conn.batch_execute(RUNTIME_PRAGMAS)
            .map_err(diesel::r2d2::Error::QueryError)
    }
}

pub fn ensure_db_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("database path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    Ok(())
}

pub fn establish_pool(path: &Path) -> Result<DbPool> {
    ensure_db_parent(path)?;
    let manager = ConnectionManager::<SqliteConnection>::new(path.to_string_lossy().as_ref());
    let pool = Pool::builder()
        .max_size(2)
        .connection_customizer(Box::new(PoolConnectionOptions))
        .build(manager)
        .context("failed to create SQLite pool")?;
    Ok(pool)
}

pub fn run_migrations(conn: &mut SqliteConnection) -> Result<()> {
    conn.run_pending_migrations(MIGRATIONS)
        .map_err(|error| anyhow!("failed running migrations: {error}"))?;
    Ok(())
}

pub fn apply_import_pragmas(conn: &mut SqliteConnection) -> Result<()> {
    conn.batch_execute(IMPORT_PRAGMAS)?;
    Ok(())
}

pub fn apply_runtime_pragmas(conn: &mut SqliteConnection) -> Result<()> {
    conn.batch_execute(RUNTIME_PRAGMAS)?;
    Ok(())
}

pub fn drop_import_indexes(conn: &mut SqliteConnection) -> Result<()> {
    conn.batch_execute(
        "DROP INDEX IF EXISTS idx_molecules_state_error;
         DROP INDEX IF EXISTS idx_molecules_state_retry;
         DROP INDEX IF EXISTS idx_cid_map_inchikey;",
    )?;
    Ok(())
}

pub fn recreate_runtime_indexes(conn: &mut SqliteConnection) -> Result<()> {
    conn.batch_execute(
        "CREATE INDEX IF NOT EXISTS idx_molecules_state_error
             ON molecules(state, updated_at, attempts, inchikey);
         CREATE INDEX IF NOT EXISTS idx_cid_map_inchikey
             ON cid_map(inchikey);",
    )?;
    Ok(())
}

pub fn insert_molecule_chunk(
    conn: &mut SqliteConnection,
    rows: &[(i64, String, String)],
    now: i64,
) -> Result<()> {
    let new_molecules = rows
        .iter()
        .map(|(_, inchi, inchikey)| NewMolecule {
            inchikey,
            inchi,
            state: STATE_NEW,
            attempts: 0,
            last_error: None,
            classification_acquired_at: None,
            raw_json_zstd: None,
            created_at: now,
            updated_at: now,
        })
        .collect::<Vec<_>>();

    let new_cid_map = rows
        .iter()
        .map(|(cid, _, inchikey)| {
            (
                cid_map::cid.eq(*cid),
                cid_map::inchikey.eq(inchikey.as_str()),
            )
        })
        .collect::<Vec<_>>();

    conn.transaction(|conn| {
        insert_or_ignore_into(molecules::table)
            .values(&new_molecules)
            .execute(conn)?;
        insert_or_ignore_into(cid_map::table)
            .values(&new_cid_map)
            .execute(conn)?;
        Ok::<(), diesel::result::Error>(())
    })?;

    Ok(())
}

pub fn load_import_state(
    conn: &mut SqliteConnection,
    source_path_value: &str,
) -> Result<Option<ImportState>> {
    Ok(import_state::table
        .filter(import_state::source_path.eq(source_path_value))
        .select(ImportState::as_select())
        .first::<ImportState>(conn)
        .optional()?)
}

pub fn save_import_state(
    conn: &mut SqliteConnection,
    source_path_value: &str,
    source_size_bytes_value: i64,
    source_mtime_epoch_value: i64,
    last_committed_line_value: i64,
    last_committed_offset_value: i64,
    updated_at_value: i64,
) -> Result<()> {
    let row = NewImportState {
        source_path: source_path_value,
        source_size_bytes: source_size_bytes_value,
        source_mtime_epoch: source_mtime_epoch_value,
        last_committed_line: last_committed_line_value,
        last_committed_offset: last_committed_offset_value,
        updated_at: updated_at_value,
    };

    diesel::insert_into(import_state::table)
        .values(&row)
        .on_conflict(import_state::source_path)
        .do_update()
        .set((
            import_state::source_size_bytes.eq(excluded(import_state::source_size_bytes)),
            import_state::source_mtime_epoch.eq(excluded(import_state::source_mtime_epoch)),
            import_state::last_committed_line.eq(excluded(import_state::last_committed_line)),
            import_state::last_committed_offset.eq(excluded(import_state::last_committed_offset)),
            import_state::updated_at.eq(excluded(import_state::updated_at)),
        ))
        .execute(conn)?;
    Ok(())
}

pub fn select_next_molecule(conn: &mut SqliteConnection) -> Result<Option<Molecule>> {
    Ok(molecules::table
        .filter(
            molecules::state
                .eq(STATE_NEW)
                .or(molecules::state.eq(STATE_ERROR)),
        )
        .order((
            diesel::dsl::sql::<diesel::sql_types::Integer>(
                "CASE WHEN state = 'new' THEN 0 ELSE 1 END",
            ),
            molecules::updated_at.asc(),
            molecules::attempts.asc(),
            molecules::inchikey.asc(),
        ))
        .select(Molecule::as_select())
        .first::<Molecule>(conn)
        .optional()?)
}

pub fn mark_molecule_done(
    conn: &mut SqliteConnection,
    molecule: &Molecule,
    raw_json: &[u8],
    taxonomy_labels: &[(&'static str, String)],
    now: i64,
) -> Result<()> {
    let compressed = compress_json_bytes(raw_json)?;
    conn.transaction(|conn| {
        diesel::update(molecules::table.filter(molecules::inchikey.eq(&molecule.inchikey)))
            .set((
                molecules::state.eq(STATE_DONE),
                molecules::attempts.eq(molecule.attempts + 1),
                molecules::last_error.eq::<Option<String>>(None),
                molecules::classification_acquired_at.eq(Some(now)),
                molecules::raw_json_zstd.eq(Some(compressed.clone())),
                molecules::updated_at.eq(now),
            ))
            .execute(conn)?;
        transition_state_count(conn, &molecule.state, STATE_DONE)?;
        for (level, label) in taxonomy_labels {
            increment_taxonomy_count(conn, level, label)?;
        }
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

pub fn mark_molecule_miss(
    conn: &mut SqliteConnection,
    molecule: &Molecule,
    now: i64,
) -> Result<()> {
    conn.transaction(|conn| {
        diesel::update(molecules::table.filter(molecules::inchikey.eq(&molecule.inchikey)))
            .set((
                molecules::state.eq(STATE_MISS),
                molecules::attempts.eq(molecule.attempts + 1),
                molecules::last_error.eq::<Option<String>>(None),
                molecules::updated_at.eq(now),
            ))
            .execute(conn)?;
        transition_state_count(conn, &molecule.state, STATE_MISS)?;
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

pub fn mark_molecule_error(
    conn: &mut SqliteConnection,
    molecule: &Molecule,
    error: &str,
    now: i64,
) -> Result<()> {
    conn.transaction(|conn| {
        diesel::update(molecules::table.filter(molecules::inchikey.eq(&molecule.inchikey)))
            .set((
                molecules::state.eq(STATE_ERROR),
                molecules::attempts.eq(molecule.attempts + 1),
                molecules::last_error.eq(Some(error.to_owned())),
                molecules::updated_at.eq(now),
            ))
            .execute(conn)?;
        if molecule.state != STATE_ERROR {
            transition_state_count(conn, &molecule.state, STATE_ERROR)?;
        }
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

fn transition_state_count(
    conn: &mut SqliteConnection,
    from_state: &str,
    to_state: &str,
) -> Result<()> {
    if from_state == to_state {
        return Ok(());
    }
    decrement_state_count(conn, from_state)?;
    increment_state_count(conn, to_state)?;
    Ok(())
}

fn increment_state_count(conn: &mut SqliteConnection, state: &str) -> Result<()> {
    diesel::insert_into(state_counts::table)
        .values(NewStateCount { state, count: 1 })
        .on_conflict(state_counts::state)
        .do_update()
        .set(state_counts::count.eq(state_counts::count + 1))
        .execute(conn)?;
    Ok(())
}

fn decrement_state_count(conn: &mut SqliteConnection, state: &str) -> Result<()> {
    diesel::update(state_counts::table.filter(state_counts::state.eq(state)))
        .set(state_counts::count.eq(state_counts::count - 1))
        .execute(conn)?;
    diesel::delete(
        state_counts::table
            .filter(state_counts::state.eq(state))
            .filter(state_counts::count.le(0)),
    )
    .execute(conn)?;
    Ok(())
}

fn increment_taxonomy_count(conn: &mut SqliteConnection, level: &str, label: &str) -> Result<()> {
    diesel::insert_into(taxonomy_counts::table)
        .values(NewTaxonomyCount {
            level,
            label,
            count: 1,
        })
        .on_conflict((taxonomy_counts::level, taxonomy_counts::label))
        .do_update()
        .set(taxonomy_counts::count.eq(taxonomy_counts::count + 1))
        .execute(conn)?;
    Ok(())
}

pub fn rebuild_state_counts(conn: &mut SqliteConnection) -> Result<()> {
    diesel::delete(state_counts::table).execute(conn)?;
    let counts = molecules::table
        .group_by(molecules::state)
        .select((molecules::state, count_star()))
        .load::<(String, i64)>(conn)?;
    let inserts = counts
        .iter()
        .map(|(state, count)| NewStateCount {
            state: state.as_str(),
            count: *count,
        })
        .collect::<Vec<_>>();
    if !inserts.is_empty() {
        diesel::insert_into(state_counts::table)
            .values(&inserts)
            .execute(conn)?;
    }
    Ok(())
}

pub fn rebuild_taxonomy_counts(conn: &mut SqliteConnection) -> Result<()> {
    diesel::delete(taxonomy_counts::table).execute(conn)?;
    let rows = molecules::table
        .filter(molecules::state.eq(STATE_DONE))
        .filter(molecules::raw_json_zstd.is_not_null())
        .select((molecules::raw_json_zstd,))
        .load::<(Option<Vec<u8>>,)>(conn)?;

    let mut counts = BTreeMap::<(String, String), i64>::new();
    for (maybe_blob,) in rows {
        let Some(blob) = maybe_blob else {
            continue;
        };
        let json = decompress_json_bytes(&blob)?;
        let entity: EntityResponse =
            serde_json::from_slice(&json).context("failed to parse stored entity JSON")?;
        for (level, label) in entity.taxonomy_labels() {
            *counts.entry((level.to_owned(), label)).or_insert(0) += 1;
        }
    }

    let inserts = counts
        .iter()
        .map(|((level, label), count)| NewTaxonomyCount {
            level,
            label,
            count: *count,
        })
        .collect::<Vec<_>>();
    if !inserts.is_empty() {
        diesel::insert_into(taxonomy_counts::table)
            .values(&inserts)
            .execute(conn)?;
    }
    Ok(())
}

pub fn rebuild_all_counters(conn: &mut SqliteConnection) -> Result<()> {
    rebuild_state_counts(conn)?;
    rebuild_taxonomy_counts(conn)?;
    Ok(())
}

pub fn collect_stats(conn: &mut SqliteConnection) -> Result<StatsSnapshot> {
    let total_molecules = molecules::table.select(count_star()).first::<i64>(conn)?;
    let states = state_counts::table
        .select(StateCount::as_select())
        .load::<StateCount>(conn)?;
    Ok(StatsSnapshot {
        total_molecules,
        new_count: state_count(&states, STATE_NEW),
        done_count: state_count(&states, STATE_DONE),
        miss_count: state_count(&states, STATE_MISS),
        error_count: state_count(&states, STATE_ERROR),
    })
}

pub fn collect_runner_snapshot(conn: &mut SqliteConnection, top_n: i64) -> Result<RunnerSnapshot> {
    Ok(RunnerSnapshot {
        stats: collect_stats(conn)?,
        top_kingdoms: top_taxonomy_counts(conn, "kingdom", top_n)?,
        top_superclasses: top_taxonomy_counts(conn, "superclass", top_n)?,
        top_classes: top_taxonomy_counts(conn, "class", top_n)?,
    })
}

fn top_taxonomy_counts(
    conn: &mut SqliteConnection,
    level: &str,
    top_n: i64,
) -> Result<Vec<CounterEntry>> {
    Ok(taxonomy_counts::table
        .filter(taxonomy_counts::level.eq(level))
        .order((taxonomy_counts::count.desc(), taxonomy_counts::label.asc()))
        .limit(top_n)
        .select(TaxonomyCount::as_select())
        .load::<TaxonomyCount>(conn)?
        .into_iter()
        .map(|row| CounterEntry {
            label: row.label,
            count: row.count,
        })
        .collect())
}

pub fn export_labels(conn: &mut SqliteConnection, output: &Path) -> Result<u64> {
    ensure_db_parent(output)?;
    let file = fs::File::create(output)
        .with_context(|| format!("failed to create {}", output.display()))?;
    let mut writer = BufWriter::new(file);
    let mut exported = 0u64;
    let mut last_inchikey: Option<String> = None;
    const PAGE_SIZE: i64 = 1000;

    loop {
        let mut query = molecules::table
            .filter(molecules::state.eq(STATE_DONE))
            .order(molecules::inchikey.asc())
            .into_boxed();
        if let Some(ref last) = last_inchikey {
            query = query.filter(molecules::inchikey.gt(last));
        }

        let rows = query
            .limit(PAGE_SIZE)
            .select(Molecule::as_select())
            .load::<Molecule>(conn)?;
        if rows.is_empty() {
            break;
        }

        for row in &rows {
            if let Some(raw) = &row.raw_json_zstd {
                let json = decompress_json_bytes(raw)?;
                let entity: Value =
                    serde_json::from_slice(&json).context("failed to parse stored raw JSON")?;
                let line = serde_json::json!({
                    "inchikey": row.inchikey,
                    "inchi": row.inchi,
                    "entity": entity
                });
                serde_json::to_writer(&mut writer, &line)?;
                writer.write_all(b"\n")?;
                exported += 1;
            }
        }

        last_inchikey = rows.last().map(|row| row.inchikey.clone());
    }

    writer.flush()?;
    Ok(exported)
}

fn state_count(states: &[StateCount], target: &str) -> i64 {
    states
        .iter()
        .find_map(|row| (row.state == target).then_some(row.count))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{molecules, taxonomy_counts};
    use crate::types::{EntityResponse, TaxNode};
    use diesel::Connection;
    use serde_json::Value;
    use tempfile::TempDir;

    fn open_test_conn() -> (TempDir, SqliteConnection) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("classyfire-test.sqlite");
        let mut conn = SqliteConnection::establish(db_path.to_str().unwrap()).unwrap();
        run_migrations(&mut conn).unwrap();
        recreate_runtime_indexes(&mut conn).unwrap();
        (dir, conn)
    }

    fn make_node(name: &str, chemont_id: &str) -> TaxNode {
        TaxNode {
            name: name.to_owned(),
            description: None,
            chemont_id: chemont_id.to_owned(),
            url: None,
        }
    }

    fn sample_entity(inchikey: &str) -> EntityResponse {
        EntityResponse {
            smiles: Some("CCCC".to_owned()),
            inchikey: Some(inchikey.to_owned()),
            kingdom: Some(make_node("Organic compounds", "CHEMONTID:0000001")),
            superclass: Some(make_node(
                "Lipids and lipid-like molecules",
                "CHEMONTID:0000002",
            )),
            class_node: Some(make_node("Fatty Acyls", "CHEMONTID:0000003")),
            subclass: Some(make_node("Fatty acids and conjugates", "CHEMONTID:0000004")),
            intermediate_nodes: Vec::new(),
            direct_parent: Some(make_node("Long-chain fatty acids", "CHEMONTID:0000005")),
            alternative_parents: Vec::new(),
            molecular_framework: None,
            substituents: Vec::new(),
            description: Some("sample".to_owned()),
            external_descriptors: Vec::new(),
            ancestors: Vec::new(),
            predicted_chebi_terms: Vec::new(),
            predicted_lipidmaps_terms: Vec::new(),
            classification_version: Some("1.0".to_owned()),
        }
    }

    fn sample_entity_json(inchikey: &str) -> Vec<u8> {
        serde_json::to_vec(&sample_entity(inchikey)).unwrap()
    }

    fn insert_test_molecule(
        conn: &mut SqliteConnection,
        inchikey: &str,
        inchi: &str,
        state: &str,
        raw_json: Option<&[u8]>,
        now: i64,
    ) {
        let compressed = raw_json.map(|bytes| compress_json_bytes(bytes).unwrap());
        let row = NewMolecule {
            inchikey,
            inchi,
            state,
            attempts: 0,
            last_error: (state == STATE_ERROR).then_some("retry me"),
            classification_acquired_at: (state == STATE_DONE).then_some(now),
            raw_json_zstd: compressed.as_deref(),
            created_at: now,
            updated_at: now,
        };
        diesel::insert_into(molecules::table)
            .values(&row)
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn import_stats_default() {
        let stats = ImportStats::default();
        assert_eq!(stats.rows_read, 0);
        assert_eq!(stats.rows_insert_attempted, 0);
        assert_eq!(stats.resumed_from_line, 0);
    }

    #[test]
    fn migrations_work_on_fresh_temp_db() {
        let (_dir, mut conn) = open_test_conn();
        let stats = collect_stats(&mut conn).unwrap();
        assert_eq!(stats.total_molecules, 0);
        assert_eq!(stats.new_count, 0);
        assert_eq!(stats.done_count, 0);
        assert_eq!(stats.miss_count, 0);
        assert_eq!(stats.error_count, 0);
    }

    #[test]
    fn rebuild_all_counters_reconstructs_state_and_taxonomy_counts() {
        let (_dir, mut conn) = open_test_conn();
        let now = 1_700_000_000;

        insert_test_molecule(
            &mut conn,
            "DONE-KEY",
            "InChI=1S/DONE",
            STATE_DONE,
            Some(&sample_entity_json("DONE-KEY")),
            now,
        );
        insert_test_molecule(
            &mut conn,
            "MISS-KEY",
            "InChI=1S/MISS",
            STATE_MISS,
            None,
            now,
        );
        insert_test_molecule(
            &mut conn,
            "ERROR-KEY",
            "InChI=1S/ERROR",
            STATE_ERROR,
            None,
            now,
        );

        diesel::insert_into(state_counts::table)
            .values(NewStateCount {
                state: "bogus",
                count: 99,
            })
            .execute(&mut conn)
            .unwrap();
        diesel::insert_into(taxonomy_counts::table)
            .values(NewTaxonomyCount {
                level: "kingdom",
                label: "Bogus",
                count: 99,
            })
            .execute(&mut conn)
            .unwrap();

        rebuild_all_counters(&mut conn).unwrap();

        let stats = collect_stats(&mut conn).unwrap();
        assert_eq!(stats.total_molecules, 3);
        assert_eq!(stats.new_count, 0);
        assert_eq!(stats.done_count, 1);
        assert_eq!(stats.miss_count, 1);
        assert_eq!(stats.error_count, 1);

        let snapshot = collect_runner_snapshot(&mut conn, 5).unwrap();
        assert_eq!(snapshot.top_kingdoms.len(), 1);
        assert_eq!(snapshot.top_kingdoms[0].label, "Organic compounds");
        assert_eq!(snapshot.top_kingdoms[0].count, 1);
        assert_eq!(
            snapshot.top_superclasses[0].label,
            "Lipids and lipid-like molecules"
        );
        assert_eq!(snapshot.top_classes[0].label, "Fatty Acyls");
    }

    #[test]
    fn export_labels_writes_done_rows_as_jsonl() {
        let (dir, mut conn) = open_test_conn();
        let now = 1_700_000_000;

        insert_test_molecule(
            &mut conn,
            "DONE-KEY",
            "InChI=1S/DONE",
            STATE_DONE,
            Some(&sample_entity_json("DONE-KEY")),
            now,
        );
        insert_test_molecule(
            &mut conn,
            "MISS-KEY",
            "InChI=1S/MISS",
            STATE_MISS,
            None,
            now,
        );
        rebuild_all_counters(&mut conn).unwrap();

        let output = dir.path().join("labels.jsonl");
        let exported = export_labels(&mut conn, &output).unwrap();
        assert_eq!(exported, 1);

        let contents = std::fs::read_to_string(output).unwrap();
        let mut lines = contents.lines();
        let value: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert!(lines.next().is_none());
        assert_eq!(value["inchikey"], "DONE-KEY");
        assert_eq!(value["inchi"], "InChI=1S/DONE");
        assert_eq!(value["entity"]["classification_version"], "1.0");
        assert_eq!(value["entity"]["kingdom"]["name"], "Organic compounds");
    }

    #[test]
    fn mark_molecule_state_transitions_update_counts() {
        let (_dir, mut conn) = open_test_conn();
        let now = 1_700_000_000;

        insert_test_molecule(&mut conn, "DONE-KEY", "InChI=1S/DONE", STATE_NEW, None, now);
        insert_test_molecule(&mut conn, "MISS-KEY", "InChI=1S/MISS", STATE_NEW, None, now);
        insert_test_molecule(
            &mut conn,
            "ERROR-KEY",
            "InChI=1S/ERROR",
            STATE_NEW,
            None,
            now,
        );
        rebuild_state_counts(&mut conn).unwrap();

        let done_molecule = molecules::table
            .find("DONE-KEY")
            .select(Molecule::as_select())
            .first::<Molecule>(&mut conn)
            .unwrap();
        let done_json = sample_entity_json("DONE-KEY");
        let done_entity: EntityResponse = serde_json::from_slice(&done_json).unwrap();
        let done_labels = done_entity.taxonomy_labels();
        mark_molecule_done(&mut conn, &done_molecule, &done_json, &done_labels, now + 1).unwrap();

        let miss_molecule = molecules::table
            .find("MISS-KEY")
            .select(Molecule::as_select())
            .first::<Molecule>(&mut conn)
            .unwrap();
        mark_molecule_miss(&mut conn, &miss_molecule, now + 2).unwrap();

        let error_molecule = molecules::table
            .find("ERROR-KEY")
            .select(Molecule::as_select())
            .first::<Molecule>(&mut conn)
            .unwrap();
        mark_molecule_error(&mut conn, &error_molecule, "temporary failure", now + 3).unwrap();

        let errored_again = molecules::table
            .find("ERROR-KEY")
            .select(Molecule::as_select())
            .first::<Molecule>(&mut conn)
            .unwrap();
        mark_molecule_error(
            &mut conn,
            &errored_again,
            "temporary failure again",
            now + 4,
        )
        .unwrap();

        let stats = collect_stats(&mut conn).unwrap();
        assert_eq!(stats.total_molecules, 3);
        assert_eq!(stats.new_count, 0);
        assert_eq!(stats.done_count, 1);
        assert_eq!(stats.miss_count, 1);
        assert_eq!(stats.error_count, 1);

        let error_row = molecules::table
            .find("ERROR-KEY")
            .select(Molecule::as_select())
            .first::<Molecule>(&mut conn)
            .unwrap();
        assert_eq!(error_row.attempts, 2);
        assert_eq!(
            error_row.last_error.as_deref(),
            Some("temporary failure again")
        );

        let snapshot = collect_runner_snapshot(&mut conn, 5).unwrap();
        assert_eq!(snapshot.top_kingdoms[0].label, "Organic compounds");
        assert_eq!(snapshot.top_kingdoms[0].count, 1);
    }

    #[test]
    fn select_next_molecule_prefers_new_before_error() {
        let (_dir, mut conn) = open_test_conn();
        let now = 1_700_000_000;

        insert_test_molecule(
            &mut conn,
            "ERROR-KEY",
            "InChI=1S/ERROR",
            STATE_ERROR,
            None,
            now,
        );
        insert_test_molecule(
            &mut conn,
            "NEW-KEY",
            "InChI=1S/NEW",
            STATE_NEW,
            None,
            now + 1,
        );

        let selected = select_next_molecule(&mut conn).unwrap().unwrap();
        assert_eq!(selected.inchikey, "NEW-KEY");

        diesel::update(molecules::table.filter(molecules::inchikey.eq("NEW-KEY")))
            .set(molecules::state.eq(STATE_DONE))
            .execute(&mut conn)
            .unwrap();

        let selected = select_next_molecule(&mut conn).unwrap().unwrap();
        assert_eq!(selected.inchikey, "ERROR-KEY");
    }
}
