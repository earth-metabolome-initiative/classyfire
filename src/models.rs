use crate::schema::{cid_map, import_state, molecules, state_counts, taxonomy_counts};
use diesel::{Identifiable, Insertable, Queryable, Selectable};

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = molecules, primary_key(inchikey))]
pub struct Molecule {
    pub inchikey: String,
    pub inchi: String,
    pub state: String,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub classification_acquired_at: Option<i64>,
    pub raw_json_zstd: Option<Vec<u8>>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = molecules)]
pub struct NewMolecule<'a> {
    pub inchikey: &'a str,
    pub inchi: &'a str,
    pub state: &'a str,
    pub attempts: i32,
    pub last_error: Option<&'a str>,
    pub classification_acquired_at: Option<i64>,
    pub raw_json_zstd: Option<&'a [u8]>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = cid_map, primary_key(cid))]
pub struct CidMap {
    pub cid: i64,
    pub inchikey: String,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = cid_map)]
pub struct NewCidMap<'a> {
    pub cid: i64,
    pub inchikey: &'a str,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = state_counts, primary_key(state))]
pub struct StateCount {
    pub state: String,
    pub count: i64,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = state_counts)]
pub struct NewStateCount<'a> {
    pub state: &'a str,
    pub count: i64,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = taxonomy_counts, primary_key(level, label))]
pub struct TaxonomyCount {
    pub level: String,
    pub label: String,
    pub count: i64,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = taxonomy_counts)]
pub struct NewTaxonomyCount<'a> {
    pub level: &'a str,
    pub label: &'a str,
    pub count: i64,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = import_state, primary_key(source_path))]
pub struct ImportState {
    pub source_path: String,
    pub source_size_bytes: i64,
    pub source_mtime_epoch: i64,
    pub last_committed_line: i64,
    pub last_committed_offset: i64,
    pub updated_at: i64,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = import_state)]
pub struct NewImportState<'a> {
    pub source_path: &'a str,
    pub source_size_bytes: i64,
    pub source_mtime_epoch: i64,
    pub last_committed_line: i64,
    pub last_committed_offset: i64,
    pub updated_at: i64,
}
