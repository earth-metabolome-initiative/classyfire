use crate::compression::decompress_json_bytes;
use crate::models::Molecule;
use crate::schema::molecules;
use crate::types::EntityResponse;
use anyhow::{anyhow, Context, Result};
use arrow::array::{Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use diesel::prelude::*;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::path::Path;
use std::sync::Arc;

const PAGE_SIZE: i64 = 10_000;

struct ExportRow {
    inchikey: String,
    inchi: String,
    smiles: Option<String>,
    kingdom: Option<String>,
    superclass: Option<String>,
    class_name: Option<String>,
    subclass: Option<String>,
    direct_parent: Option<String>,
    classification_version: Option<String>,
    classification_acquired_at: Option<i64>,
    entity_json: String,
}

pub fn export_parquet(conn: &mut SqliteConnection, output: &Path) -> Result<u64> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("inchikey", DataType::Utf8, false),
        Field::new("inchi", DataType::Utf8, false),
        Field::new("smiles", DataType::Utf8, true),
        Field::new("kingdom", DataType::Utf8, true),
        Field::new("superclass", DataType::Utf8, true),
        Field::new("class", DataType::Utf8, true),
        Field::new("subclass", DataType::Utf8, true),
        Field::new("direct_parent", DataType::Utf8, true),
        Field::new("classification_version", DataType::Utf8, true),
        Field::new("classification_acquired_at", DataType::Int64, true),
        Field::new("entity_json", DataType::Utf8, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();

    let file = std::fs::File::create(output)
        .with_context(|| format!("failed to create {}", output.display()))?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .context("failed to create Parquet writer")?;

    let mut last_inchikey: Option<String> = None;
    let mut total_rows = 0_u64;

    loop {
        let page = load_done_page(conn, last_inchikey.as_deref())?;
        if page.is_empty() {
            break;
        }

        let rows = page
            .iter()
            .map(export_row_from_molecule)
            .collect::<Result<Vec<_>>>()?;
        write_page(&mut writer, schema.clone(), &rows)?;

        total_rows += rows.len() as u64;
        last_inchikey = page.last().map(|row| row.inchikey.clone());

        if total_rows.is_multiple_of(100_000) {
            eprintln!("[export] {total_rows} rows written");
        }
    }

    writer.close().context("failed to close Parquet writer")?;
    let file_size = std::fs::metadata(output).map_or(0, |metadata| metadata.len());
    eprintln!(
        "[export] done: {total_rows} rows exported to {} ({:.1} MB)",
        output.display(),
        file_size as f64 / 1_048_576.0
    );
    Ok(total_rows)
}

fn load_done_page(
    conn: &mut SqliteConnection,
    after_inchikey: Option<&str>,
) -> Result<Vec<Molecule>> {
    let mut query = molecules::table
        .filter(molecules::state.eq(crate::db::STATE_DONE))
        .into_boxed();

    if let Some(last) = after_inchikey {
        query = query.filter(molecules::inchikey.gt(last));
    }

    Ok(query
        .order(molecules::inchikey.asc())
        .limit(PAGE_SIZE)
        .select(Molecule::as_select())
        .load::<Molecule>(conn)?)
}

fn export_row_from_molecule(molecule: &Molecule) -> Result<ExportRow> {
    let raw_json = molecule
        .raw_json_zstd
        .as_ref()
        .ok_or_else(|| anyhow!("done row {} is missing raw_json_zstd", molecule.inchikey))?;
    let entity_json = decompress_json_bytes(raw_json)
        .with_context(|| format!("failed to decompress raw JSON for {}", molecule.inchikey))?;
    let entity_json = String::from_utf8(entity_json).with_context(|| {
        format!(
            "stored raw JSON is not valid UTF-8 for {}",
            molecule.inchikey
        )
    })?;
    let entity: EntityResponse = serde_json::from_str(&entity_json).with_context(|| {
        format!(
            "stored raw JSON is not valid EntityResponse for {}",
            molecule.inchikey
        )
    })?;

    Ok(ExportRow {
        inchikey: molecule.inchikey.clone(),
        inchi: molecule.inchi.clone(),
        smiles: entity.smiles,
        kingdom: entity.kingdom.map(|node| node.name),
        superclass: entity.superclass.map(|node| node.name),
        class_name: entity.class_node.map(|node| node.name),
        subclass: entity.subclass.map(|node| node.name),
        direct_parent: entity.direct_parent.map(|node| node.name),
        classification_version: entity.classification_version,
        classification_acquired_at: molecule.classification_acquired_at,
        entity_json,
    })
}

fn write_page(
    writer: &mut ArrowWriter<std::fs::File>,
    schema: Arc<Schema>,
    rows: &[ExportRow],
) -> Result<()> {
    let len = rows.len();
    let mut inchikey_builder = StringBuilder::with_capacity(len, len * 30);
    let mut inchi_builder = StringBuilder::with_capacity(len, len * 64);
    let mut smiles_builder = StringBuilder::with_capacity(len, len * 32);
    let mut kingdom_builder = StringBuilder::with_capacity(len, len * 24);
    let mut superclass_builder = StringBuilder::with_capacity(len, len * 32);
    let mut class_builder = StringBuilder::with_capacity(len, len * 32);
    let mut subclass_builder = StringBuilder::with_capacity(len, len * 32);
    let mut direct_parent_builder = StringBuilder::with_capacity(len, len * 32);
    let mut version_builder = StringBuilder::with_capacity(len, len * 8);
    let mut acquired_builder = Int64Builder::with_capacity(len);
    let mut entity_json_builder = StringBuilder::with_capacity(len, len * 512);

    for row in rows {
        inchikey_builder.append_value(&row.inchikey);
        inchi_builder.append_value(&row.inchi);
        append_optional_string(&mut smiles_builder, row.smiles.as_deref());
        append_optional_string(&mut kingdom_builder, row.kingdom.as_deref());
        append_optional_string(&mut superclass_builder, row.superclass.as_deref());
        append_optional_string(&mut class_builder, row.class_name.as_deref());
        append_optional_string(&mut subclass_builder, row.subclass.as_deref());
        append_optional_string(&mut direct_parent_builder, row.direct_parent.as_deref());
        append_optional_string(&mut version_builder, row.classification_version.as_deref());
        append_optional_i64(&mut acquired_builder, row.classification_acquired_at);
        entity_json_builder.append_value(&row.entity_json);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(inchikey_builder.finish()),
            Arc::new(inchi_builder.finish()),
            Arc::new(smiles_builder.finish()),
            Arc::new(kingdom_builder.finish()),
            Arc::new(superclass_builder.finish()),
            Arc::new(class_builder.finish()),
            Arc::new(subclass_builder.finish()),
            Arc::new(direct_parent_builder.finish()),
            Arc::new(version_builder.finish()),
            Arc::new(acquired_builder.finish()),
            Arc::new(entity_json_builder.finish()),
        ],
    )
    .context("failed to create record batch")?;

    writer
        .write(&batch)
        .context("failed to write record batch")?;
    Ok(())
}

fn append_optional_string(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_optional_i64(builder: &mut Int64Builder, value: Option<i64>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::compress_json_bytes;
    use crate::db::{recreate_runtime_indexes, run_migrations, STATE_DONE};
    use crate::models::NewMolecule;
    use crate::schema::molecules;
    use crate::types::{EntityResponse, TaxNode};
    use arrow::array::{Int64Array, StringArray};
    use diesel::Connection;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::TempDir;

    fn open_test_conn() -> (TempDir, SqliteConnection) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("classyfire-export-test.sqlite");
        let mut conn = SqliteConnection::establish(db_path.to_str().unwrap()).unwrap();
        run_migrations(&mut conn).unwrap();
        recreate_runtime_indexes(&mut conn).unwrap();
        (dir, conn)
    }

    fn node(name: &str, chemont_id: &str) -> TaxNode {
        TaxNode {
            name: name.to_owned(),
            description: None,
            chemont_id: chemont_id.to_owned(),
            url: None,
        }
    }

    fn sample_entity() -> EntityResponse {
        EntityResponse {
            smiles: Some("CCCC".to_owned()),
            inchikey: Some("DONE-KEY".to_owned()),
            kingdom: Some(node("Organic compounds", "CHEMONTID:0000001")),
            superclass: Some(node("Lipids and lipid-like molecules", "CHEMONTID:0000002")),
            class_node: Some(node("Fatty Acyls", "CHEMONTID:0000003")),
            subclass: Some(node("Fatty acids and conjugates", "CHEMONTID:0000004")),
            intermediate_nodes: Vec::new(),
            direct_parent: Some(node("Long-chain fatty acids", "CHEMONTID:0000005")),
            alternative_parents: Vec::new(),
            molecular_framework: None,
            substituents: Vec::new(),
            description: None,
            external_descriptors: Vec::new(),
            ancestors: Vec::new(),
            predicted_chebi_terms: Vec::new(),
            predicted_lipidmaps_terms: Vec::new(),
            classification_version: Some("1.0".to_owned()),
        }
    }

    #[test]
    fn export_parquet_writes_done_rows() {
        let (dir, mut conn) = open_test_conn();
        let now = 1_700_000_000_i64;
        let raw_json = serde_json::to_vec(&sample_entity()).unwrap();
        let compressed = compress_json_bytes(&raw_json).unwrap();

        diesel::insert_into(molecules::table)
            .values(NewMolecule {
                inchikey: "DONE-KEY",
                inchi: "InChI=1S/DONE",
                state: STATE_DONE,
                attempts: 1,
                last_error: None,
                classification_acquired_at: Some(now),
                raw_json_zstd: Some(&compressed),
                created_at: now,
                updated_at: now,
            })
            .execute(&mut conn)
            .unwrap();

        let output = dir.path().join("labels.parquet");
        let exported = export_parquet(&mut conn, &output).unwrap();
        assert_eq!(exported, 1);

        let file = std::fs::File::open(output).unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        let inchikeys = batch
            .column_by_name("inchikey")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let inchi = batch
            .column_by_name("inchi")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let kingdom = batch
            .column_by_name("kingdom")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let acquired = batch
            .column_by_name("classification_acquired_at")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let entity_json = batch
            .column_by_name("entity_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(inchikeys.value(0), "DONE-KEY");
        assert_eq!(inchi.value(0), "InChI=1S/DONE");
        assert_eq!(kingdom.value(0), "Organic compounds");
        assert_eq!(acquired.value(0), now);
        assert!(entity_json
            .value(0)
            .contains("\"classification_version\":\"1.0\""));
    }
}
