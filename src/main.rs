use anyhow::{Context, Result};
use clap::Parser;
use classyfire_crawler::config::{Cli, Commands, RuntimeConfig, DEFAULT_IMPORT_CHUNK_SIZE};
use classyfire_crawler::daemon::run_daemon;
use classyfire_crawler::db::{
    apply_import_pragmas, apply_runtime_pragmas, collect_stats, establish_pool, export_labels,
    rebuild_all_counters, recreate_runtime_indexes, run_migrations,
};
use classyfire_crawler::export::export_parquet;
use classyfire_crawler::importer::{import_has_pending_work, import_pubchem_dump};
use classyfire_crawler::zenodo;

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    match cli.command {
        Commands::ImportPubchem(args) => {
            let pool = establish_pool(&args.db_args.db)?;
            let mut conn = pool.get()?;
            run_migrations(&mut conn)?;
            let has_pending_work = import_has_pending_work(&mut conn, &args.input)?;
            let existing_stats = collect_stats(&mut conn)?;
            let needs_counter_rebuild = existing_stats.total_molecules > 0
                && existing_stats.new_count == 0
                && existing_stats.done_count == 0
                && existing_stats.miss_count == 0
                && existing_stats.error_count == 0;
            let needs_finalization = has_pending_work || needs_counter_rebuild;
            if has_pending_work {
                classyfire_crawler::db::drop_import_indexes(&mut conn)?;
                apply_import_pragmas(&mut conn)?;
            }
            let now = chrono::Utc::now().timestamp();
            let stats =
                import_pubchem_dump(&mut conn, &args.input, DEFAULT_IMPORT_CHUNK_SIZE, now)?;
            if needs_finalization {
                if !has_pending_work {
                    eprintln!("repairing incomplete previous import finalization");
                }
                eprintln!("finalizing import: restoring SQLite runtime settings");
                apply_runtime_pragmas(&mut conn)?;
                eprintln!("finalizing import: recreating indexes");
                recreate_runtime_indexes(&mut conn)?;
                eprintln!("finalizing import: rebuilding counters");
                rebuild_all_counters(&mut conn)?;
            } else {
                eprintln!(
                    "import is already up to date; skipped index rebuild and counter refresh"
                );
            }
            println!(
                "imported PubChem dump: resumed_from_line={} rows_read={} rows_insert_attempted={}",
                stats.resumed_from_line, stats.rows_read, stats.rows_insert_attempted
            );
        }
        Commands::RunGet(args) => {
            run_daemon(RuntimeConfig::from_env(args.db))?;
        }
        Commands::Stats(args) => {
            let pool = establish_pool(&args.db)?;
            let mut conn = pool.get()?;
            run_migrations(&mut conn)?;
            recreate_runtime_indexes(&mut conn)?;
            let stats = collect_stats(&mut conn)?;
            println!("total_molecules={}", stats.total_molecules);
            println!("new={}", stats.new_count);
            println!("done={}", stats.done_count);
            println!("miss={}", stats.miss_count);
            println!("error={}", stats.error_count);
        }
        Commands::ExportLabels(args) => {
            let pool = establish_pool(&args.db_args.db)?;
            let mut conn = pool.get()?;
            run_migrations(&mut conn)?;
            let exported = export_labels(&mut conn, &args.output)
                .with_context(|| format!("failed exporting labels to {}", args.output.display()))?;
            println!("exported_labels={exported}");
        }
        Commands::ExportParquet(args) => {
            let pool = establish_pool(&args.db_args.db)?;
            let mut conn = pool.get()?;
            run_migrations(&mut conn)?;
            let exported = export_parquet(&mut conn, &args.output).with_context(|| {
                format!(
                    "failed exporting Parquet labels to {}",
                    args.output.display()
                )
            })?;
            println!("exported_parquet_rows={exported}");
        }
        Commands::PublishZenodo(args) => {
            let config = RuntimeConfig::from_env(args.db);
            config.validate()?;
            let zenodo_config = config.zenodo_config().context(
                "ZENODO_TOKEN and CLASSYFIRE_ZENODO_DEPOSIT_ID must be set for publish-zenodo",
            )?;

            let pool = establish_pool(&config.db)?;
            let mut conn = pool.get()?;
            run_migrations(&mut conn)?;

            let stats = collect_stats(&mut conn)?;
            if stats.done_count == 0 {
                println!("skipped_zenodo_publish=no_done_rows");
                return Ok(());
            }

            let parquet_path = std::env::temp_dir().join(format!(
                "classyfire-labels-{}.parquet",
                chrono::Utc::now().format("%Y%m%dT%H%M%S")
            ));
            export_parquet(&mut conn, &parquet_path)?;
            let doi = zenodo::publish(
                &zenodo_config.token,
                &zenodo_config.deposit_id,
                &parquet_path,
                &stats,
            )?;
            let _ = std::fs::remove_file(&parquet_path);
            println!("published_zenodo_doi={doi}");
        }
        Commands::RebuildCounters(args) => {
            let pool = establish_pool(&args.db)?;
            let mut conn = pool.get()?;
            run_migrations(&mut conn)?;
            rebuild_all_counters(&mut conn)?;
            println!("rebuilt counters");
        }
    }
    Ok(())
}
