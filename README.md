# ClassyFire Streaming Downloader

[![CI](https://github.com/earth-metabolome-initiative/classyfire/actions/workflows/ci.yml/badge.svg)](https://github.com/earth-metabolome-initiative/classyfire/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19235916.svg)](https://doi.org/10.5281/zenodo.19235916)
[![license](https://img.shields.io/github/license/earth-metabolome-initiative/classyfire)](LICENSE)

Rust downloader for streaming the PubChem `CID-InChI-Key` TSV and crawling ClassyFire only through `GET /entities/{InChIKey}.json`.

This project exists to build a local, durable copy of ClassyFire labels for PubChem compounds with a very small operational footprint. The crawler streams the input once, writes successful classifications directly into compressed output shards, and tracks terminal row states in compact bitmap files instead of a database.

The code deliberately sticks to the `GET /entities/{InChIKey}.json` path because it has been much less fragile than the batch query flow. The batch endpoints were accepted by the server, but in practice they were too unreliable to drain at scale, with slow queues, throttling, HTML error pages, and multi-page result retrieval failures. This downloader therefore optimizes for boring long-run stability rather than maximum short-term throughput.

> [!WARNING]
> The underlying service should be treated with caution. The original ClassyFire paper presents the system as a freely accessible large-scale API and discusses a path toward full open sourcing, but in practice the public service has been unreliable for bulk access and the historical software stack depended on proprietary ChemAxon components. This project therefore assumes that long-term durability must come from local copies, local exports, and periodic archival releases rather than trust in the upstream service remaining stable or fully reproducible.
>
> The full PubChem crawl is also extremely slow. PubChem currently contributes about 123.1 million unique `InChIKey`s. At the observed live rate of roughly 3.1 GET requests per minute, a full pass would take on the order of 75 years. Even at the nominal 5-second cadence used by this downloader (12 requests per minute), a full pass would still take about 19.5 years. In other words, this is a long-running label recovery project, not a short-term scrape.

## What It Does

- streams the PubChem `CID-InChI-Key` TSV directly from plain text or `.zst`
- calls `GET /entities/{InChIKey}.json` at a conservative fixed cadence
- writes successful results directly into rotating `JSONL.zst` shards
- tracks terminal row states in `mmap`ed bitmap files
- resumes from a small checkpoint file
- shows a small live TUI with the current key, recent results, and recent errors

The crawler is row-oriented:

- duplicate `InChIKey`s are allowed
- each input row is handled independently
- successful output keeps `CID`, `InChI`, `InChIKey`, and the full raw ClassyFire response

There is no SQLite database, no rebuild step, and no export step after the crawl. The success shards are the canonical artifact.

## Main Command

Run the downloader:

```bash
cargo run --release -- run \
  --input /data/CID-InChI-Key.full.txt.zst \
  --output-dir /data/classyfire-run
```

The output directory will contain:

```text
/data/classyfire-run/
  checkpoint.json
  state.success.bits
  state.miss.bits
  state.invalid.bits
  state.failed.bits
  success/
    success-000001.jsonl.zst
    success-000002.jsonl.zst
    ...
```

## CLI View

The downloader ships with a small live terminal dashboard so you can see the current key, last result, and recent errors while the crawl is running.

## Defaults

Runtime defaults are in `src/config.rs`:

- GET cadence: `5s`
- throttle backoff: `300s`
- request timeout: `30s`
- status refresh: `1s`
- success shard rotation: `100,000` records or `128 MiB`

All operational defaults can be overridden with `CLASSYFIRE_*` environment variables. The binary loads them from `.env` automatically at startup.

Start by copying the checked-in example:

```bash
cp .env.example .env
```

## Terminal States

Each input row ends in exactly one terminal state:

- `success`
- `miss`
- `invalid_input`
- `failed`

Only `success` produces an explicit output record. The other states exist only in the bitmap files.

## License

MIT
