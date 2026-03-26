# Streaming Redesign

## Status

This document describes the streaming architecture now implemented in the crawler.

Current design choices:

- no SQLite
- no exact deduplication by `InChIKey`
- one success record per input row
- terminal row states tracked in `mmap`ed bitmaps
- only successful classifications emitted as explicit output files
- canonical output format is `JSONL.zst`, not Parquet

## Goals

- run for a long time on a small VM
- keep RAM usage flat and predictable
- minimize disk growth and temporary disk spikes
- keep the canonical artifact as close as possible to the original ClassyFire response
- avoid heavyweight rebuild/export phases

## Non-Goals

- exact deduplication across repeated `InChIKey`s
- retry passes over transient failures
- a canonical Parquet sink
- a general-purpose local query database

## Rationale

The current design pays for several things we do not actually need:

- SQLite schema and migrations
- a large imported work queue
- rebuild procedures
- export procedures
- a second serialization step to produce publishable artifacts

The replacement design should instead:

- stream the PubChem input once
- classify rows directly
- write successful results directly into final compressed shards
- track non-success outcomes in compact row-indexed state bitmaps

## Input Model

The input remains the PubChem `CID-InChI-Key` TSV stream.

Important choices:

- Duplicates are allowed.
- If the same `InChIKey` appears in multiple rows, each row is treated independently.
- Success output is row-oriented, not unique-`InChIKey`-oriented.

This means repeated `InChIKey`s may produce repeated upstream requests and repeated successful output records. This is intentional.

## On-Disk Layout

Recommended runtime layout:

```text
run/
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

Notes:

- `checkpoint.json` stores the current scan position and current output shard metadata.
- `checkpoint.json` also keeps the ntfy topic so restarts reuse the same status URL.
- external release tooling can reuse that saved topic through `notify-zenodo-release` to announce a finished Zenodo record.
- The four `*.bits` files are `mmap`ed bitmaps indexed by input row number.
- The `success/` directory contains the canonical artifact shards.

## State Representation

Use four parallel row-indexed bitmaps:

- `state.success.bits`
- `state.miss.bits`
- `state.invalid.bits`
- `state.failed.bits`

Interpretation:

- all four bits unset => `unseen`
- exactly one bit set => terminal state for that row

This keeps the representation simple, `mmap`-friendly, and compact.

At 130,000,000 input rows:

- 1 bitmap = 130,000,000 bits = 16,250,000 bytes
- 4 bitmaps = 65,000,000 bytes
- total bitmap storage is about 62 MiB

Rules:

- a row may transition only once from `unseen` to a terminal state
- terminal states are immutable
- startup should validate that no row has more than one terminal bit set

## Terminal States

The redesign keeps these terminal classes distinct:

- `success`
- `miss`
- `invalid_input`
- `failed`

Definitions:

- `success`: valid input row, successful request, and classified entity returned
- `miss`: valid input row, request succeeded, but no classification exists
- `invalid_input`: malformed TSV row or invalid/missing `InChIKey`
- `failed`: valid input row, but request/parse/upstream handling failed in a non-retryable way for this design

`failed` is terminal because this redesign deliberately does not support retry passes.

## Success Record Format

Canonical success shards are `JSONL.zst`.

Each line should carry:

- `row_index`
- `cid`
- `inchi`
- `inchikey`
- `fetched_at`
- `classyfire`

Example:

```json
{
  "row_index": 1234567,
  "cid": 2244,
  "inchi": "InChI=1S/CH4/h1H4",
  "inchikey": "VNWKTOKETHGBQD-UHFFFAOYSA-N",
  "fetched_at": "2026-03-26T12:34:56Z",
  "classyfire": {
    "...": "full raw ClassyFire response object"
  }
}
```

Design rules:

- store the full raw ClassyFire response object
- do not separately duplicate flattened taxonomy fields in the canonical artifact
- keep the local envelope minimal

This makes the success artifact the archival source of truth and avoids later recrawling when downstream field requirements change.

## Why Not Parquet

Parquet is not the right canonical sink for this crawler.

Reasons:

- the payload is nested and irregular
- row-oriented append behavior matters more than columnar analytics
- completed shards should be directly publishable
- schema evolution is easier with JSONL

Measured on the current labeled subset:

- labeled rows: `346`
- raw `JSONL`: `2,590,439` bytes
- `JSONL.zst`: `185,071` bytes
- current Parquet export: `243,863` bytes

That sample is small, but it is enough to show that the current nested payload compresses very well as `JSONL.zst`, and better than the current Parquet export implementation.

Parquet may still exist later as a derived analytics artifact, but it should not be the primary crawl sink.

## Estimated Success Artifact Size

Using the current measured `JSONL.zst` size:

- `185,071 / 346 = about 535 bytes per successful row`

Rough extrapolations if future rows are similar:

- `1M` successful rows => about `0.50 GiB`
- `10M` successful rows => about `5.0 GiB`
- `123.13M` successful rows => about `61 GiB`

These are only rough estimates. Real size depends on the future shape and compressibility of the returned ClassyFire payloads.

## Rotation Strategy

Success output should rotate by both:

- record count
- file size

Recommended initial policy:

- rotate when either threshold is reached
- keep thresholds configurable

Reasoning:

- count-based rotation is simple and predictable
- size-based rotation keeps upload units bounded

Reasonable first defaults:

- `100,000` success records per shard
- `128 MiB` compressed shard target

The exact defaults can be tuned later.

## Checkpoint File

`checkpoint.json` should be tiny and explicit.

Suggested fields:

- `input_path`
- `input_size_bytes`
- `input_mtime_epoch`
- `current_row_index`
- `current_success_shard_id`
- `current_success_records`

Example:

```json
{
  "input_path": "/data/CID-InChI-Key.full.txt.zst",
  "input_size_bytes": 1234567890,
  "input_mtime_epoch": 1767225600,
  "current_row_index": 8345221,
  "current_success_shard_id": 12,
  "current_success_records": 48123
}
```

## Main Crawl Loop

The crawler should do only this:

1. Open the input stream.
2. Open or create the checkpoint file.
3. Open or create the four bitmap files and `mmap` them.
4. Open the current success shard writer.
5. Stream rows in order.
6. If the row is already terminal in the bitmap, skip it.
7. Parse the TSV row.
8. If invalid, mark `invalid_input`.
9. Otherwise call `GET /entities/{InChIKey}.json`.
10. If classified, append one success record and mark `success`.
11. If definite miss, mark `miss`.
12. Otherwise mark `failed`.
13. Periodically flush shard writer, bitmaps, and checkpoint.
14. Rotate success shard when count or size limit is reached.

There should be no export phase after this loop. The output shards are already the canonical artifact.

## Failure Policy

This redesign deliberately does not include a retry system.

Implications:

- transient upstream failures are terminal and go to `failed`
- there is no second pass over `failed` rows
- the code stays much smaller and simpler

This is acceptable because:

- the crawl is already extremely slow
- restarts are expected to be rare
- the implementation goal is lean archival harvesting, not exhaustive recovery

## Restart Behavior

Restarts are expected to be rare. The design should optimize for simplicity, not perfect fast resume.

Preferred restart model:

- reopen the bitmaps and checkpoint
- reopen or roll the current success shard
- resume streaming input
- skip rows already marked terminal

This may require replaying part of the input stream to reach the saved position. That is acceptable for the first implementation if it removes the need for a heavier local indexing layer.

If restart cost later becomes a real problem, add a lighter-weight source indexing mechanism. Do not reintroduce SQLite just to solve that.

## Upload Model

Uploads should operate on completed success shards directly.

This means:

- no DB export step
- no temporary full-dataset Parquet creation
- no weekly rebuild

A publisher can simply:

- detect newly closed `success-*.jsonl.zst` shards
- upload them directly
- record uploaded shard ids in a tiny manifest if needed

## What Got Removed From The Codebase

The streaming cutover deletes:

- SQLite database schema
- import phase into SQLite
- aggregate counter rebuild paths
- JSONL export from SQLite
- Parquet export from SQLite
- publish-via-temporary-export flow

The resulting crawler should be mostly:

- input reader
- classifier
- bitmap state manager
- success shard writer
- optional uploader

## Remaining Work

The core runtime is now in place. The main optional follow-up is direct upload of completed success shards.
