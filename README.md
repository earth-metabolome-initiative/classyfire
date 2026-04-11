# ClassyFire Streaming Downloader

[![CI](https://github.com/earth-metabolome-initiative/classyfire/actions/workflows/ci.yml/badge.svg)](https://github.com/earth-metabolome-initiative/classyfire/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19235916.svg)](https://doi.org/10.5281/zenodo.19235916)
[![license](https://img.shields.io/github/license/earth-metabolome-initiative/classyfire)](LICENSE)
![crawl status](https://img.shields.io/badge/crawl_status-257%2C112%2F123%2C541%2C080_labeled_%7C_12.0%2Fmin_%7C_ETA_~2045--10--08-blue)

Rust crawler for building a local copy of ClassyFire labels for PubChem compounds.

It streams the PubChem `CID-InChI-Key` table, queries ClassyFire through `GET /entities/{InChIKey}.json`, writes successful responses into compressed shards, and publishes weekly partial releases.

> [!WARNING]
> ClassyFire is usable for slow, long-run recovery, not for fast bulk export. This project keeps the request rate conservative and treats local archives as the durable output.

## Quick Start, then wait 20+ years

1. Download the PubChem file and decompress it to plain TSV:

```bash
curl -L -o CID-InChI-Key.gz \
  https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-InChI-Key.gz
gzip -cd CID-InChI-Key.gz > CID-InChI-Key.tsv
```

2. Create the environment file:

```bash
cp .env.example .env
```

Required variables:

- `ZENODO_TOKEN`
- `CLASSYFIRE_ZENODO_DEPOSIT_ID`

3. Run the crawler and wait 20+ years:

```bash
cargo run --release -- run \
  --input ./CID-InChI-Key.tsv \
  --output-dir ./classyfire-run
```

## License

MIT
