# ClassyFire Streaming Downloader

[![CI](https://github.com/earth-metabolome-initiative/classyfire/actions/workflows/ci.yml/badge.svg)](https://github.com/earth-metabolome-initiative/classyfire/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19235916.svg)](https://doi.org/10.5281/zenodo.19235916)
[![license](https://img.shields.io/github/license/earth-metabolome-initiative/classyfire)](LICENSE)
[![crawl status](https://img.shields.io/badge/crawl_status-124%2C569%2F123%2C541%2C080_labeled_%7C_11.4%2Fmin_%7C_ETA_~2046--11--02-blue)](#project-status)

Rust crawler for building a local copy of ClassyFire labels for PubChem compounds.

It streams the PubChem `CID-InChI-Key` table, queries ClassyFire through `GET /entities/{InChIKey}.json`, writes successful responses into compressed shards, and publishes weekly partial releases.

> [!WARNING]
> ClassyFire is usable for slow, long-run recovery, not for fast bulk export. This project keeps the request rate conservative and treats local archives as the durable output.

## Project Status

- `124,569 / 123,541,080` compounds labeled (`0.101%`)
- current rate: about `11.4` compounds per minute
- expected completion: around `2046-11-02` if that rate holds
- partial releases are published weekly

## Quick Start

1. Download the PubChem file and decompress it to plain TSV:

```bash
curl -L -o CID-InChI-Key.gz \
  https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-InChI-Key.gz
gzip -cd CID-InChI-Key.gz > CID-InChI-Key.tsv
```

1. Create the environment file:

```bash
cp .env.example .env
```

Required variables:

- `ZENODO_TOKEN`
- `CLASSYFIRE_ZENODO_DEPOSIT_ID`

1. Run the crawler:

```bash
cargo run --release -- run \
  --input ./CID-InChI-Key.tsv \
  --output-dir ./classyfire-run
```

## License

MIT
