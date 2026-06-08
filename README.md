# oc_internship

Graph enrichment pipeline for OpenCitations bibliographic data. The pipeline reads a `.tar.gz` archive containing CSV files from the OpenCitations Meta dump, creates RDF bibliographic resources, and enriches them by querying external APIs (Crossref, OpenAlex) to find missing identifiers such as DOIs and OpenAlex IDs.

## Setup

```bash
git clone https://github.com/theair-hub/oc_internship
cd oc_internship
git submodule update --init --recursive
python -m uv sync
```

## Usage

```bash
python enricher_support.py --csv_path "path/to/archive.tar.gz"
```

A sample archive with 10 CSV files is included for testing:

```bash
python enricher_support.py --csv_path "sample_10.tar.gz"
```

### Main arguments

| Argument | Default | Description |
|---|---|---|
| `--csv_path` | required | Path to the `.tar.gz` archive |
| `--num-csv` | all | Number of CSV files to process |
| `--batch-size` | 10 | CSVs to process before flushing the graph |
| `--test-limit` | None | Stop after N entities |

## Output

| File/Folder | Description |
|---|---|
| `enriched/enriched.jsonld` | Enriched graph |
| `provenance/provenance.nq` | Provenance |
| `incomplete.nt` | BRs missing at least one identifier |
| `processed_files.json` | Already processed CSVs (allows resuming interrupted runs) |
