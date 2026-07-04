# OC Enricher Support

A command-line tool that reads an OpenCitations Meta CSV dump, builds
Bibliographic Resources (BRs) compliant with the OpenCitations Data Model
(OCDM) using [`oc_ocdm`](https://pypi.org/project/oc-ocdm/), enriches them
with external identifiers using
[`oc_graphenricher`](https://opencitations.github.io/oc_graphenricher/intro.html)'s
`GraphEnricher` (Crossref, OpenAlex, Wikidata, VIAF, ORCID), and saves the
result to disk via `oc_graphenricher`'s storage layer
(`single_file_storage` / `directory_storage`).

This script is a thin orchestration layer: it does the CSV parsing and BR
creation itself, then delegates all enrichment logic and output writing to
`oc_graphenricher`.

## What it does

1. **Reads** a `.tar.gz` archive containing OC Meta CSV files (each row has
   an `id` field with an `omid:` plus zero or more other identifiers, and a
   `title` field).
2. **Creates** an OCDM `BibliographicResource` for each row, attaching any
   identifiers already present in the CSV (DOI, ISSN, ISBN, PMID, PMCID,
   OpenAlex, URL).
3. **Enriches** each batch of resources by querying external sources
   (Crossref and OpenAlex always; Wikidata, VIAF, and ORCID optionally) to
   find and attach any missing identifiers.
4. **Saves** the enriched graph and its provenance to disk, either as a
   single JSON file pair per batch or in the OCDM bucketed directory layout.
5. **Resumes safely**: already-processed CSV files are tracked in
   `processed_files.json`, so re-running the same command skips what's
   already done.

## What it's for

If you have a bulk OC Meta dump and want an OCDM-compliant, identifier-rich
RDF graph without writing any code, this script does the whole pipeline —
parsing, resource creation, enrichment, and storage — from a single
terminal command, with every relevant option configurable via flags.

## Relationship to `oc_graphenricher`

`enricher_support.py` is a thin CLI wrapper around
[`oc_graphenricher`](https://opencitations.github.io/oc_graphenricher/intro.html), the
library that actually performs the enrichment and storage. Specifically:

- Resource/identifier creation uses `oc_ocdm.graph.GraphSet` directly
  (built from CSV rows, not from an existing RDF file via `Reader`).
- Enrichment is delegated to `oc_graphenricher.enricher.GraphEnricher`,
  called once per batch with `use_wikidata` / `use_viaf` / `use_orcid` /
  `checkpoint_interval` set from the CLI flags below.
- Storage is delegated to `oc_graphenricher.storage.single_file_storage`
  or `oc_graphenricher.storage.directory_storage`, selected and configured
  via the CLI flags below (`--storage-type`, `--supplier-prefix`,
  `--output-format`, `--zip-output`, `--items-per-directory`,
  `--items-per-file`, `--wanted-label`, `--info-dir`).

If you need behavior beyond what's exposed here (e.g. `counter_handler`,
or using `Reader` to enrich an existing `.nt`/RDF file instead of a CSV
dump), use `oc_graphenricher` directly — see its
[documentation](https://opencitations.github.io/oc_graphenricher/intro.html) for the full API.

Requires `oc_graphenricher>=2.1.0`.

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (recommended) or `pip` + a virtual
  environment

## Setup

### With uv (recommended)

```bash
uv init                              # only if the project isn't set up yet
uv add "oc_graphenricher>=2.1.0"
uv add oc_ocdm polars psutil rdflib
```

### With pip

```bash
python -m venv .venv
# Windows: .venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate
pip install "oc_graphenricher>=2.1.0" oc_ocdm polars psutil rdflib
```

## Quick start

Process just the first CSV in the archive, stopping after 20 entities —
useful as a smoke test before a full run:

```bash
uv run python enricher_support.py --csv_path path/to/dump.tar.gz --num-csv 1 --test-limit 20
```

A full run over the first 5 CSV files, flushing every 10:

```bash
uv run python enricher_support.py \
  --csv_path path/to/dump.tar.gz \
  --num-csv 5 \
  --batch-size 10
```

## Configuration reference

### Input / batching

| Flag | Default | Meaning |
|---|---|---|
| `--csv_path` | *(required)* | Path to the `.tar.gz` archive |
| `--base-iri` | `https://w3id.org/oc/meta/` | Base IRI for created resources |
| `--num-csv` | `1` | How many CSV files (inside the archive) to process |
| `--test-limit` | none | Stop after N entities total (useful for quick tests) |
| `--batch-size` | `10` | Flush (enrich + save + reset) every N processed CSVs |
| `--max-retries` | `2` | Retry attempts on network timeouts during enrichment |
| `--retry-delay` | `30` | Seconds to wait between retries |

### Output location

| Flag | Default | Meaning |
|---|---|---|
| `--output-dir` | `output` | Where enriched graphs and provenance are saved |
| `--info-dir` | `<output-dir>/info` | Persistent counters shared across batches — **do not delete between runs of the same job**, or entity numbering will collide |

### Storage strategy

| Flag | Default | Meaning |
|---|---|---|
| `--storage-type` | `single-file` | `single-file`: one JSON pair per batch. `directory`: OCDM bucketed layout |
| `--supplier-prefix` | none | Supplier prefix for entities whose IRI doesn't already carry one |
| `--output-format` | none | e.g. `json-ld` (passed through if set) |
| `--zip-output` / `--no-zip-output` | library default | Force zipping the output files on/off |
| `--items-per-directory` | `10000` | Directory bucket size (`directory` storage only) |
| `--items-per-file` | `1000` | File bucket size (`directory` storage only) |
| `--wanted-label` | none | Label passed through to provenance |
| `--checkpoint-interval` | none | Also write the graph every N processed BRs, independent of batching |

### Enrichment sources

| Flag | Default | Meaning |
|---|---|---|
| `--use-wikidata` / `--no-use-wikidata` | on | Query Wikidata for a matching item |
| `--use-viaf` / `--no-use-viaf` | on | Query VIAF for author identifiers |
| `--use-orcid` / `--no-use-orcid` | on | Query ORCID for author identifiers |

> Crossref and OpenAlex are always queried — the underlying library doesn't
> expose a switch to disable them.

### Debugging

| Flag | Meaning |
|---|---|
| `--debug` | Print verbose logs, including every network call made during enrichment |
| `--clear-cache` | Delete the `GraphEnricher_cache.sqlite*` cache before running, forcing fresh network requests |

The enrichment step caches every HTTP response indefinitely
(`GraphEnricher_cache.sqlite`, created next to the script). This speeds up
repeated runs over the same data, but means a re-run won't re-query a
source it already asked. Use `--clear-cache` whenever you want to be sure
you're seeing live results.

## Output

- `output/enriched/000N_<label>.json[.zip]` and
  `output/provenance/000N_<label>.json[.zip]` — with `--storage-type
  single-file` (default)
- `output/<type>/<prefix>/<bucket>/...` — with `--storage-type directory`
- `output/info/` — persistent entity/provenance counters (keep across runs
  of the same job)
- `processed_files.json` — tracks which CSV files have already been
  processed, so you can safely re-run the same command to resume

## Example: full custom run

```bash
uv run python enricher_support.py \
  --csv_path dump.tar.gz \
  --num-csv 50 \
  --batch-size 10 \
  --storage-type directory \
  --supplier-prefix 060 \
  --use-wikidata --no-use-viaf --no-use-orcid \
  --max-retries 3 --retry-delay 60
```