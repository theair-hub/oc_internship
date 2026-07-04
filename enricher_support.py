# SPDX-FileCopyrightText: 2026 Ilaria De Dominicis <ilaria.dedominicis2@studio.unibo.it>
#
# SPDX-License-Identifier: ISC

import tarfile

from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic import BibliographicResource
from rdflib import URIRef
import io

import logging
import os
import psutil
import time
import json
import polars as pl
from oc_graphenricher.enricher import GraphEnricher
from oc_graphenricher.storage import single_file_storage, directory_storage


class EnricherSupport:
    """
    Utility class to:
    - read CSV files
    - create Bibliographic Resources (BRs)
    - enrich RDF graphs
    - save enriched and incomplete graphs

    Both *which external sources are used to enrich* (Wikidata/VIAF/ORCID) and
    *how the result is stored* (single-file vs OCDM directory layout, format,
    zipping, ...) are fully configurable through the constructor / CLI, rather
    than being hardcoded.
    """

    def __init__(
        self,
        csv_zip_path: str,
        base_iri: str,
        *,
        graph_set: GraphSet | None = None,
        # --- where things are saved ---
        output_dir: str = "output",
        info_dir: str | None = None,
        # --- storage strategy ---
        storage_type: str = "single-file",  # "single-file" or "directory"
        supplier_prefix: str | None = None,
        wanted_label: str | None = None,
        output_format: str | None = None,
        zip_output: bool | None = None,
        items_per_directory: int = 10000,
        items_per_file: int = 1000,
        # --- which external sources to query during enrichment ---
        use_wikidata: bool = True,
        use_viaf: bool = True,
        use_orcid: bool = True,
        checkpoint_interval: int | None = None,
        debug: bool = False,
    ):
        if not csv_zip_path:
            raise ValueError("csv_zip_path is required")

        if not base_iri:
            raise ValueError("base_iri is required")

        if storage_type not in ("single-file", "directory"):
            raise ValueError('storage_type must be "single-file" or "directory"')

        self.csv_zip_path = csv_zip_path
        self.base_iri = base_iri
        self.missing_data: list[str] = []
        self.created_br: int = 0
        self.total_created_br: int = 0
        self.resp_agent = "https://orcid.org/0009-0008-2026-5889" # to specify name or id, like ORCID

        self.output_dir = output_dir
        self.storage_type = storage_type
        self.supplier_prefix = supplier_prefix
        self.wanted_label = wanted_label
        self.output_format = output_format
        self.zip_output = zip_output
        self.items_per_directory = items_per_directory
        self.items_per_file = items_per_file

        self.use_wikidata = use_wikidata
        self.use_viaf = use_viaf
        self.use_orcid = use_orcid
        self.checkpoint_interval = checkpoint_interval
        self.debug = debug

        self._batch_counter = 0

        if self.storage_type == "single-file":
            os.makedirs(os.path.join(self.output_dir, "enriched"), exist_ok=True)
            os.makedirs(os.path.join(self.output_dir, "provenance"), exist_ok=True)
        else:
            os.makedirs(self.output_dir, exist_ok=True)

        # IMPORTANT: this must be a single, persistent directory shared across
        # every batch. Entities created via g_set.add_id(...) don't get an
        # explicit URI (unlike BRs, which use the omid), so their numbering
        # relies entirely on the counters oc_ocdm keeps in info_dir. If this
        # directory is not reused between batches, different batches will
        # reassign the same identifier URIs (e.g. id/1) to different entities.
        self.info_dir = info_dir or os.path.join(self.output_dir, "info")
        os.makedirs(self.info_dir, exist_ok=True)

        self.g_set = graph_set or GraphSet(base_iri=base_iri, info_dir=self.info_dir)

    def load_processed_files(self):
        self.processed_files = set()
        if os.path.exists("processed_files.json"):
            with open("processed_files.json", "r", encoding="utf-8") as f:
                self.processed_files = set(json.load(f))

    def save_processed_files_batch(self, batch_files: list[str]):
        for f in batch_files:
            self.processed_files.add(f)
        with open("processed_files.json", "w", encoding="utf-8") as f:
            json.dump(list(self.processed_files), f, indent=2, ensure_ascii=False)

    def resources_ok(self, max_ram_percent=95):
        ram = psutil.virtual_memory().percent
        print(f"RAM: {ram}%")
        return ram <= max_ram_percent

    def _flush_batch(
        self,
        label: str = "batch",
        max_retries: int = 5,
        retry_delay: int = 30,
    ):
        print(f"\n--- Enriching {label} ({self.created_br} BR) ---")

        self.enrich(
            label=label,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )

        self.total_created_br += self.created_br
        self.g_set = GraphSet(base_iri=self.base_iri, info_dir=self.info_dir)
        self.created_br = 0

        print(f"--- GraphSet reset (total so far: {self.total_created_br} BR) ---\n")

    def process_folder_streaming(
        self,
        test_limit: int | None = None,
        num_csv: int | None = None,
        batch_size: int = 10,
        max_retries: int = 5,
        retry_delay: int = 30,
    ):
        self.load_processed_files()

        counter = 0
        csv_count = 0

        # processes the OC Meta dump in tar.gz format, which contains multiple CSV files

        with tarfile.open(self.csv_zip_path, "r:gz") as tar:

            files_to_process = [
                m for m in tar.getmembers()
                if m.isfile() and m.name.lower().endswith(".csv")
            ]

            files_to_process = [
                m for m in files_to_process
                if os.path.basename(m.name) not in self.processed_files
            ]

            # if the use specified a limit on the number of CSV files to process, slice the list accordingly

            if num_csv:
                files_to_process = files_to_process[:num_csv]

            for member in files_to_process:

                file_name = os.path.basename(member.name)

                if file_name in self.processed_files:
                    print(f"Skipped (already processed): {file_name}")
                    continue

                print(f"Processing: {file_name}")
                csv_count += 1

                try:
                    extracted = tar.extractfile(member)

                    if extracted is None:
                        print(f"Could not extract: {file_name}")
                        continue

                    df = pl.read_csv(
                        io.BytesIO(extracted.read()),
                        columns=["id", "title"],
                        schema_overrides={"id": pl.Utf8, "title": pl.Utf8},
                        infer_schema=False,
                        null_values=["", "N/A"],
                    )

                    for row_index, row in enumerate(df.iter_rows(named=True), start=1):

                        if row_index % 500 == 0 and not self.resources_ok():
                            raise RuntimeError(f"System resources too high during {file_name}")

                        ids_field = row.get("id")
                        title_field = row.get("title")

                        if not ids_field:
                            continue

                        identifiers = ids_field.split()
                        omid = None
                        others = []

                        for identifier in identifiers:
                            if identifier.startswith("omid:"):
                                omid = identifier.removeprefix("omid:")
                            else:
                                others.append(identifier)

                        if not omid:
                            continue

                        try:
                            self.create_br_from_omid(omid, others, title_field)
                        except Exception as e:
                            self.missing_data.append((omid, str(e)))

                        if test_limit:
                            counter += 1
                            if counter >= test_limit:
                                print(f"\n--- TEST COMPLETED ({counter} entities) ---")
                                if self.created_br > 0:
                                    self._flush_batch(
                                        label=f"test ({counter} entities)",
                                        max_retries=max_retries,
                                        retry_delay=retry_delay,
                                    )
                                return

                    print(f"Completed: {file_name} ({self.created_br} BR in current batch)")
                    self.save_processed_files_batch([file_name])

                    if csv_count % batch_size == 0:
                        self._flush_batch(
                            label=f"CSV #{csv_count}",
                            max_retries=max_retries,
                            retry_delay=retry_delay,
                        )

                except RuntimeError as e:
                    print(f"Controlled interruption: {e}")
                    return

                except Exception as e:
                    print(f"Error reading {file_name}: {e}")

        if self.created_br > 0:
            self._flush_batch(
                label="final",
                max_retries=max_retries,
                retry_delay=retry_delay,
            )

        print(f"\nProcessed {csv_count} CSVs, {self.total_created_br} BR created in total.")
        print(f"Missing/error BRs or identifiers: {len(self.missing_data)}")

    def create_br_from_omid(
        self,
        omid: str,
        others: list[str],
        title: str | None = None,
    ) -> BibliographicResource | None:

        schemas = {
            identifier.split(":", 1)[0]
            for identifier in others
            if ":" in identifier
        }

        if {"doi", "issn", "openalex"}.issubset(schemas):
            return None

        br_uri = URIRef(f"{self.base_iri}{omid}")
        br = self.g_set.add_br(resp_agent=self.resp_agent, res=br_uri)

        schema_map_factories = {
            "doi": "create_doi",
            "issn": "create_issn",
            "isbn": "create_isbn",
            "pmid": "create_pmid",
            "pmcid": "create_pmcid",
            "openalex": "create_openalex",
            "url": "create_url",
        }

        for identifier in others:
            try:
                schema, literal = identifier.split(":", 1)

                if schema not in schema_map_factories:
                    self.missing_data.append((identifier, f"Unknown schema: {schema}"))
                    continue

                id_obj = self.g_set.add_id(resp_agent=self.resp_agent)
                getattr(id_obj, schema_map_factories[schema])(literal)
                br.has_identifier(id_obj)

            except Exception as e:
                self.missing_data.append((identifier, str(e)))

        if title and title.strip().lower() not in ("", "unknown"):
            br.has_title(title)

        self.created_br += 1
        return br

    def _build_storage(self, label: str):
        """
        Build the storage object for the current batch, honoring
        self.storage_type and every storage-related option set by the user.
        """
        common_kwargs = {"info_dir": self.info_dir}
        if self.supplier_prefix is not None:
            common_kwargs["supplier_prefix"] = self.supplier_prefix
        if self.wanted_label is not None:
            common_kwargs["wanted_label"] = self.wanted_label
        if self.output_format is not None:
            common_kwargs["output_format"] = self.output_format
        if self.zip_output is not None:
            common_kwargs["zip_output"] = self.zip_output

        if self.storage_type == "directory":
            # All batches share the same output_dir: the directory layout
            # buckets entities by their own numeric IRI, not by batch, so
            # calling this repeatedly across batches is safe.
            return directory_storage(
                output_dir=self.output_dir,
                items_per_directory=self.items_per_directory,
                items_per_file=self.items_per_file,
                **common_kwargs,
            )

        # single-file: one pair of files per batch, so batches don't overwrite
        # each other.
        self._batch_counter += 1
        safe_label = "".join(c if c.isalnum() else "_" for c in label).strip("_") or "batch"
        graph_path = os.path.join(
            self.output_dir, "enriched", f"{self._batch_counter:04d}_{safe_label}.json"
        )
        provenance_path = os.path.join(
            self.output_dir, "provenance", f"{self._batch_counter:04d}_{safe_label}.json"
        )
        return single_file_storage(
            graph_path=graph_path,
            provenance_path=provenance_path,
            **common_kwargs,
        )

    def enrich(
        self,
        label: str = "batch",
        max_retries: int = 5,
        retry_delay: int = 30,
    ):
        storage = self._build_storage(label)

        enricher_kwargs = {
            "graph_set": self.g_set,
            "storage": storage,
            "use_wikidata": self.use_wikidata,
            "use_viaf": self.use_viaf,
            "use_orcid": self.use_orcid,
            "debug": self.debug,
        }
        if self.checkpoint_interval is not None:
            enricher_kwargs["checkpoint_interval"] = self.checkpoint_interval

        enricher = GraphEnricher(**enricher_kwargs)

        for attempt in range(1, max_retries + 1):
            try:
                enricher.enrich()
                print("Enrichment completed.")
                break
            except Exception as e:
                is_timeout = (
                    "ReadTimeoutError" in type(e).__name__
                    or "TimeoutError" in str(e)
                    or "timed out" in str(e).lower()
                )
                if is_timeout and attempt < max_retries:
                    print(f"Timeout (attempt {attempt}/{max_retries}). Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                elif is_timeout:
                    print(f"Timeout after {max_retries} attempts. Proceeding with partial data.")
                else:
                    raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bibliographic enrichment with OpenCitations")

    # --- input / batching ---
    parser.add_argument("--csv_path", required=True, help="Path to the .tar.gz archive")
    parser.add_argument("--base-iri", default="https://w3id.org/oc/meta/")
    parser.add_argument("--num-csv", type=int, default=1)
    parser.add_argument("--test-limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--retry-delay", type=int, default=30)

    # --- where things are saved ---
    parser.add_argument("--output-dir", default="output", help="Where enriched/provenance files are saved")
    parser.add_argument(
        "--info-dir",
        default=None,
        help="Persistent counters directory shared across batches (default: <output-dir>/info)",
    )

    # --- storage strategy ---
    parser.add_argument(
        "--storage-type",
        choices=["single-file", "directory"],
        default="single-file",
        help="single-file: one JSON pair per batch. directory: OCDM bucketed layout under --output-dir.",
    )
    parser.add_argument(
        "--supplier-prefix",
        default=None,
        help="Supplier prefix for entities whose IRI doesn't already contain one (directory storage).",
    )
    parser.add_argument("--wanted-label", default=None, help="Label passed through to ProvSet.")
    parser.add_argument("--output-format", default=None, help='e.g. "json-ld" (passed through if set).')
    parser.add_argument(
        "--zip-output",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Zip the produced graph/provenance files. Omit to use the library default.",
    )
    parser.add_argument("--items-per-directory", type=int, default=10000, help="Only used with --storage-type directory.")
    parser.add_argument("--items-per-file", type=int, default=1000, help="Only used with --storage-type directory.")
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=None,
        help="Write the graph every N processed BRs, in addition to the per-batch flush.",
    )

    # --- which external sources to use ---
    parser.add_argument("--use-wikidata", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--use-viaf", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--use-orcid", action=argparse.BooleanOptionalAction, default=True)

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose logging from oc_graphenricher (e.g. every identifier lookup attempt).",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Delete GraphEnricher_cache.sqlite* before running, forcing fresh network calls.",
    )

    args = parser.parse_args()

    if args.clear_cache:
        import glob
        for cache_file in glob.glob("GraphEnricher_cache.sqlite*"):
            os.remove(cache_file)
            print(f"Removed stale cache file: {cache_file}")

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format="%(name)s - %(levelname)s - %(message)s")

    enricher = EnricherSupport(
        csv_zip_path=args.csv_path,
        base_iri=args.base_iri,
        output_dir=args.output_dir,
        info_dir=args.info_dir,
        storage_type=args.storage_type,
        supplier_prefix=args.supplier_prefix,
        wanted_label=args.wanted_label,
        output_format=args.output_format,
        zip_output=args.zip_output,
        items_per_directory=args.items_per_directory,
        items_per_file=args.items_per_file,
        use_wikidata=args.use_wikidata,
        use_viaf=args.use_viaf,
        use_orcid=args.use_orcid,
        checkpoint_interval=args.checkpoint_interval,
        debug=args.debug,
    )

    enricher.process_folder_streaming(
        num_csv=args.num_csv,
        test_limit=args.test_limit,
        batch_size=args.batch_size,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay,
    )

    print(f"\nTotal BRs created: {enricher.total_created_br}")
    print(f"Missing/error BRs or identifiers: {len(enricher.missing_data)}")
    if enricher.missing_data:
        print(enricher.missing_data[:5])