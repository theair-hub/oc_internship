import tarfile
import shutil

from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic import BibliographicResource
from rdflib import URIRef
import io

import sys
import os
import psutil
import time
import json
import polars as pl
from datetime import datetime

# Add local package path
sys.path.insert(
    0,
    r"C:\Users\ilari\Desktop\VS_CODE\OpenCitations\oc_graphenricher"
)

from oc_graphenricher.enricher import GraphEnricher


class EnricherSupport:
    """
    Utility class to:
    - read CSV files
    - create Bibliographic Resources (BRs)
    - enrich RDF graphs
    - save enriched and incomplete graphs
    """

    def __init__(
        self,
        csv_zip_path: str,
        base_iri: str,
        *,
        graph_set: GraphSet | None = None,
    ):
        if not csv_zip_path:
            raise ValueError("csv_zip_path is required")

        if not base_iri:
            raise ValueError("base_iri is required")

        self.csv_zip_path = csv_zip_path
        self.base_iri = base_iri
        self.missing_data: list[str] = []
        self.created_br: int = 0
        self.total_created_br: int = 0
        self.g_set = graph_set or GraphSet(base_iri=base_iri)
        self.resp_agent = "https://orcid.org/0009-0008-2026-5889"

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
        incomplete_file: str,
        label: str = "batch",
        max_retries: int = 5,
        retry_delay: int = 30,
    ):
        print(f"\n--- Enriching {label} ({self.created_br} BR) ---")

        self.enrich(
            incomplete_file=incomplete_file,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )

        self.total_created_br += self.created_br
        self.g_set = GraphSet(base_iri=self.base_iri)
        self.created_br = 0

        print(f"--- GraphSet reset (total so far: {self.total_created_br} BR) ---\n")

    def process_folder_streaming(
        self,
        test_limit: int | None = None,
        num_csv: int | None = None,
        batch_size: int = 10,
        incomplete_file: str = "incomplete.nt",
        max_retries: int = 5,
        retry_delay: int = 30,
    ):
        self.load_processed_files()

        counter = 0
        csv_count = 0

        with tarfile.open(self.csv_zip_path, "r:gz") as tar:

            files_to_process = [
                m for m in tar.getmembers()
                if m.isfile() and m.name.lower().endswith(".csv")
            ]

            if not files_to_process:
                print("No CSV files found in archive.")
                return

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
                                        incomplete_file,
                                        label=f"test ({counter} entities)",
                                        max_retries=max_retries,
                                        retry_delay=retry_delay,
                                    )
                                return

                    print(f"Completed: {file_name} ({self.created_br} BR in current batch)")
                    self.save_processed_files_batch([file_name])

                    if csv_count % batch_size == 0:
                        self._flush_batch(
                            incomplete_file,
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
                incomplete_file,
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

    def enrich(
        self,
        incomplete_file: str = "incomplete.nt",
        max_retries: int = 5,
        retry_delay: int = 30,
    ):
        os.makedirs(os.path.join(os.getcwd(), "info_dir"), exist_ok=True)
        os.makedirs(os.path.join(os.getcwd(), "enriched"), exist_ok=True)
        os.makedirs(os.path.join(os.getcwd(), "provenance"), exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

        timestamped_enriched = os.path.join("enriched", f"enriched_{timestamp}.jsonld")
        timestamped_provenance = os.path.join("provenance", f"provenance_{timestamp}.nq")
        temp_incomplete = f"incomplete_temp_{timestamp}.nt"  # ← root, non in enriched/
        
        enricher = GraphEnricher(
            self.g_set,
            graph_filename=timestamped_enriched,
            provenance_filename=timestamped_provenance,
            incomplete_filename=temp_incomplete,
            info_dir=os.path.join(os.getcwd(), "info_dir"),
            use_wikidata=False,
            use_viaf=False,
            use_orcid=False
        )

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

        # Appendi le BR incomplete al file principale (append-safe in formato nt)
        if os.path.exists(temp_incomplete):
            with open(temp_incomplete, "r", encoding="utf-8") as src:
                with open(incomplete_file, "a", encoding="utf-8") as dst:
                    shutil.copyfileobj(src, dst)
            os.remove(temp_incomplete)
