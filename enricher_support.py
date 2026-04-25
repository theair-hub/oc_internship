from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic import BibliographicResource
from oc_ocdm.graph.entities.identifier import Identifier
from oc_graphenricher.enricher import GraphEnricher
from rdflib import URIRef, Graph
from urllib.parse import quote
import os, csv, io
import psutil, time, zipfile
import json

class EnricherSupport:

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
        self.g_set = graph_set or GraphSet(base_iri=base_iri)

    def load_processed_files(self):
        self.processed_files = set()
        if os.path.exists("processed_files.json"):
            with open("processed_files.json", "r", encoding="utf-8") as f:
                self.processed_files = set(json.load(f))

    def save_processed_file(self, file_name):
        self.processed_files.add(file_name)
        with open("processed_files.json", "w", encoding="utf-8") as f:
            json.dump(list(self.processed_files), f, indent=2, ensure_ascii=False)

    def resources_ok(self, max_ram_percent=85, max_cpu_percent=95):
        ram = psutil.virtual_memory().percent
        cpu = psutil.cpu_percent(interval=0.5)
        print(f"RAM: {ram}% | CPU: {cpu}%")
        if ram > max_ram_percent:
            return False
        if cpu > max_cpu_percent: # cpu posso anche toglierla ma ok
            return False
        return True

    def process_folder_streaming(
        self,
        test_limit: int | None = None,
        num_csv: int | None = None,
        batch_size: int = 10
    ):
        self.load_processed_files()
        counter = 0
        csv_count = 0
        batch_files = []  # tiene traccia dei file del batch corrente

        files_to_process = []
        for root, dirs, files in os.walk(self.csv_zip_path):
            for file_name in files:
                if file_name.lower().endswith(".csv"):
                    files_to_process.append(os.path.join(root, file_name))

        if not files_to_process:
            print("Nessun file CSV trovato.")
            return

        if num_csv:
            files_to_process = files_to_process[:num_csv]

        for csv_path in files_to_process:
            file_name = os.path.basename(csv_path)

            if file_name in self.processed_files:
                print(f"Saltato (già processato): {file_name}")
                continue

            print(f"Processing: {file_name}")
            csv_count += 1
            batch_files.append(file_name) # batch per memoria...

            try:
                with open(csv_path, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)

                    for row_index, row in enumerate(reader, start=1):
                        if row_index % 500 == 0 and not self.resources_ok():
                            raise RuntimeError(
                                f"Risorse sistema troppo alte durante {file_name}"
                            )

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

                        if omid:
                            try:
                                self.create_br_from_omid(omid, others, title_field)
                            except Exception as e:
                                self.missing_data.append((omid, str(e)))

                            if test_limit:
                                counter += 1
                                if counter >= test_limit:
                                    print(f"\n--- TEST COMPLETATO ({counter} entità) ---")
                                    return

                print(f"Completato: {file_name}")

                # enrich + reset ogni batch_size CSV
                if csv_count % batch_size == 0:
                    print(f"\n--- Arricchimento batch (CSV #{csv_count}) ---")
                    self.enrich(
                        enriched_file=f"enriched_{csv_count}.ttl",
                        incomplete_file=f"incomplete_{csv_count}.ttl"
                    )
                    # salva i file processati SOLO dopo l'enrich riuscito
                    for f in batch_files:
                        self.save_processed_file(f)
                    batch_files = []  # reset lista batch
                    self.g_set = GraphSet(base_iri=self.base_iri)
                    self.created_br = 0
                    print(f"--- Reset GraphSet, memoria liberata ---\n")

            except RuntimeError as e:
                print(f"Interruzione controllata: {e}")
                return
            except Exception as e:
                print(f"Errore leggendo {file_name}: {e}")

        # enrich finale per i CSV rimasti
        if self.created_br > 0:
            print(f"\n--- Arricchimento batch finale ---")
            self.enrich(
                enriched_file=f"enriched_final.ttl",
                incomplete_file=f"incomplete_final.ttl"
            )
            for f in batch_files:
                self.save_processed_file(f)

        print(f"\nProcessati {csv_count} CSV, {self.created_br} BR create.")

    def create_br_from_omid(
        self,
        omid: str,
        others: list[str],
        title: str | None = None
    ) -> BibliographicResource:

        br_uri = URIRef(f"{self.base_iri}{omid}")

        br = self.g_set.add_br(
            resp_agent="EnricherSupport",
            res=br_uri
        )

        for identifier in others:
            try:
                schema, literal = identifier.split(":", 1)

                # GraphSet dovrebbe generare uri corretto automaticamente
                id_obj = self.g_set.add_id(resp_agent="EnricherSupport")

                schema_map = {
                    "doi":      id_obj.create_doi,
                    "issn":     id_obj.create_issn,
                    "isbn":     id_obj.create_isbn,
                    "pmid":     id_obj.create_pmid,
                    "pmcid":    id_obj.create_pmcid,
                    "openalex": id_obj.create_openalex,
                    "wikidata": id_obj.create_wikidata,
                    "url":      id_obj.create_url,
                }

                if schema in schema_map:
                    schema_map[schema](literal)
                    br.has_identifier(id_obj)
                else:
                    self.missing_data.append((identifier, f"Schema sconosciuto: {schema}"))

            except Exception as e:
                self.missing_data.append((identifier, str(e)))

        if title:
            br.has_title(title)

        self.created_br += 1
        return br

    def enrich(
        self,
        enriched_file="enriched.ttl",
        incomplete_file="incomplete.ttl",
        max_retries: int = 5,
        retry_delay: int = 30
    ):
        for br in self.g_set.get_br():
            title = br.get_title()
            if not title or title.strip() == "":
                br.has_title("Untitled")

        enricher = GraphEnricher(self.g_set)

        for attempt in range(1, max_retries + 1):
            try:
                enricher.enrich()
                print("Arricchimento completato.")
                break
            except Exception as e:
                if "ReadTimeoutError" in type(e).__name__ or "TimeoutError" in str(e) or "timed out" in str(e).lower():
                    if attempt < max_retries:
                        print(f"Timeout (tentativo {attempt}/{max_retries}). Riprovo tra {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        print(f"Timeout dopo {max_retries} tentativi. Procedo con dati parziali.")
                else:
                    raise

        merged_graph = Graph()
        incomplete_graph = Graph()

        for g in self.g_set.graphs():
            merged_graph += g

        for br in self.g_set.get_br():
            has_doi = False
            has_issn = False
            has_wikidata = False
            has_openalex = False

            for identifier in br.get_identifiers():
                scheme_str = str(identifier.get_scheme()).lower()
                if "doi" in scheme_str:
                    has_doi = True
                elif "issn" in scheme_str:
                    has_issn = True
                elif "wikidata" in scheme_str:
                    has_wikidata = True
                elif "openalex" in scheme_str:
                    has_openalex = True

            if not (has_doi and has_issn and has_wikidata and has_openalex):
                br_uri = br.res
                for g in self.g_set.graphs():
                    for triple in g.triples((br_uri, None, None)):
                        incomplete_graph.add(triple)
                    for triple in g.triples((None, None, br_uri)):
                        incomplete_graph.add(triple)

        merged_graph.serialize(enriched_file, format="turtle")
        print(f"Grafo arricchito salvato in: {enriched_file}")

        if len(incomplete_graph) > 0:
            incomplete_graph.serialize(incomplete_file, format="turtle")
            print(f"BR incomplete salvate in: {incomplete_file}")

        return self.g_set
