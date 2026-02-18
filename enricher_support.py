from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic import BibliographicResource
from oc_ocdm.graph.entities.identifier import Identifier
from oc_graphenricher.enricher import GraphEnricher
from rdflib import URIRef, Graph
import os, csv, io
import psutil

class EnricherSupport:

    def __init__(
        self,
        csv_zip_path: str,
        base_iri: str,
        *,
        graph_set: GraphSet | None = None,
    ):
        # check sul formato
        if not csv_zip_path:
            raise ValueError("csv_zip_path is required")

        if not base_iri:
            raise ValueError("base_iri is required")

        self.csv_zip_path = csv_zip_path
        self.base_iri = base_iri

        # internal state
        self.selected_ids: list[str] = []
        self.missing_data: list[str] = []
        self.created_br: int = 0

        # dependencies
        self.g_set = graph_set or GraphSet(base_iri=base_iri)

# support functions 

    def load_processed_files(self):
        self.processed_files = set()
        if os.path.exists("processed_files.txt"):
            with open("processed_files.txt", "r", encoding="utf-8") as f:
                for line in f:
                    self.processed_files.add(line.strip())

    def save_processed_file(self, file_name):
        with open("processed_files.txt", "a", encoding="utf-8") as f:
            f.write(file_name + "\n")

    def resources_ok(self, max_ram_percent=85, max_cpu_percent=95):
        ram = psutil.virtual_memory().percent
        cpu = psutil.cpu_percent(interval=0.5)

        print(f"RAM: {ram}% | CPU: {cpu}%")

        if ram > max_ram_percent:
            return False

        if cpu > max_cpu_percent:
            return False

        return True

    def extract_ids_from_csv(
        self,
        test_limit: int | None = None,
        num_csv: int | None = None
    ):
        counter = 0

        # Trova tutti i CSV nella cartella e sottocartelle
        files_to_process = []
        for root, dirs, files in os.walk(self.csv_zip_path):
            for file_name in files:
                if file_name.lower().endswith(".csv"):
                    files_to_process.append(os.path.join(root, file_name))

        if not files_to_process:
            print("Nessun file CSV trovato.")
            return

        # Se è stato specificato num_csv, prendi solo i primi n file
        if num_csv:
            files_to_process = files_to_process[:num_csv]

        # legge CSV
        for csv_path in files_to_process:
            file_name = os.path.basename(csv_path)

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
                            result = {
                                "omid": omid,
                                "others": others,
                                "title": title_field
                            }

                            self.selected_ids.append(result)

                            if test_limit:
                                counter += 1
                                if counter >= test_limit:
                                    print("\n--- TEST COMPLETATO ---")
                                    return

            except RuntimeError as e:
                print(f"Interruzione controllata: {e}")
                return

            except Exception as e:
                print(f"Errore leggendo {csv_path}: {e}")



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
                # crea l'Identifier con URI e grafo gestiti dal GraphSet
                id_obj = self.g_set.add_id(
                    resp_agent="EnricherSupport",
                    res=URIRef(f"{self.base_iri}id/{literal}")  # URI unico
                )
                id_obj.schema = schema
                id_obj.literal = literal
                br.has_identifier(id_obj)
            except Exception as e:
                self.missing_data.append((identifier, str(e)))


        if title:
            br.has_title(title)


        self.created_br += 1
        return br


    def build_graphset(self):
        for item in self.selected_ids:
            try:
                self.create_br_from_omid(
                    item["omid"],
                    item.get("others", []),
                    item.get("title")
                )
            except Exception as e:
                self.missing_data.append((item["omid"], str(e)))


    def enrich(
        self,
        enriched_file="enriched.ttl",
        incomplete_file="incomplete.ttl"
    ):

        # Arricchimento
        enricher = GraphEnricher(self.g_set)
        enricher.enrich()
        print("Arricchimento completato.")

        merged_graph = Graph()
        incomplete_graph = Graph()

        # Unisco tutti i grafi del GraphSet
        for g in self.g_set.graphs():
            merged_graph += g

        # Controllo BR?
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

            # Se manca almeno uno la BR incompleta quindi non salva
            if not (has_doi and has_issn and has_wikidata and has_openalex):

                br_uri = br.res

                for g in self.g_set.graphs():
                    for triple in g.triples((br_uri, None, None)):
                        incomplete_graph.add(triple)

                    for triple in g.triples((None, None, br_uri)):
                        incomplete_graph.add(triple)

        # Salvataggio
        merged_graph.serialize(enriched_file, format="turtle")
        print(f"Grafo arricchito salvato in: {enriched_file}")

        if len(incomplete_graph) > 0:
            incomplete_graph.serialize(incomplete_file, format="turtle")
            print(f"BR incomplete salvate in: {incomplete_file}")

        return self.g_set



""" 
Errore :( 

BR create: 3000
New ID found: 5:   0%|                         New ID found: 5:   0%| | 13/3000 [00:07<3New ID found: 1172: 100%|██████████████████████████| 3000/3000 [53:16<00:00,  1.07s/it]
[Storer: INFO] Store the graphs into a file: starting process
[Storer: INFO] File 'enriched.rdf' added.
Traceback (most recent call last):
  File "c:\Users\ilari\Desktop\OpenCitations\test.py", line 45, in <module>
    main()
    ~~~~^^
  File "c:\Users\ilari\Desktop\OpenCitations\test.py", line 28, in main
    enricher.enrich(enriched_file="test_enriched.ttl")
    ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "c:\Users\ilari\Desktop\OpenCitations\enricher_support.py", line 196, in enrich  
    enricher.enrich()
    ~~~~~~~~~~~~~~~^^
  File "C:\Users\ilari\AppData\Roaming\Python\Python313\site-packages\oc_graphenricher\enricher\__init__.py", line 238, in enrich
    prov.generate_provenance()
    ~~~~~~~~~~~~~~~~~~~~~~~~^^
  File "C:\Users\ilari\AppData\Roaming\Python\Python313\site-packages\oc_ocdm\prov\prov_set.py", line 168, in generate_provenance
    last_snapshot_res: Optional[URIRef] = self._retrieve_last_snapshot(cur_subj.res)    
e", int(subj_count), supplier_prefix=supplier_prefix))
                                   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\ilari\AppData\Roaming\Python\Python313\site-packages\oc_ocdm\counter_handler\in_memory_counter_handler.py", line 116, in read_counter
    self.prov_counters[entity_short_name][prov_short_name] += [0] * missing_counters
                                                              ~~~~^~~~~~~~~~~~~~~~~~
MemoryError """