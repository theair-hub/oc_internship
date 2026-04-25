from enricher_support import EnricherSupport
import argparse

# Peroni ha detto di usare argparse 
parser = argparse.ArgumentParser(description="Arricchimento bibliografico con OpenCitations")

parser.add_argument("--csv_path", default=r"C:\\Users\\ilari\\Desktop\\OpenCitations\\output_csv_2026_01_14")
parser.add_argument("--base-iri", default="https://w3id.org/oc/meta/")
parser.add_argument("--num-csv", type=int, default=31)
parser.add_argument("--test-limit", type=int, default=None)
parser.add_argument("--batch-size", type=int, default=5)
parser.add_argument("--max-retries", type=int, default=5)
parser.add_argument("--retry-delay", type=int, default=30)

args = parser.parse_args()

enricher = EnricherSupport(csv_zip_path=args.csv_path, base_iri=args.base_iri)

enricher.process_folder_streaming(
    num_csv=args.num_csv,
    test_limit=args.test_limit,
    batch_size=args.batch_size
)

print(f"\nTotale BR creati: {enricher.created_br}")
print(f"BR/ID mancanti o con errori: {len(enricher.missing_data)}")
if enricher.missing_data:
    print(enricher.missing_data[:5])

# errore di Wikidata: troppe richieste [GraphEnricher-WikiData]:JSONDecodeError('Expecting value: line 1 column 1 (char 0)')__
"""         SELECT ?item WHERE {
              ?item p:P356 ?x.
              ?x ps:P356 "10.22435/BPK.V46I1.48".
        } LIMIT 1
        __text/html; charset=utf-8                                                     
[GraphEnricher-WikiData]:JSONDecodeError('Expecting value: line 1 column 1 (char 0)')__
        SELECT ?item WHERE {
              ?item p:P356 ?x.
              ?x ps:P356 "10.14238/PI56.5.2016.304-10".
        } LIMIT 1
        __text/html; charset=utf-8                                                     
[GraphEnricher-WikiData]:JSONDecodeError('Expecting value: line 1 column 1 (char 0)')__
        SELECT ?item WHERE {
              ?item p:P356 ?x.
              ?x ps:P356 "10.22435/HSJI.V12I2.2239".
        } LIMIT 1
        __text/html; charset=utf-8                                                     
[GraphEnricher-WikiData]:JSONDecodeError('Expecting value: line 1 column 1 (char 0)')__
 """
