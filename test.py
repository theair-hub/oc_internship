from enricher_support import EnricherSupport
import argparse

parser = argparse.ArgumentParser(description="Arricchimento bibliografico con OpenCitations")

parser.add_argument("--csv_path", default=r"C:\Users\ilari\Desktop\VS_CODE\OpenCitations\output_csv_2026_01_14")
parser.add_argument("--base-iri", default="https://w3id.org/oc/meta/")
parser.add_argument("--num-csv", type=int, default=13)
parser.add_argument("--test-limit", type=int, default=None)
parser.add_argument("--batch-size", type=int, default=5)
parser.add_argument("--max-retries", type=int, default=5)
parser.add_argument("--retry-delay", type=int, default=30)

args = parser.parse_args()

enricher = EnricherSupport(csv_zip_path=args.csv_path, base_iri=args.base_iri)

enricher.process_folder_streaming(
    num_csv=args.num_csv,
    test_limit=args.test_limit,
    batch_size=args.batch_size,
    max_retries=args.max_retries, 
    retry_delay=args.retry_delay, 
)

print(f"\nTotale BR creati: {enricher.total_created_br}")
print(f"BR/ID mancanti o con errori: {len(enricher.missing_data)}")
if enricher.missing_data:
    print(enricher.missing_data[:5])

# note: 2 hours for 500 csv. 
# 2h10m19s×80
# ≈ 173 ore e 45 minuti
# ≈ 7 giorni e 6 ore continuativi.


# [GraphEnricher-OpenAlex]:KeyError('results')__https://api.openalex.org/works?filter=doi:10.1109/itherm40304.2017&select=id__application/json               
# [GraphEnricher-OpenAlex]:KeyError('results')__https://api.openalex.org/works?filter=doi:10.1109/isie38194.2017&select=id__application/json                 
# [GraphEnricher-OpenAlex]:KeyError('results')__https://api.openalex.org/works?filter=doi:10.1109/sibcon39200.2017&select=id__application/json               
# [GraphEnricher-OpenAlex]:KeyError('results')__https://api.openalex.org/works?filter=doi:10.1109/isse41842.2017&select=id__application/json  
