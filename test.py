from enricher_support import EnricherSupport

def main():
    csv_path = r"..."  
    base_iri = "https://w3id.org/oc/meta/"
    enricher = EnricherSupport(csv_zip_path=csv_path, base_iri=base_iri)

    # carica file già processati (opzionale)
    enricher.load_processed_files()

    # estrae OMID + identificatori dal CSV
    enricher.extract_ids_from_csv(num_csv=1) # numero di CSV opzionale

    # costruisce BR e Identifiers
    enricher.build_graphset()

    # arricchisce e salva 
    g_set = enricher.enrich(
        enriched_file="enriched.rdf",
        incomplete_file="incomplete.ttl"
    )

    # print
    print(f"\nTotale BR creati: {enricher.created_br}")
    print(f"BR/ID mancanti o con errori: {len(enricher.missing_data)}")
    if enricher.missing_data:
        print(enricher.missing_data[:5])

if __name__ == "__main__":
    main()
