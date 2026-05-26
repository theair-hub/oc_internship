# Graph Enrichment Pipeline

Il sistema è composto da due classi principali che lavorano in sequenza: `EnricherSupport` gestisce la lettura dei dati e l'orchestrazione del processo, mentre `GraphEnricher` si occupa dell'arricchimento vero e proprio delle entità RDF.

---

## EnricherSupport

`EnricherSupport` è il punto di ingresso del processo. Prende in input un archivio `.tar.gz` contenente file CSV con dati bibliografici di OpenCitations (OMID, identificatori, titolo) e li trasforma in un grafo RDF pronto per l'arricchimento.

### Flusso principale: `process_folder_streaming`

Il metodo apre l'archivio una sola volta e itera i CSV in streaming, senza estrarre tutto su disco. Per ogni riga:

1. Estrae l'OMID e gli altri identificatori (DOI, ISSN, OpenAlex, ecc.)
2. Salta le righe già complete (che hanno già DOI + ISSN + OpenAlex)
3. Chiama `create_br_from_omid` per creare la `BibliographicResource` nel `GraphSet`

I file già processati vengono tracciati in `processed_files.json`, così un run interrotto può riprendere da dove si era fermato.

### Batch processing

Per evitare di esaurire la RAM con dataset grandi, il grafo viene arricchito e svuotato ogni `batch_size` CSV tramite `_flush_batch`. Dopo ogni flush il `GraphSet` viene ricreato da zero. Un contatore `total_created_br` tiene traccia del totale complessivo.

### `create_br_from_omid`

Crea una `BibliographicResource` nel `GraphSet` a partire dall'OMID, collegandovi tutti gli identificatori disponibili tramite i metodi factory di `oc_ocdm` (`create_doi`, `create_issn`, ecc.). Se il titolo è presente e non è "unknown", viene aggiunto alla BR. Le entità vengono create con `resp_agent = "https://orcid.org/0009-0008-2026-5889"`.

### `enrich`

Istanzia un `GraphEnricher` e chiama il suo metodo `enrich()` con gestione automatica dei timeout (fino a `max_retries` tentativi con pausa `retry_delay`). I nomi dei file di output vengono resi univoci con un timestamp al microsecondo per evitare sovrascritture tra batch.

---

## GraphEnricher

`GraphEnricher` riceve un `GraphSet` già popolato e lo arricchisce interrogando API esterne per trovare identificatori mancanti.

### Flusso di arricchimento: `enrich`

Itera ogni `BibliographicResource` nel grafo (saltando issue e volumi di riviste) e per ciascuna:

**Arricchimento della BR:**
- Se ha un ISSN, interroga Crossref per trovare ISSN aggiuntivi
- Se non ha un DOI ma ha un titolo valido, interroga Crossref per trovarlo
- Se non ha un ID Wikidata, lo cerca tramite DOI, ISSN, PMID o PMCID
- Se non ha un ID OpenAlex, lo cerca tramite tutti gli identificatori disponibili

**Arricchimento degli autori (AR con ruolo `iri_author`):**
- Se manca l'ORCID, lo cerca tramite le API ORCID
- Se manca il VIAF, lo cerca tramite le API VIAF
- Se manca il Wikidata ID, lo cerca tramite gli altri identificatori trovati

**Arricchimento dei publisher (AR con ruolo `iri_publisher`):**
- Se manca il Crossref ID, lo cerca tramite il DOI della BR

Tutti i nuovi identificatori trovati vengono aggiunti al grafo tramite `_add_id`, che verifica prima che non siano già presenti.

### Salvataggio degli output

Al termine dell'iterazione, `GraphEnricher` produce tre output:

- **`enriched_<timestamp>.jsonld`** — il grafo arricchito in formato JSON-LD
- **`provenance_<timestamp>.nq`** — la provenance in formato N-Quads, generata da `ProvSet` che traccia chi ha aggiunto cosa e quando
- **`incomplete.nt`** — le BR che al termine risultano ancora prive di almeno uno tra DOI, ISSN, Wikidata e OpenAlex; questo file viene aggiornato in append (N-Triples) senza sovrascrivere i batch precedenti

L'`info_dir` viene usata da `oc_ocdm` per tenere i contatori interni degli snapshot di provenance, garantendo URI valide e progressive per ogni entità.

---

## Struttura delle cartelle di output

```
graph/
  br/   ← BibliographicResource arricchite
  id/   ← Identifier
provenance/
  br/   ← provenance delle BR
  id/   ← provenance degli ID
incomplete.nt     ← BR incomplete (append)
info_dir/         ← contatori interni oc_ocdm
processed_files.json  ← CSV già processati
```

---

## Considerazioni

- Il sistema è progettato per essere **riprendibile**: se il processo viene interrotto, i CSV già completati non vengono riprocessati grazie a `processed_files.json`. L'unico rischio è la perdita del batch in corso al momento del crash.
- L'uso di `requests_cache` in `GraphEnricher` evita chiamate duplicate alle API esterne durante lo stesso run.
- Wikidata è disabilitato (`use_wikidata=False`) nella configurazione attuale per ridurre i tempi di esecuzione.

### Sources
- [OpenCitations Meta dump](https://download.opencitations.net/#meta)
- [OpenCitations Documentation](https://github.com/opencitations/crowdsourcing/blob/main/docs/csv_documentation-v1_1_2.pdf)
- [OC-OCDM Documentation](https://opencitations.github.io/oc_ocdm/) 
- [OC GraphEnricher GitHub](https://github.com/opencitations/oc_graphenricher/tree/main)
