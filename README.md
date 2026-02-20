# oc_internship

## Descrizione

1. **Legge CSV** → carica OMID e identificatori.
2. **Estrae OMID + identificatori** dai record.
3. **Crea BR (`fabio:Expression`) e Identifier (`datacite:Identifier`)**.
4. **Costruisce un `GraphSet`** con tutti i nodi.
5. **Arricchisce automaticamente** il grafo (metadata, titoli, schemi identificatori).
6. **Controlla completezza** dei BR.
7. **Salva grafo arricchito** in `enriched.rdf`.
8. **Salva BR incomplete** separatamente in `incomplete.ttl`.

## Struttura dati

BibliographicResource (fabio:Expression) – soggetti BR/br/...
├─ dcterms:title → "Titolo della pubblicazione"
├─ datacite:hasIdentifier → Identifier (id/...)
│   ├─ rdf:type → datacite:Identifier
│   ├─ datacite:usesIdentifierScheme → datacite:doi
│   ├─ literalreification:hasLiteralValue → "10.xxxx/xxxx"
│   └─ rdfs:label → "identifier 978..."

## File principali

- `test.py` → esempio di esecuzione per testing.
- `enriched.rdf` → grafo arricchito con le entità del primo CSV.
- `incomplete.ttl` → BR incompleti rilevati durante l’arricchimento.

## Fonti e riferimenti

- [GraphSet Documentation](https://oc-ocdm.readthedocs.io/en/latest/modules/graph/oc_ocdm.graph.graph_set.html#oc_ocdm.graph.graph_set.GraphSet)  
- [OC GraphEnricher GitHub](https://github.com/opencitations/oc_graphenricher/tree/main)
