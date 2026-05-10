# Graph Enrichment Pipeline

This project implements a streaming pipeline for building and enriching a bibliographic RDF knowledge graph starting from large CSV datasets containing OMID-based records and external persistent identifiers.

## Overview

The pipeline:
- reads CSV files in streaming mode,
- extracts OMID, titles, and external identifiers (DOI, ISSN, OpenAlex, etc.),
- builds a RDF graph of Bibliographic Resources (BRs),
- attaches Identifier nodes to each BR,
- enriches metadata automatically using GraphEnricher,
- manages memory through batch processing and periodic GraphSet resets,
- exports enriched and incomplete RDF graphs.

## Data Model

### Bibliographic Resource (BR)
Each BR is identified by an OMID-based URI and represents a scholarly entity. It has identifier(s) and title (if present). 

### Identifier
Represents persistent identifiers associated with a BR:
- DOI
- ISSN
- ISBN
- PMID / PMCID
- OpenAlex ID
- URL

Each identifier stores:
- scheme type
- literal value

## Enrichment Process

Enrichment is performed using external scholarly APIs via the Graph Enricher module:
- Crossref (DOI, ISSN resolution)
- OpenAlex (entity linking and identifiers)
- ORCID (author identifiers)
- VIAF (authority control data)

New identifiers are automatically added when discovered.

## Output

The pipeline generates two outputs:

### Enriched graph
- Format: JSON-LD
- File: `enriched.jsonld`
- Contains all processed and enriched BRs

### Incomplete graph
- Format: Turtle (`.ttl`)
- File: `incomplete.ttl`
- Contains BRs missing at least one key identifier (DOI, ISSN, OpenAlex, Wikidata)

### Sources
- [OpenCitations Meta dump](https://download.opencitations.net/#meta)
- [OpenCitations Documentation](https://github.com/opencitations/crowdsourcing/blob/main/docs/csv_documentation-v1_1_2.pdf)
- [OC-OCDM Documentation](https://opencitations.github.io/oc_ocdm/) 
- [OC GraphEnricher GitHub](https://github.com/opencitations/oc_graphenricher/tree/main)
