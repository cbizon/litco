# LITCO: Literature CoOccurence Investigation

## Goal

We have a number of tools and data sets looking at the existence of biomedical entities in particular pubmed ids.  We want to compare these.  In particular we are going to compare
- NGD
- OmniCorp
- PubTator

## Basic Setup

* **github**: This project has a github repo at https://github.com/cbizon/litco
* **uv**: we are using uv for package and environment management
* **tests**: we are using pytest, and want to maintain high code coverage

### Commands
```bash
# Install dependencies
uv sync

# Run cleaners (memory-efficient chunked processing)
uv run python src/clean_ngd.py
uv run python src/clean_pubtator.py  
uv run python src/clean_omnicorp.py

# Monitor NGD processing progress
python src/monitor_ngd.py

# Convert cleaned JSONL back to SQLite format
uv run python src/jsonl_to_sqlite.py cleaned/ngd/ngd_cleaned.jsonl cleaned/ngd/ngd_cleaned.sqlite

# Run tests
uv run pytest

# Check coverage
uv run pytest --cov=src
```

## Key Dependencies

### APIs

@../cdocs/nodenorm.md

## Project structure

```
litco/
├── input/                    # Source data (never modified)
│   ├── ngd/                 # NGD SQLite database
│   ├── omnicorp/            # OmniCorp TSV files  
│   ├── pubtator/            # PubTator gzipped files
│   └── tsv-output/          # Additional output directory
├── cleaned/                  # Processed output (JSONL + SQLite format)
│   ├── ngd/                 # NGD cleaned outputs
│   │   ├── ngd_cleaned.jsonl           # Main normalized output
│   │   ├── ngd_cleaned.sqlite          # SQLite format (same schema as input)
│   │   ├── ngd_biolink_classes.json    # Biolink type classifications
│   │   └── ngd_failed_normalizations.txt  # CURIEs that failed normalization
│   ├── omnicorp/
│   └── pubtator/
├── src/                      # Source code
│   ├── normalization.py     # Shared CURIE normalization utilities
│   ├── clean_ngd.py        # NGD data cleaning pipeline (memory-efficient chunked processing)
│   ├── clean_pubtator.py   # PubTator data cleaning pipeline
│   ├── clean_omnicorp.py   # OmniCorp data cleaning pipeline
│   ├── monitor_ngd.py      # NGD processing progress monitor
│   └── jsonl_to_sqlite.py  # Convert JSONL output back to SQLite format
└── tests/                   # Test suite
    ├── test_normalization.py
    ├── test_clean_ngd.py
    ├── test_clean_pubtator.py
    └── test_clean_omnicorp.py
```

## Input

The input data may never be changed. 

### NGD

The input data is found in input/ngd

It is a sqlite3 database: data\_01\_RAW\_KGs\_rtx\_kg2\_v2.10.0\_curie\_to\_pmids.sqlite
The database has a single table: curie\_to\_pmids

It looks like:
sqlite> select * from curie\_to\_pmids limit 1;
NCBITaxon:5322|[25506817, 30584069, 38317964, 32916239, 38095768, 28785311, 34251681, 38395555, 38827301, 23997606, 24419623, 36591916, 38361132, 38862766, 34361776, 32036274, 33043511, 38056637, 24049471, 38930624, 38480583, 38784713, 38935369, 30647626, 28386251, 39004366, 37627983, 38760401, 25763030, 38823127, 38944091, 34855004, 38135005, 28324451, 38774755, 34354531, 30487785, 38657898, 38790763, 38231788, 38166381, 38690030, 38090223, 25897324, 38539882, 38352628, 38881654, 38398710, 38128888, 37458939]

### OmniCorp
There are a series of tsv files in input/omnicorp that all have the following 2 column structure.

https://www.ncbi.nlm.nih.gov/pubmed/3963809     http://purl.obolibrary.org/obo/PATO_0001421
https://www.ncbi.nlm.nih.gov/pubmed/3963809     http://purl.obolibrary.org/obo/PATO_0001510
https://www.ncbi.nlm.nih.gov/pubmed/3963809     http://purl.obolibrary.org/obo/NBO_0000313
https://www.ncbi.nlm.nih.gov/pubmed/3963809     http://id.nlm.nih.gov/mesh/D001154
https://www.ncbi.nlm.nih.gov/pubmed/3963809     http://purl.obolibrary.org/obo/SO_0001514
https://www.ncbi.nlm.nih.gov/pubmed/3963809     http://purl.obolibrary.org/obo/HP_0000001
https://www.ncbi.nlm.nih.gov/pubmed/3963809     http://purl.obolibrary.org/obo/BFO_0000023

The first column is an IRI for a pubmed resource.  The second is an IRI for an entity.  These entities will also need to be normalized.

### PubTator

The PubTator data is a gzipped TSV file: `bioconcepts2pubtator3.gz`

The entity files contain five columns:
- **PMID**: PubMed abstract identifier
- **Type**: Entity type (gene, disease, chemical, species, mutation)
- **Concept ID**: Database identifier (may be bare numbers needing prefix inference)
- **Mentions**: Bio-concept mentions in the abstract
- **Resource**: Source annotation resource (e.g., MeSH, gene2pubmed)


## Implementation

### Memory-Efficient Processing (NGD)

The NGD cleaner uses a memory-efficient chunked processing approach to handle large datasets (2.1M+ CURIEs):

1. **Two-Pass Architecture**: 
   - Pass 1: Extract all CURIEs and build complete normalization mapping
   - Pass 2: Process data in chunks, writing complete records immediately
2. **Chunked Data Extraction**: SQL-based chunking using LIMIT/OFFSET
3. **Streaming Output**: Write normalized records as soon as all original CURIEs for a normalized CURIE are encountered
4. **Memory Bounded**: Memory usage stays constant regardless of dataset size

### Normalization Pipeline

All three data sources are processed through a common normalization pipeline implemented in `src/normalization.py`:

1. **CURIE Extraction**: Extract CURIE-PMID pairs from source format
2. **API Normalization**: Batch normalize CURIEs using NodeNormalizer API  
3. **Merging**: Handle cases where multiple input CURIEs normalize to same concept
4. **Output**: Generate JSONL files with standardized format

### Key Components

#### `normalization.py`
- `RobustAPIClient`: HTTP client with exponential backoff and retry logic
- `CurieNormalizer`: Handles batched CURIE normalization with failure tracking  
- `merge_normalized_data()`: Merges PMID sets when CURIEs collapse to same concept
- `convert_to_output_format()`: Standardizes output to JSONL with 'publications' key

#### Data Cleaners
- `clean_ngd.py`: Memory-efficient chunked processing of SQLite database with two-pass architecture
- `clean_pubtator.py`: Streams through gzipped file, infers CURIE prefixes from entity types
- `clean_omnicorp.py`: Processes TSV files, converts IRIs to CURIEs

#### Utility Scripts
- `monitor_ngd.py`: Real-time monitoring of NGD processing progress
- `jsonl_to_sqlite.py`: Convert cleaned JSONL output back to SQLite format (same schema as input)

#### Output Format
All cleaners produce JSONL files with consistent schema:
```json
{
  "curie": "CHEBI:15377",
  "publications": ["PMID:12345", "PMID:67890"],
  "original_curies": ["MESH:D014867"],
  "biolink_types": ["biolink:SmallMolecule"]
}
```

### Testing

Each component has comprehensive tests:
- Unit tests for normalization utilities
- Integration tests for each cleaner
- Mock API responses to avoid external dependencies
- Coverage tracking with pytest

## ***RULES OF THE ROAD***

- Don't use mocks. 

- Ask clarifying questions

- Do not implement bandaids - treat the root cause of problems

- Once we have a test, do not delete it without explicit permission.  

- Do not return made up results if an API fails.  Let it fail.

- When changing code, don't make duplicate functions - just change the function. We can always roll back changes if needed.

- Keep the directories clean, don't leave a bunch of junk laying around.

- When making pull requests, NEVER ever mention a `co-authored-by` or similar aspects. In particular, never mention the tool used to create the commit message or PR.

- Check git status before commits

- always work on a feature branch, and suggest when we should make a pull request.

