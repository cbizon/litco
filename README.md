# LITCO: Literature Co-Occurrence Investigation

A comprehensive toolkit for analyzing biomedical drug-disease relationships across literature databases and knowledge graphs.

## Overview

LITCO investigates how well biomedical entities (drugs and diseases) from clinical datasets are represented across multiple data sources. The project focuses on **MEDI (Medication Indication Resource)** drug-disease pairs and analyzes their coverage across:

### Literature Databases:
- **NGD** (Natural Language Database) - SQLite database with CURIE-to-PMID mappings
- **OmniCorp** - TSV files with PubMed-entity associations  
- **PubTator** - Gzipped TSV files with annotated biomedical concepts

### Knowledge Graphs:
- **RoboKOP** - Large-scale biomedical knowledge graph

### Clinical Data Sources:
- **Indications** - FDA/EMA/PMDA approved drug-disease treatment relationships
- **Contraindications** - Drug-disease pairs where treatment is contraindicated

All data sources are normalized using the [NodeNormalizer API](https://nodenormalization-dev.apps.renci.org/docs) to enable cross-source comparisons and identify coverage gaps.

## Installation

This project uses [uv](https://github.com/astral-sh/uv) for package management:

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync
```

## Usage

LITCO operates through a multi-phase analysis pipeline:

### Phase 1: Data Source Processing 

Convert raw data sources to a common SQLite format:

```bash
# Convert PubTator to NGD-compatible SQLite
uv run python src/pubtator_to_sqlite.py input/pubtator/bioconcepts2pubtator3.gz input/pubtator/pubtator_raw.sqlite

# Convert OmniCorp TSV files to NGD-compatible SQLite  
uv run python src/omnicorp_to_sqlite.py input/omnicorp/ input/omnicorp/omnicorp_raw.sqlite

# NGD is already in SQLite format
```

### Phase 2: Normalization and Cleaning

Normalize entities across all data sources using NodeNormalizer API:

```bash
# Clean and normalize literature databases
uv run python src/clean_ngd.py
uv run python src/clean_pubtator.py  
uv run python src/clean_omnicorp.py

# Extract identifier sets for fast analysis lookups
uv run python src/extract_identifiers.py

# Normalize RoboKOP knowledge graph nodes  
uv run python src/normalize_nodes.py input/robokop/nodes.jsonl analysis_results/raw_data_extracts/normalized_robokop_nodes.csv
```

**Outputs:**

**Literature Database Cleaning (`clean_*.py`):**
- `cleaned/ngd/ngd_cleaned.jsonl` - Normalized NGD data with standardized CURIE-to-PMID mappings
- `cleaned/pubtator/pubtator_cleaned.jsonl` - Normalized PubTator data with entity-to-PMID mappings
- `cleaned/omnicorp/omnicorp_cleaned.jsonl` - Normalized OmniCorp data with entity-to-PMID mappings
- `cleaned/*/biolink_classes.json` - Biolink type classifications for each dataset
- `cleaned/*/failed_normalizations.txt` - CURIEs that could not be normalized via NodeNormalizer API

**Identifier Extraction (`extract_identifiers.py`):**
- `analysis_results/raw_data_extracts/ngd_identifiers.pkl` - NGD identifier set (pickle format)
- `analysis_results/raw_data_extracts/pubtator_identifiers.pkl` - PubTator identifier set (pickle format) 
- `analysis_results/raw_data_extracts/omnicorp_identifiers.pkl` - OmniCorp identifier set (pickle format)
- `analysis_results/raw_data_extracts/all_identifiers.pkl` - Combined identifier sets for all datasets (used by analysis scripts for efficient coverage checking)

**RoboKOP Normalization (`normalize_nodes.py`):**
- `analysis_results/raw_data_extracts/normalized_robokop_nodes.csv` - Normalized RoboKOP knowledge graph nodes with preferred CURIEs and biolink types

### Phase 3: Coverage Analysis

Cross-reference MEDI entities with RoboKOP knowledge graph and generate complete coverage reports:

```bash
uv run python src/analysis/medi_inspection.py
```

**Purpose:** Generate comprehensive entity coverage analysis:
- Literature databases (NGD/PubTator/OmniCorp) coverage for each entity
- RoboKOP knowledge graph coverage 
- Human-readable labels from original MEDI data
- Complete cross-source comparison

**Output:** `analysis_results/medi_inspection/`
- `medi_inspection_summary.txt` - Overall coverage statistics  
- `medi_drugs_robokop_comparison.csv` - Complete drug coverage analysis (2,419 drugs)
- `medi_diseases_robokop_comparison.csv` - Complete disease coverage analysis (3,017 diseases)

**Note:** These comprehensive files replace the previous `unique_drugs_coverage.csv` and `unique_diseases_coverage.csv` files, providing the same information plus RoboKOP data.

### Phase 4: Missing Entity Investigation

Investigate entities missing from all literature databases:

```bash
uv run python src/analysis/missing_entity_analysis.py --max-entities 500 --verbose
```

**Purpose:** For entities not found in any literature database:
1. Search PubMed for entity labels → get top PMIDs
2. Check what entities ARE found in those PMIDs across our datasets
3. Identify systematic gaps in literature extraction

**Input:** Uses comprehensive coverage results from `medi_inspection.py`
**Output:** `analysis_results/missing_entities/`
- `gap_analysis_summary.txt` - Human-readable summary
- `gap_analysis_report.json` - Detailed analysis results
- `missing_drugs.csv` / `missing_diseases.csv` - Completely missing entities

### Pipeline Architecture

**Phase 1 Features:**
- **PubTator**: Handles semicolon-delimited concept IDs, converts to proper CURIEs based on entity type
- **OmniCorp**: Optimized per-file sorting with multi-way merge for faster processing of large TSV collections
- **Memory-efficient**: Streaming processing with external sorting for arbitrarily large datasets
- **Intelligence gathering**: Tracks unknown patterns for analysis

**Phase 2 Features:**
- **Two-pass normalization**: Builds complete CURIE mapping first, then processes in memory-bounded chunks
- **API normalization**: Uses NodeNormalizer API with batching and retry logic
- **CURIE merging**: Handles cases where multiple input CURIEs normalize to same concept
- **Unified output**: Standardized JSONL format across all data sources

### Output Files

**Phase 1 Output:**
- `*_raw.sqlite` - NGD-compatible SQLite databases
- `*_unknown_*.txt` - Unknown patterns found during conversion (for analysis)

**Phase 2 Output:**
- `*_cleaned.jsonl` - Successfully normalized data  
- `*_failed_normalizations.txt` - Failed normalization attempts
- `*_biolink_classes.json` - Biolink type classifications

### Testing

Run the test suite:

```bash
uv run pytest
```

## Project Structure

```
litco/
├── input/                 # Source data (never modified)
│   ├── ngd/              # NGD SQLite database
│   ├── omnicorp/         # OmniCorp TSV files
│   ├── pubtator/         # PubTator gzipped files
│   └── medi/             # MEDI indication/contraindication files
├── cleaned/              # Processed output
│   ├── ngd/
│   ├── omnicorp/
│   └── pubtator/
├── analysis_results/     # Analysis outputs
│   ├── medi_inspection/
│   ├── missing_entities/
│   └── raw_data_extracts/
├── src/                  # Source code
│   ├── analysis/         # Analysis scripts
│   ├── normalization.py  # Shared normalization utilities
│   ├── clean_ngd.py     # NGD data processor
│   ├── clean_pubtator.py # PubTator data processor
│   └── clean_omnicorp.py # OmniCorp data processor
└── tests/               # Test suite
```

## Data Sources

### NGD
- **Input**: SQLite database with `curie_to_pmids` table
- **Format**: `CURIE|[pmid1, pmid2, ...]`
- **Example**: `NCBITaxon:5322|[25506817, 30584069, ...]`

### OmniCorp  
- **Input**: TSV files with PubMed URL and entity IRI columns
- **Format**: `https://www.ncbi.nlm.nih.gov/pubmed/PMID    ENTITY_IRI`
- **Example**: `https://www.ncbi.nlm.nih.gov/pubmed/3963809    http://purl.obolibrary.org/obo/PATO_0001421`

### PubTator
- **Input**: Gzipped TSV with 5 columns
- **Format**: `PMID    Type    ConceptID    Mentions    Resource`
- **Example**: `12345    gene    1234    insulin    gene2pubmed`

## Analysis Workflow Summary

The complete LITCO analysis pipeline answers key research questions:

### 1. **Data Availability**: 
   - Which MEDI drug-disease pairs are found in literature databases?
   - How does coverage vary between NGD, PubTator, and OmniCorp?

### 2. **Normalization Impact**: 
   - Do entities have better coverage before or after CURIE normalization?
   - Where do normalization processes improve or hurt coverage?

### 3. **Missing Entity Investigation**: 
   - For entities missing from all literature databases, are they truly absent from literature?
   - What other entities appear in papers where missing entities should appear?

### 4. **Knowledge Graph Coverage**: 
   - How does RoboKOP knowledge graph coverage compare to literature database coverage?
   - Which entities are well-represented in knowledge graphs but missing from literature extraction?

### 5. **Systematic Gaps**: 
   - Are there systematic biases in literature extraction or knowledge graph construction?
   - What types of entities (by prefix, biolink type, etc.) are systematically underrepresented?

## Key Research Findings

Current analysis reveals significant insights about biomedical entity coverage:

### Literature Database Coverage (Phase 3)
**Indications (11,071 drug-disease pairs):**
- **NGD**: 9,010 pairs (81.4%) - excellent coverage
- **PubTator**: 5,565 pairs (50.3%) - moderate coverage  
- **OmniCorp**: 7,618 pairs (68.8%) - good coverage
- **At least one dataset**: 9,488 pairs (85.7%) - very good overall coverage

**Contraindications (3,981 drug-disease pairs):**
- **NGD**: 2,661 pairs (66.8%) - good coverage
- **PubTator**: 1,678 pairs (42.2%) - moderate coverage
- **OmniCorp**: 2,263 pairs (56.8%) - moderate coverage  
- **At least one dataset**: 2,775 pairs (69.7%) - good overall coverage

### Cross-Source Coverage (Phase 4)
**MEDI Entity Coverage across Literature + Knowledge Graph:**
- **Total entities**: 5,436 (2,419 drugs + 3,017 diseases)
- **Drugs in RoboKOP**: 1,745/2,419 (72.1%)
- **Diseases in RoboKOP**: 2,716/3,017 (90.0%)
- **Drugs in literature**: 1,961/2,419 (81.1%)
- **Diseases in literature**: 2,735/3,017 (90.7%)

### Missing Entity Gaps (Phase 5)
- **740 MEDI entities** completely missing from all literature databases
- **458 missing drugs** and **282 missing diseases**
- Systematic investigation shows most are genuinely rare in literature, not extraction failures

### Critical Bug Fix
- **Fixed normalization logic** that was incorrectly treating same-ID normalizations as failures
- **Previous false negatives** showed 0% coverage; corrected analysis shows 50-90% coverage rates
- **Proper handling** of cases where CHEBI:10023 → CHEBI:10023 (successful normalization, not failure)

## Recent Updates & Improvements

### September 2024 Major Updates
1. **Normalization Logic Fix**: Corrected critical bug in coverage analysis that was treating successful same-ID normalizations as failures
2. **RoboKOP nodes.jsonl Support**: Updated `medi_inspection.py` to directly read RoboKOP nodes.jsonl format (no normalization required)  
3. **Streamlined Output**: Eliminated redundant `unique_*_coverage.csv` files - all entity coverage now in comprehensive `medi_inspection` results
4. **Label Integration**: Added human-readable labels from source MEDI data to all output files
5. **Path Corrections**: Fixed relative path issues for running analysis scripts from `src/analysis/` directory
6. **Improved Dependencies**: All analysis scripts now properly reference the comprehensive coverage files

### Current Pipeline Status
- ✅ **Phase 1-2**: Data processing and normalization (stable)
- ✅ **Phase 3**: Drug-disease pair coverage analysis (bug fixed)  
- ✅ **Phase 4**: Comprehensive entity coverage with RoboKOP integration (updated)
- ✅ **Phase 5**: Missing entity investigation (dependencies updated)

## Contributing

- Use `uv` for dependency management
- Run `pytest` before committing  
- Maintain high code coverage
- Follow existing code patterns
- Document analysis scripts with clear docstrings explaining purpose and output