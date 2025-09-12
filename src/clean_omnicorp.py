#!/usr/bin/env python3
"""
Clean OmniCorp data by normalizing CURIEs and converting to JSONLINES format.

This script:
1. Processes TSV files with PubMed URLs and entity IRIs
2. Converts PubMed URLs to PMID integers
3. Converts entity IRIs to CURIEs
4. Normalizes CURIEs using the node normalizer API
5. Handles merging when normalized CURIEs collapse to the same identifier
6. Outputs cleaned data as JSONLINES with 'publications' key
"""

import logging
import json
import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set

from normalization import (
    CurieNormalizer,
    merge_normalized_data,
    convert_to_output_format,
    convert_failed_to_output_format,
    write_biolink_classes
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class OmniCorpCleaner:
    """Clean and normalize OmniCorp data."""
    
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize CURIE normalizer
        self.normalizer = CurieNormalizer()
        
        # Track data
        self.curie_to_pmids = defaultdict(set)
    
    def extract_pmid_from_url(self, pubmed_url: str) -> int:
        """Extract PMID from PubMed URL."""
        # URL format: https://www.ncbi.nlm.nih.gov/pubmed/4307
        match = re.search(r'/pubmed/(\d+)$', pubmed_url)
        if match:
            return int(match.group(1))
        else:
            raise ValueError(f"Could not extract PMID from URL: {pubmed_url}")
    
    def convert_iri_to_curie(self, iri: str) -> str:
        """Convert IRI to CURIE format."""
        # Handle different IRI patterns
        
        # CHEBI: http://purl.obolibrary.org/obo/CHEBI_17822 -> CHEBI:17822
        if 'purl.obolibrary.org/obo/CHEBI_' in iri:
            return iri.replace('http://purl.obolibrary.org/obo/CHEBI_', 'CHEBI:')
        
        # MESH: http://id.nlm.nih.gov/mesh/D014346 -> MESH:D014346  
        if 'id.nlm.nih.gov/mesh/' in iri:
            return iri.replace('http://id.nlm.nih.gov/mesh/', 'MESH:')
        
        # Other OBO terms: http://purl.obolibrary.org/obo/RO_0002432 -> RO:0002432
        obo_match = re.search(r'purl\.obolibrary\.org/obo/([A-Z]+)_(\d+)', iri)
        if obo_match:
            prefix = obo_match.group(1)
            number = obo_match.group(2)
            return f"{prefix}:{number}"
        
        # If we can't convert, log and return the original IRI
        logger.warning(f"Could not convert IRI to CURIE: {iri}")
        return iri
    
    def process_tsv_file(self, file_path: Path):
        """Process a single TSV file."""
        logger.info(f"Processing {file_path}")
        
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    # Split on tab
                    parts = line.split('\t')
                    if len(parts) != 2:
                        logger.warning(f"Line {line_num} has {len(parts)} columns, expected 2: {line}")
                        continue
                    
                    pubmed_url, entity_iri = parts
                    
                    # Extract PMID
                    try:
                        pmid = self.extract_pmid_from_url(pubmed_url)
                    except ValueError as e:
                        logger.warning(f"Line {line_num}: {e}")
                        continue
                    
                    # Convert IRI to CURIE
                    curie = self.convert_iri_to_curie(entity_iri)
                    
                    # Store in our data structure
                    self.curie_to_pmids[curie].add(pmid)
                    
                except Exception as e:
                    logger.error(f"Error processing line {line_num} in {file_path}: {e}")
                    logger.error(f"Line content: {line}")
    
    def process_all_files(self):
        """Process all TSV files in the input directory."""
        # Find all .tsv files
        tsv_files = list(self.input_dir.glob("*.tsv"))
        
        if not tsv_files:
            logger.warning(f"No .tsv files found in {self.input_dir}")
            return
        
        logger.info(f"Found {len(tsv_files)} TSV files to process")
        
        for file_path in sorted(tsv_files):
            self.process_tsv_file(file_path)
        
        logger.info(f"Processed all files. Found {len(self.curie_to_pmids)} unique CURIEs")
        
        # Convert sets to lists for compatibility with normalizer
        curie_to_pmids_list = {curie: list(pmids) for curie, pmids in self.curie_to_pmids.items()}
        return curie_to_pmids_list
    
    def write_jsonlines(self, data: List[Dict], filename: str):
        """Write data to JSONLINES format."""
        output_path = self.output_dir / filename
        logger.info(f"Writing {len(data)} records to {output_path}")
        
        with open(output_path, 'w') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')
                
        logger.info(f"Successfully wrote {output_path}")
    
    def write_failed_normalizations(self, filename: str):
        """Write failed normalization data to simple text format."""
        output_path = self.output_dir / filename.replace('.jsonl', '.txt')
        failed_normalizations = self.normalizer.get_failed_normalizations()
        failed_data = convert_failed_to_output_format(failed_normalizations)
        
        logger.info(f"Writing {len(failed_data)} failed normalization records to {output_path}")
        
        with open(output_path, 'w') as f:
            for curie in failed_data:
                f.write(curie + '\n')
                
        logger.info(f"Successfully wrote {output_path}")
    
    def clean(self):
        """Run the complete cleaning pipeline."""
        logger.info("Starting OmniCorp data cleaning pipeline")
        
        # Step 1: Process all TSV files
        curie_to_pmids = self.process_all_files()
        
        if not curie_to_pmids:
            logger.error("No data extracted from files")
            return
        
        # Step 2: Normalize CURIEs
        normalized_mapping = self.normalizer.normalize_all_curies(curie_to_pmids)
        
        # Step 3: Merge data based on normalized CURIEs
        merged_data, original_curies_by_normalized = merge_normalized_data(curie_to_pmids, normalized_mapping)
        
        # Step 4: Convert to output format
        output_data = convert_to_output_format(merged_data, original_curies_by_normalized)
        
        # Step 5: Write main JSONLINES output
        self.write_jsonlines(output_data, "omnicorp_cleaned.jsonl")
        
        # Step 6: Write failed normalizations
        self.write_failed_normalizations("omnicorp_failed_normalizations.jsonl")
        
        # Step 7: Write biolink classes
        biolink_classes = self.normalizer.get_biolink_classes()
        if biolink_classes:
            biolink_classes_path = self.output_dir / "omnicorp_biolink_classes.json"
            write_biolink_classes(biolink_classes, str(biolink_classes_path))
        
        logger.info("OmniCorp data cleaning pipeline completed successfully")
        logger.info(f"Total records processed: {len(curie_to_pmids)}")
        logger.info(f"Successfully normalized records: {len(output_data)}")
        logger.info(f"Failed normalization records: {len(self.normalizer.get_failed_normalizations())}")


def main():
    """Main entry point."""
    input_dir = "input/omnicorp"
    output_dir = "cleaned/omnicorp"
    
    cleaner = OmniCorpCleaner(input_dir, output_dir)
    cleaner.clean()


if __name__ == "__main__":
    main()