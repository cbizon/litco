#!/usr/bin/env python3
"""
Shared SQLite cleaning logic for NGD and PubTator datasets.

This module provides generic functions to clean SQLite databases with curie_to_pmids schema:
- Two-pass normalization approach for memory efficiency
- Chunked processing to handle large datasets
- Unified output format (JSONL with 'publications' key)
"""

import sqlite3
import json
import ast
import logging
from pathlib import Path
from typing import Dict, List
from collections import defaultdict

from normalization import (
    CurieNormalizer, 
    write_biolink_classes
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SQLiteCleaner:
    """Generic SQLite cleaner for curie_to_pmids databases."""
    
    def __init__(self, input_db_path: str, output_dir: str, dataset_name: str):
        self.input_db_path = Path(input_db_path)
        self.output_dir = Path(output_dir)
        self.dataset_name = dataset_name
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize CURIE normalizer
        self.normalizer = CurieNormalizer()
    
    def extract_data_in_chunks(self, chunk_size: int = 50000):
        """Extract data in chunks using SQL LIMIT/OFFSET for memory efficiency."""
        logger.info(f"Extracting data from {self.input_db_path} in chunks of {chunk_size}")
        
        conn = sqlite3.connect(self.input_db_path)
        cursor = conn.cursor()
        
        offset = 0
        chunk_num = 1
        
        while True:
            cursor.execute("SELECT curie, pmids FROM curie_to_pmids LIMIT ? OFFSET ?", 
                          (chunk_size, offset))
            rows = cursor.fetchall()
            
            if not rows:
                break
                
            chunk_data = {}
            for curie, pmids_str in rows:
                try:
                    pmids_list = ast.literal_eval(pmids_str)
                    if isinstance(pmids_list, list):
                        chunk_data[curie] = pmids_list
                    else:
                        logger.warning(f"Unexpected pmids format for {curie}: {pmids_str}")
                except (ValueError, SyntaxError) as e:
                    logger.error(f"Failed to parse pmids for {curie}: {pmids_str} - {e}")
            
            if chunk_num % 10 == 0:  # Log every 10th chunk to reduce noise
                logger.info(f"Extracted chunk {chunk_num} ({chunk_num * chunk_size:,} records processed)")
            yield chunk_data
            
            offset += chunk_size
            chunk_num += 1
            
        conn.close()
    
    def build_complete_normalization_mapping(self) -> Dict[str, str]:
        """First pass: collect all CURIEs and build complete normalization mapping."""
        logger.info("Building complete normalization mapping (Pass 1)")
        
        # Collect all CURIEs without loading PMID data
        conn = sqlite3.connect(self.input_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT curie FROM curie_to_pmids")
        
        all_original_curies = []
        for (curie,) in cursor.fetchall():
            all_original_curies.append(curie)
        
        conn.close()
        logger.info(f"Found {len(all_original_curies)} total CURIEs to normalize")
        
        # Create dummy PMID data for normalization (normalizer needs this structure)
        dummy_pmid_data = {curie: [] for curie in all_original_curies}
        
        # Normalize all CURIEs
        complete_mapping = self.normalizer.normalize_all_curies(dummy_pmid_data)
        
        logger.info(f"Successfully normalized {len(complete_mapping)} CURIEs")
        return complete_mapping
    
    def write_failed_normalizations(self, filename: str):
        """Write failed normalization data to simple text format."""
        output_path = self.output_dir / filename
        failed_normalizations_dict = self.normalizer.get_failed_normalizations_dict()
        
        if failed_normalizations_dict:
            logger.info(f"Writing {len(failed_normalizations_dict)} failed normalization records to {output_path}")
            # Write simple text format with just the CURIEs
            with open(output_path, 'w') as f:
                for curie in sorted(failed_normalizations_dict.keys()):
                    f.write(curie + '\n')
        else:
            logger.info("No failed normalizations to write")
    
    def clean(self, chunk_size: int = 50000) -> int:
        """Run memory-efficient chunked cleaning pipeline."""
        logger.info(f"Starting {self.dataset_name} data cleaning pipeline")
        
        # Pass 1: Build complete normalization mapping
        normalization_mapping = self.build_complete_normalization_mapping()
        
        if not normalization_mapping:
            logger.error("No CURIEs were successfully normalized")
            return 0
        
        # Build reverse mapping: normalized_curie -> set of original_curies
        normalized_to_originals = defaultdict(set)
        for orig, norm in normalization_mapping.items():
            normalized_to_originals[norm].add(orig)
        
        logger.info(f"Found {len(normalized_to_originals)} unique normalized CURIEs")
        
        # Track progress: normalized_curie -> (seen_originals, pmids_so_far)
        progress_tracker = {}
        for norm_curie in normalized_to_originals:
            progress_tracker[norm_curie] = (set(), set())  # (seen_originals, pmids)
        
        records_written = 0
        
        # Open output file for streaming writes
        output_path = self.output_dir / f"{self.dataset_name}_cleaned.jsonl"
        logger.info(f"Writing results to {output_path}")
        
        with open(output_path, 'w') as output_file:
            # Pass 2: Process data chunks
            for chunk_data in self.extract_data_in_chunks(chunk_size):
                for original_curie, pmids in chunk_data.items():
                    if original_curie in normalization_mapping:
                        normalized_curie = normalization_mapping[original_curie]
                        seen_originals, accumulated_pmids = progress_tracker[normalized_curie]
                        
                        # Add this original CURIE and its PMIDs
                        seen_originals.add(original_curie)
                        accumulated_pmids.update(pmids)
                        
                        # Check if we've seen all original CURIEs for this normalized CURIE
                        expected_originals = normalized_to_originals[normalized_curie]
                        if seen_originals == expected_originals:
                            # Complete! Write record and free memory
                            record = {
                                "curie": normalized_curie,
                                "original_curies": sorted(expected_originals),
                                "publications": [f"PMID:{pmid}" for pmid in sorted(accumulated_pmids)]
                            }
                            output_file.write(json.dumps(record) + '\n')
                            del progress_tracker[normalized_curie]  # Free memory
                            records_written += 1
                            
                            if records_written % 10000 == 0:  # Log every 10K records
                                logger.info(f"Written {records_written:,} complete records")
        
        # Write failed normalizations
        self.write_failed_normalizations(f"{self.dataset_name}_failed_normalizations.txt")
        
        # Write biolink classes
        biolink_classes = self.normalizer.get_biolink_classes()
        if biolink_classes:
            biolink_classes_path = self.output_dir / f"{self.dataset_name}_biolink_classes.json"
            write_biolink_classes(biolink_classes, str(biolink_classes_path))
        
        logger.info(f"{self.dataset_name} data cleaning pipeline completed successfully")
        logger.info(f"Successfully written records: {records_written}")
        logger.info(f"Failed normalization records: {len(self.normalizer.get_failed_normalizations())}")
        
        return records_written


def clean_sqlite_curie_to_pmids(
    input_sqlite_path: str,
    output_dir: str, 
    dataset_name: str,
    chunk_size: int = 50000
) -> int:
    """
    Clean SQLite database with curie_to_pmids schema using two-pass normalization.
    
    Args:
        input_sqlite_path: Path to input SQLite database
        output_dir: Directory for output files
        dataset_name: Name of dataset (used in output filenames)
        chunk_size: Number of records to process per chunk
        
    Returns:
        Number of records written to output
    """
    cleaner = SQLiteCleaner(input_sqlite_path, output_dir, dataset_name)
    return cleaner.clean(chunk_size)