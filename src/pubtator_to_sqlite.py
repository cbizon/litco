#!/usr/bin/env python3
"""
Convert PubTator TSV format to NGD-compatible SQLite format.

This script uses a memory-efficient sort-based approach:
1. Pass 1: Stream through PubTator file, convert IRIs to CURIEs, write to temp file, sort
2. Pass 2: Stream through sorted file to aggregate PMIDs by CURIE into SQLite

Output SQLite has same schema as NGD: curie_to_pmids table with curie|pmids columns.
"""

import gzip
import json
import logging
import sqlite3
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PubTatorToSQLiteConverter:
    """Convert PubTator format to NGD-compatible SQLite format."""
    
    def __init__(self, input_file: str, output_sqlite: str):
        self.input_file = Path(input_file)
        self.output_sqlite = Path(output_sqlite)
        
        # Statistics tracking
        self.stats = {
            'lines_processed': 0,
            'valid_pairs': 0,
            'invalid_concept_ids': 0,
            'curie_constructions': defaultdict(int)
        }
        
        # Track unknown patterns for analysis
        self.unknown_patterns = set()
    
    def convert_concept_id_to_curie(self, concept_id: str, entity_type: str) -> Optional[str]:
        """Convert concept ID to proper CURIE format based on type context."""
        # Skip invalid/missing concept IDs
        if concept_id == "-" or not concept_id.strip():
            return None
            
        # Already in CURIE format (contains colon)
        if ":" in concept_id:
            return concept_id
        
        # Bare number - infer prefix based on entity type
        if concept_id.isdigit():
            if entity_type.lower() == "species":
                curie = f"NCBITaxon:{concept_id}"
                self.stats['curie_constructions']['Species->NCBITaxon'] += 1
                return curie
            elif entity_type.lower() == "gene":
                curie = f"NCBIGene:{concept_id}"
                self.stats['curie_constructions']['Gene->NCBIGene'] += 1
                return curie
            elif entity_type.lower() == "disease":
                curie = f"OMIM:{concept_id}"
                self.stats['curie_constructions']['Disease->OMIM'] += 1
                return curie
            elif entity_type.lower() == "chemical":
                # Chemicals as numbers are tricky - treat as unknown for now
                curie = f"UNKNOWN_CHEMICAL:{concept_id}"
                self.stats['curie_constructions']['Chemical->UNKNOWN'] += 1
                
                # Track unknown pattern for analysis
                self.unknown_patterns.add(f"UNKNOWN_CHEMICAL:{concept_id}")
                
                # Log first few examples
                if self.stats['curie_constructions']['Chemical->UNKNOWN'] <= 10:
                    logger.warning(f"UNKNOWN chemical number #{self.stats['curie_constructions']['Chemical->UNKNOWN']}: {concept_id} -> {curie}")
                elif self.stats['curie_constructions']['Chemical->UNKNOWN'] == 11:
                    logger.warning("... (suppressing further UNKNOWN chemical examples)")
                    
                return curie
            else:
                curie = f"UNKNOWN_{entity_type.upper()}:{concept_id}"
                self.stats['curie_constructions'][f'{entity_type}->UNKNOWN'] += 1
                
                # Track unknown pattern for analysis
                self.unknown_patterns.add(f"UNKNOWN_{entity_type.upper()}:{concept_id}")
                
                # Log first few examples of each unknown entity type
                unknown_key = f'{entity_type}->UNKNOWN'
                if self.stats['curie_constructions'][unknown_key] <= 5:
                    logger.warning(f"UNKNOWN entity type #{self.stats['curie_constructions'][unknown_key]}: {entity_type}:{concept_id} -> {curie}")
                elif self.stats['curie_constructions'][unknown_key] == 6:
                    logger.warning(f"... (suppressing further UNKNOWN {entity_type} examples)")
                    
                return curie
        
        # Handle CVCL cell line format (these are Cellosaurus identifiers)
        if concept_id.startswith("CVCL_"):
            # Convert to correct Cellosaurus prefix (won't normalize but is accurate)
            curie = concept_id.replace("CVCL_", "Cellosaurus:", 1)
            self.stats['curie_constructions']['CVCL_->Cellosaurus'] += 1
            return curie
        
        # Other formats - return as-is
        self.stats['curie_constructions'][f'{entity_type}->OTHER'] += 1
        
        # Track unknown pattern for analysis
        self.unknown_patterns.add(f"OTHER_{entity_type.upper()}:{concept_id}")
        
        return concept_id
    
    def pass1_extract_and_sort(self) -> Path:
        """Pass 1: Extract CURIE-PMID pairs and create sorted temporary file."""
        logger.info("Pass 1: Extracting and sorting CURIE-PMID pairs")
        
        # Create temporary file for CURIE-PMID pairs
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False)
        temp_path = Path(temp_file.name)
        
        try:
            with gzip.open(self.input_file, 'rt') as f:
                for line_num, line in enumerate(f, 1):
                    self.stats['lines_processed'] += 1
                    
                    if line_num % 1000000 == 0:
                        logger.info(f"Pass 1: {line_num:,} lines processed")
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Parse TSV line: PMID, Type, Concept ID, Mentions, Resource
                    parts = line.split('\t')
                    if len(parts) != 5:
                        continue
                    
                    pmid_str, entity_type, concept_id, mentions, resource = parts
                    
                    # Parse PMID
                    try:
                        pmid = int(pmid_str)
                    except ValueError:
                        continue
                    
                    # Split semicolon-delimited concept IDs and process each individually
                    concept_ids = concept_id.split(';')
                    for individual_concept_id in concept_ids:
                        individual_concept_id = individual_concept_id.strip()
                        if not individual_concept_id:
                            continue
                            
                        # Convert to CURIE
                        curie = self.convert_concept_id_to_curie(individual_concept_id, entity_type)
                        if curie is None:
                            self.stats['invalid_concept_ids'] += 1
                            continue
                        
                        # Write CURIE-PMID pair to temp file
                        temp_file.write(f"{curie}\t{pmid}\n")
                        self.stats['valid_pairs'] += 1
            
            temp_file.close()
            logger.info(f"Pass 1: {self.stats['lines_processed']:,} lines processed, {self.stats['valid_pairs']:,} valid pairs extracted")
            
            # Sort the temporary file by CURIE (first column)
            sorted_temp = temp_path.with_suffix('.sorted.tsv')
            logger.info("Sorting CURIE-PMID pairs...")
            
            # Use Unix sort for efficient external sorting
            subprocess.run([
                'sort', '-k1,1', str(temp_path), '-o', str(sorted_temp)
            ], check=True)
            
            # Remove unsorted temp file
            temp_path.unlink()
            logger.info(f"Pass 1 complete: sorted file created at {sorted_temp}")
            return sorted_temp
            
        except Exception as e:
            temp_file.close()
            if temp_path.exists():
                temp_path.unlink()
            raise e
    
    def pass2_aggregate_to_sqlite(self, sorted_file: Path):
        """Pass 2: Aggregate sorted CURIE-PMID pairs into SQLite database."""
        logger.info("Pass 2: Aggregating to SQLite format")
        
        # Create output SQLite database
        self.output_sqlite.parent.mkdir(parents=True, exist_ok=True)
        if self.output_sqlite.exists():
            self.output_sqlite.unlink()
        
        conn = sqlite3.connect(str(self.output_sqlite))
        cursor = conn.cursor()
        
        # Create table with same schema as NGD
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS curie_to_pmids (
                curie TEXT PRIMARY KEY,
                pmids TEXT
            )
        ''')
        
        current_curie = None
        current_pmids = []
        records_written = 0
        
        try:
            with open(sorted_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    if line_num % 1000000 == 0:
                        logger.info(f"Pass 2: {line_num:,} lines processed, {records_written:,} records written")
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split('\t')
                    if len(parts) != 2:
                        continue
                    
                    curie, pmid_str = parts
                    try:
                        pmid = int(pmid_str)
                    except ValueError:
                        continue
                    
                    # Check if we're still on the same CURIE
                    if curie == current_curie:
                        current_pmids.append(pmid)
                    else:
                        # Write previous CURIE if we have one
                        if current_curie is not None:
                            pmids_json = json.dumps(sorted(set(current_pmids)))  # Dedupe and sort
                            cursor.execute(
                                'INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)',
                                (current_curie, pmids_json)
                            )
                            records_written += 1
                            
                            if records_written % 50000 == 0:
                                conn.commit()  # Periodic commits
                        
                        # Start new CURIE
                        current_curie = curie
                        current_pmids = [pmid]
                
                # Don't forget the last CURIE
                if current_curie is not None:
                    pmids_json = json.dumps(sorted(set(current_pmids)))
                    cursor.execute(
                        'INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)',
                        (current_curie, pmids_json)
                    )
                    records_written += 1
            
            conn.commit()
            logger.info(f"Pass 2 complete: {records_written:,} records written to SQLite")
            
        finally:
            conn.close()
            # Clean up sorted temp file
            if sorted_file.exists():
                sorted_file.unlink()
    
    def log_statistics(self):
        """Log processing statistics."""
        logger.info("=== CONVERSION STATISTICS ===")
        logger.info(f"Total lines processed: {self.stats['lines_processed']:,}")
        logger.info(f"Valid CURIE-PMID pairs: {self.stats['valid_pairs']:,}")
        logger.info(f"Invalid concept IDs: {self.stats['invalid_concept_ids']:,}")
        
        if self.stats['curie_constructions']:
            logger.info("CURIE construction patterns:")
            for pattern, count in sorted(self.stats['curie_constructions'].items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {pattern}: {count:,}")
    
    def convert(self):
        """Run the complete conversion pipeline."""
        logger.info(f"Converting PubTator file to SQLite: {self.input_file} -> {self.output_sqlite}")
        
        # Pass 1: Extract and sort
        sorted_file = self.pass1_extract_and_sort()
        
        # Write unknown patterns after Pass 1 (before Pass 2 in case it fails)
        self.write_unknown_patterns()
        
        # Pass 2: Aggregate to SQLite
        self.pass2_aggregate_to_sqlite(sorted_file)
        
        # Log statistics
        self.log_statistics()
        
        logger.info("Conversion completed successfully")
    
    def write_unknown_patterns(self):
        """Write unknown concept ID patterns to file for analysis."""
        if self.unknown_patterns:
            output_dir = Path("cleaned/pubtator")
            output_dir.mkdir(parents=True, exist_ok=True)
            unknown_file = output_dir / "pubtator_unknown_patterns.txt"
            
            logger.info(f"Writing {len(self.unknown_patterns)} unknown patterns to {unknown_file}")
            with open(unknown_file, 'w') as f:
                for pattern in sorted(self.unknown_patterns):
                    f.write(pattern + '\n')
        else:
            logger.info("No unknown patterns to write")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert PubTator TSV to NGD-compatible SQLite')
    parser.add_argument('input_file', help='Input gzipped PubTator TSV file')
    parser.add_argument('output_sqlite', help='Output SQLite database file')
    
    args = parser.parse_args()
    
    converter = PubTatorToSQLiteConverter(args.input_file, args.output_sqlite)
    converter.convert()


if __name__ == "__main__":
    main()