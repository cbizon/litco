#!/usr/bin/env python3
"""
Convert cleaned NGD JSONL output back to SQLite format.

This script:
1. Reads the cleaned NGD JSONL file
2. Converts it back to the same SQLite format as the original input
3. Creates a table with same structure: curie_to_pmids (curie TEXT, pmids TEXT)
4. Formats PMIDs as Python list strings (same as input format)
"""

import sqlite3
import json
import logging
from pathlib import Path
from typing import List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def convert_jsonl_to_sqlite(jsonl_file: str, sqlite_file: str):
    """Convert JSONL output back to SQLite format matching input structure."""
    jsonl_path = Path(jsonl_file)
    sqlite_path = Path(sqlite_file)
    
    if not jsonl_path.exists():
        raise FileNotFoundError(f"Input JSONL file not found: {jsonl_path}")
    
    logger.info(f"Converting {jsonl_path} to {sqlite_path}")
    
    # Create SQLite database
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    
    # Create table with same structure as input
    cursor.execute("DROP TABLE IF EXISTS curie_to_pmids")
    cursor.execute("CREATE TABLE curie_to_pmids (curie TEXT, pmids TEXT)")
    cursor.execute("CREATE UNIQUE INDEX unique_curie ON curie_to_pmids (curie)")
    
    # Process JSONL file
    records_processed = 0
    records_written = 0
    
    with open(jsonl_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            records_processed += 1
            
            if records_processed % 50000 == 0:
                logger.info(f"Processed {records_processed:,} records")
            
            try:
                record = json.loads(line.strip())
                
                # Extract data
                curie = record['curie']
                publications = record['publications']
                
                # Convert PMID:XXXXX format back to plain integers
                pmids = []
                for pub in publications:
                    if pub.startswith('PMID:'):
                        pmid_str = pub[5:]  # Remove 'PMID:' prefix
                        try:
                            pmid_int = int(pmid_str)
                            pmids.append(pmid_int)
                        except ValueError:
                            logger.warning(f"Invalid PMID format: {pub}")
                    else:
                        logger.warning(f"Unexpected publication format: {pub}")
                
                # Format as Python list string (same as input format)
                pmids_str = str(pmids)
                
                # Insert into database
                cursor.execute("INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)", 
                             (curie, pmids_str))
                records_written += 1
                
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error on line {line_num}: {e}")
            except KeyError as e:
                logger.error(f"Missing key on line {line_num}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error on line {line_num}: {e}")
    
    # Commit and close
    conn.commit()
    conn.close()
    
    logger.info(f"Conversion completed successfully")
    logger.info(f"Records processed: {records_processed:,}")
    logger.info(f"Records written: {records_written:,}")
    logger.info(f"Output file: {sqlite_path}")
    
    # Verify the database
    verify_sqlite_database(sqlite_path)


def verify_sqlite_database(sqlite_file: str):
    """Verify the created SQLite database has expected structure and content."""
    logger.info("Verifying SQLite database...")
    
    conn = sqlite3.connect(sqlite_file)
    cursor = conn.cursor()
    
    # Check table structure
    cursor.execute("PRAGMA table_info(curie_to_pmids)")
    columns = cursor.fetchall()
    expected_columns = [
        (0, 'curie', 'TEXT', 0, None, 0),
        (1, 'pmids', 'TEXT', 0, None, 0)
    ]
    
    if columns != expected_columns:
        logger.warning("Database structure doesn't match expected format")
        logger.warning(f"Expected: {expected_columns}")
        logger.warning(f"Actual: {columns}")
    else:
        logger.info("✅ Database structure matches input format")
    
    # Check record count
    cursor.execute("SELECT COUNT(*) FROM curie_to_pmids")
    count = cursor.fetchone()[0]
    logger.info(f"✅ Total records in database: {count:,}")
    
    # Show sample records
    cursor.execute("SELECT curie, pmids FROM curie_to_pmids LIMIT 3")
    samples = cursor.fetchall()
    logger.info("📋 Sample records:")
    for curie, pmids_str in samples:
        # Parse the pmids to show count
        try:
            pmids_list = eval(pmids_str)  # Safe since we generated it
            logger.info(f"  {curie}: {len(pmids_list)} PMIDs")
        except:
            logger.info(f"  {curie}: {pmids_str}")
    
    conn.close()
    logger.info("Database verification completed")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Convert cleaned NGD JSONL to SQLite")
    parser.add_argument(
        "jsonl_file", 
        help="Input JSONL file (cleaned NGD output)"
    )
    parser.add_argument(
        "sqlite_file", 
        help="Output SQLite database file"
    )
    
    args = parser.parse_args()
    
    try:
        convert_jsonl_to_sqlite(args.jsonl_file, args.sqlite_file)
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())