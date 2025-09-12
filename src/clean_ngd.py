#!/usr/bin/env python3
"""
Clean NGD data by normalizing CURIEs and converting to JSONLINES format.

This script is now a thin wrapper around the shared sqlite_cleaner module.
"""

import logging
from sqlite_cleaner import clean_sqlite_curie_to_pmids

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    input_db = "input/ngd/data_01_RAW_KGs_rtx_kg2_v2.10.0_curie_to_pmids.sqlite"
    output_dir = "cleaned/ngd"
    
    records_written = clean_sqlite_curie_to_pmids(
        input_sqlite_path=input_db,
        output_dir=output_dir,
        dataset_name="ngd"
    )
    
    logger.info(f"NGD cleaning completed: {records_written} records written")


if __name__ == "__main__":
    main()