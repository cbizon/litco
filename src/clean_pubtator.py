#!/usr/bin/env python3
"""
Clean PubTator data by normalizing CURIEs and converting to JSONLINES format.

This script is now a thin wrapper around the shared sqlite_cleaner module.
It expects the PubTator data to have already been converted to SQLite format
using pubtator_to_sqlite.py.
"""

import logging
from sqlite_cleaner import clean_sqlite_curie_to_pmids

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    input_sqlite = "input/pubtator/pubtator_curie_to_pmids.sqlite"  # Created by pubtator_to_sqlite.py
    output_dir = "cleaned/pubtator"
    
    records_written = clean_sqlite_curie_to_pmids(
        input_sqlite_path=input_sqlite,
        output_dir=output_dir,
        dataset_name="pubtator"
    )
    
    logger.info(f"PubTator cleaning completed: {records_written} records written")


if __name__ == "__main__":
    main()