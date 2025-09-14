#!/usr/bin/env python3
"""
Clean OmniCorp data by normalizing CURIEs and converting to JSONLINES format.

This script is now a thin wrapper around the shared sqlite_cleaner module.
It expects the OmniCorp data to have already been converted to SQLite format
using omnicorp_to_sqlite.py.
"""

import logging
from sqlite_cleaner import clean_sqlite_curie_to_pmids

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main entry point."""
    input_sqlite = "input/omnicorp/omnicorp_curie_to_pmids.sqlite"  # Created by omnicorp_to_sqlite.py
    output_dir = "cleaned/omnicorp"
    
    records_written = clean_sqlite_curie_to_pmids(
        input_sqlite_path=input_sqlite,
        output_dir=output_dir,
        dataset_name="omnicorp"
    )
    
    logger.info(f"OmniCorp cleaning completed: {records_written} records written")


if __name__ == "__main__":
    main()