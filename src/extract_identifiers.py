#!/usr/bin/env python3
"""
Extract just the identifiers from each cleaned dataset and store in pickle format for fast loading.

This creates lightweight identifier sets that can be quickly loaded for analysis
without parsing the full JSONL files each time.
"""

import json
import pickle
import logging
from pathlib import Path
from typing import Set

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_identifiers_from_jsonl(file_path: Path) -> Set[str]:
    """Extract all CURIEs from a cleaned JSONL file."""
    logger.info(f"Extracting identifiers from {file_path}")
    
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return set()
    
    identifiers = set()
    
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % 1000000 == 0:
                logger.info(f"  Processed {line_num:,} records")
            
            try:
                record = json.loads(line.strip())
                curie = record['curie']
                identifiers.add(curie)
                
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON on line {line_num}")
            except KeyError as e:
                logger.warning(f"Missing key {e} on line {line_num}")
    
    logger.info(f"  Extracted {len(identifiers):,} unique identifiers")
    return identifiers


def main():
    """Extract identifiers from all datasets and save as pickle files."""
    logger.info("Starting identifier extraction")
    
    # Define input/output paths
    datasets = {
        'ngd': {
            'input': Path('cleaned/ngd/ngd_cleaned.jsonl'),
            'output': Path('analysis_results/raw_data_extracts/ngd_identifiers.pkl')
        },
        'pubtator': {
            'input': Path('cleaned/pubtator/pubtator_cleaned.jsonl'),
            'output': Path('analysis_results/raw_data_extracts/pubtator_identifiers.pkl')
        },
        'omnicorp': {
            'input': Path('cleaned/omnicorp/omnicorp_cleaned.jsonl'),
            'output': Path('analysis_results/raw_data_extracts/omnicorp_identifiers.pkl')
        }
    }
    
    # Ensure output directory exists
    output_dir = Path('analysis_results/raw_data_extracts')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Extract and save identifiers for each dataset
    all_identifiers = {}
    
    for dataset_name, paths in datasets.items():
        logger.info(f"\n=== Processing {dataset_name.upper()} ===")
        
        # Extract identifiers
        identifiers = extract_identifiers_from_jsonl(paths['input'])
        all_identifiers[dataset_name] = identifiers
        
        # Save to pickle file
        logger.info(f"Saving {dataset_name} identifiers to {paths['output']}")
        with open(paths['output'], 'wb') as f:
            pickle.dump(identifiers, f)
        
        logger.info(f"Saved {len(identifiers):,} identifiers for {dataset_name}")
    
    # Save combined pickle with all datasets
    combined_output = output_dir / 'all_identifiers.pkl'
    logger.info(f"\nSaving combined identifiers to {combined_output}")
    with open(combined_output, 'wb') as f:
        pickle.dump(all_identifiers, f)
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("IDENTIFIER EXTRACTION SUMMARY")
    logger.info("="*60)
    for dataset_name, identifiers in all_identifiers.items():
        logger.info(f"{dataset_name.upper():>12}: {len(identifiers):>10,} unique identifiers")
    
    total_unique = len(set().union(*all_identifiers.values()))
    logger.info(f"{'TOTAL UNIQUE':>12}: {total_unique:>10,} across all datasets")
    
    logger.info(f"\nPickle files saved in: {output_dir}")
    logger.info("Use these for fast loading in future analyses:")
    logger.info("  import pickle")
    logger.info("  with open('analysis_results/raw_data_extracts/all_identifiers.pkl', 'rb') as f:")
    logger.info("      identifiers = pickle.load(f)")


if __name__ == "__main__":
    main()