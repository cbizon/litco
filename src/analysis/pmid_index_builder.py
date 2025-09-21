#!/usr/bin/env python3
"""
Build reverse PMID indexes for efficient entity lookup.

This module creates reverse indexes mapping PMIDs to entities
for fast lookup during missing entity analysis.
"""

import json
import pickle
import logging
from pathlib import Path
from typing import Dict, Set
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def build_pmid_index_for_dataset(dataset_name: str, jsonl_path: Path) -> Dict[str, Set[str]]:
    """
    Build a reverse PMID index for a dataset.
    
    Args:
        dataset_name: Name of the dataset
        jsonl_path: Path to the cleaned JSONL file
        
    Returns:
        Dictionary mapping PMID -> set of CURIEs
    """
    logger.info(f"Building PMID index for {dataset_name} from {jsonl_path}")
    
    if not jsonl_path.exists():
        logger.warning(f"JSONL file not found: {jsonl_path}")
        return {}
    
    pmid_index = defaultdict(set)
    
    with open(jsonl_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % 1000000 == 0:
                logger.info(f"  Processed {line_num:,} records from {dataset_name}")
            
            try:
                record = json.loads(line.strip())
                curie = record['curie']
                publications = record.get('publications', [])
                
                # Add this CURIE to each PMID's set
                for pub in publications:
                    # Remove PMID: prefix to get just the number
                    pmid = pub.replace("PMID:", "")
                    pmid_index[pmid].add(curie)
                    
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON on line {line_num} in {dataset_name}")
            except KeyError as e:
                logger.warning(f"Missing key {e} on line {line_num} in {dataset_name}")
    
    # Convert defaultdict to regular dict with sets converted to lists for JSON serialization
    pmid_index = {pmid: list(curies) for pmid, curies in pmid_index.items()}
    
    logger.info(f"Built index for {dataset_name}: {len(pmid_index):,} PMIDs")
    return pmid_index


def build_all_pmid_indexes():
    """Build PMID indexes for all datasets."""
    logger.info("Building PMID indexes for all datasets")
    
    datasets = {
        'NGD': Path('cleaned/ngd/ngd_cleaned.jsonl'),
        'PubTator': Path('cleaned/pubtator/pubtator_cleaned.jsonl'),
        'OmniCorp': Path('cleaned/omnicorp/omnicorp_cleaned.jsonl')
    }
    
    output_dir = Path('../../analysis_results/pmid_indexes')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    all_indexes = {}
    
    for dataset_name, jsonl_path in datasets.items():
        logger.info(f"\n=== Building index for {dataset_name} ===")
        
        pmid_index = build_pmid_index_for_dataset(dataset_name, jsonl_path)
        all_indexes[dataset_name] = pmid_index
        
        # Save individual index
        index_output = output_dir / f'{dataset_name.lower()}_pmid_index.pkl'
        with open(index_output, 'wb') as f:
            pickle.dump(pmid_index, f)
        
        logger.info(f"Saved {dataset_name} PMID index to {index_output}")
    
    # Save combined index
    combined_output = output_dir / 'all_pmid_indexes.pkl'
    with open(combined_output, 'wb') as f:
        pickle.dump(all_indexes, f)
    
    logger.info(f"Saved combined PMID indexes to {combined_output}")
    
    # Generate summary
    logger.info("\n" + "="*60)
    logger.info("PMID INDEX SUMMARY")
    logger.info("="*60)
    for dataset, index in all_indexes.items():
        logger.info(f"{dataset:>12}: {len(index):>10,} PMIDs indexed")
    
    # Count total unique PMIDs across all datasets
    all_pmids = set()
    for index in all_indexes.values():
        all_pmids.update(index.keys())
    
    logger.info(f"{'TOTAL UNIQUE':>12}: {len(all_pmids):>10,} PMIDs across all datasets")
    logger.info(f"\nIndexes saved in: {output_dir}")
    
    return all_indexes


def main():
    """Main function to build PMID indexes."""
    build_all_pmid_indexes()


if __name__ == "__main__":
    main()