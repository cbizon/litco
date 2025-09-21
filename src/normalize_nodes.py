#!/usr/bin/env python3
"""
Batch normalize node IDs from a nodes.jsonl file using existing CurieNormalizer.

Reads nodes.jsonl, extracts 'id' fields, normalizes via existing normalization code,
and outputs a 2-column CSV: OriginalCURIE | NormalizedCURIE
"""

import json
import csv
import logging
import argparse
from pathlib import Path

from normalization import CurieNormalizer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_node_ids(input_file: Path) -> list[str]:
    """Extract unique node IDs from nodes.jsonl file."""
    logger.info(f"Extracting node IDs from {input_file}...")
    
    node_ids = []
    seen_ids = set()
    
    with open(input_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            try:
                node = json.loads(line.strip())
                node_id = node.get('id')
                
                if node_id and node_id not in seen_ids:
                    node_ids.append(node_id)
                    seen_ids.add(node_id)
                
                if line_num % 100000 == 0:
                    logger.info(f"  Processed {line_num:,} lines, found {len(node_ids):,} unique IDs")
            
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON on line {line_num}")
    
    logger.info(f"Found {len(node_ids):,} unique node IDs")
    return node_ids


def normalize_and_save(node_ids: list[str], output_file: Path, batch_size: int = 1000):
    """Normalize node IDs and save to CSV."""
    logger.info(f"Normalizing {len(node_ids):,} node IDs in batches of {batch_size}")
    
    normalizer = CurieNormalizer()
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['OriginalCURIE', 'NormalizedCURIE'])
        
        total_batches = (len(node_ids) + batch_size - 1) // batch_size
        successful = 0
        
        for i in range(0, len(node_ids), batch_size):
            batch = node_ids[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} IDs)")
            
            # Create fake pmid_data for normalize_curies method
            fake_pmid_data = {curie: [1] for curie in batch}
            
            # Normalize batch
            normalized_mapping = normalizer.normalize_curies(batch, fake_pmid_data)
            
            # Write results
            for original_curie in batch:
                normalized_curie = normalized_mapping.get(original_curie, original_curie)
                writer.writerow([original_curie, normalized_curie])
                
                if normalized_curie != original_curie:
                    successful += 1
        
        logger.info(f"Normalization complete! {successful:,}/{len(node_ids):,} successfully normalized")


def main():
    parser = argparse.ArgumentParser(description="Normalize node IDs from nodes.jsonl")
    parser.add_argument("input_file", help="Path to nodes.jsonl file")
    parser.add_argument("output_file", help="Output CSV file path")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size (default: 1000)")
    
    args = parser.parse_args()
    
    input_file = Path(args.input_file)
    output_file = Path(args.output_file)
    
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return
    
    # Create output directory
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Extract node IDs
    node_ids = extract_node_ids(input_file)
    
    # Normalize and save
    normalize_and_save(node_ids, output_file, args.batch_size)
    
    logger.info(f"Results saved to: {output_file}")


if __name__ == "__main__":
    main()