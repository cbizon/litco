#!/usr/bin/env python3
"""
Analyze failed normalizations across NGD, PubTator, and OmniCorp datasets.

This script reads the failed normalization files from each dataset and reports
the count of failed CURIEs by prefix for comparison across data sources.
"""

import logging
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_prefix(curie: str) -> str:
    """Extract the prefix from a CURIE (everything before the colon)."""
    if ':' in curie:
        return curie.split(':', 1)[0]
    else:
        # Handle cases where there's no colon (shouldn't happen in proper CURIEs)
        return 'NO_PREFIX'


def analyze_failed_file(file_path: Path) -> Dict[str, int]:
    """Analyze a single failed normalizations file and return prefix counts."""
    if not file_path.exists():
        logger.warning(f"Failed normalization file does not exist: {file_path}")
        return {}
    
    prefix_counts = defaultdict(int)
    total_lines = 0
    
    logger.info(f"Analyzing {file_path}")
    
    with open(file_path, 'r') as f:
        for line in f:
            curie = line.strip()
            if curie:  # Skip empty lines
                prefix = extract_prefix(curie)
                prefix_counts[prefix] += 1
                total_lines += 1
    
    logger.info(f"  Found {total_lines:,} failed CURIEs with {len(prefix_counts)} unique prefixes")
    return dict(prefix_counts)


def analyze_all_datasets() -> Dict[str, Dict[str, int]]:
    """Analyze failed normalizations from all three datasets."""
    logger.info("Analyzing failed normalizations across all datasets")
    
    # Define paths to failed normalization files
    failed_files = {
        'NGD': Path('../../cleaned/ngd/ngd_failed_normalizations.txt'),
        'PubTator': Path('../../cleaned/pubtator/pubtator_failed_normalizations.txt'),
        'OmniCorp': Path('../../cleaned/omnicorp/omnicorp_failed_normalizations.txt')
    }
    
    results = {}
    
    for dataset_name, file_path in failed_files.items():
        logger.info(f"\n=== Analyzing {dataset_name} ===")
        results[dataset_name] = analyze_failed_file(file_path)
    
    return results


def print_summary_report(results: Dict[str, Dict[str, int]]):
    """Print a comprehensive summary report of failed normalizations."""
    print("\n" + "="*80)
    print("FAILED NORMALIZATION ANALYSIS REPORT")
    print("="*80)
    
    # Overall statistics
    print("\n📊 OVERALL STATISTICS:")
    print("-" * 40)
    for dataset, prefix_counts in results.items():
        total_failed = sum(prefix_counts.values())
        unique_prefixes = len(prefix_counts)
        print(f"{dataset:>12}: {total_failed:>10,} failed CURIEs, {unique_prefixes:>3} unique prefixes")
    
    # Collect all unique prefixes across datasets
    all_prefixes = set()
    for prefix_counts in results.values():
        all_prefixes.update(prefix_counts.keys())
    
    print(f"\n🏷️  UNIQUE PREFIXES ACROSS ALL DATASETS: {len(all_prefixes)}")
    
    # Sort prefixes by total failures across all datasets
    prefix_totals = defaultdict(int)
    for prefix_counts in results.values():
        for prefix, count in prefix_counts.items():
            prefix_totals[prefix] += count
    
    sorted_prefixes = sorted(prefix_totals.items(), key=lambda x: x[1], reverse=True)
    
    print("\n📋 FAILED NORMALIZATIONS BY PREFIX (sorted by total failures):")
    print("-" * 80)
    print(f"{'Prefix':<20} {'NGD':>12} {'PubTator':>12} {'OmniCorp':>12} {'Total':>12}")
    print("-" * 80)
    
    for prefix, total_count in sorted_prefixes:
        ngd_count = results.get('NGD', {}).get(prefix, 0)
        pubtator_count = results.get('PubTator', {}).get(prefix, 0)
        omnicorp_count = results.get('OmniCorp', {}).get(prefix, 0)
        
        print(f"{prefix:<20} {ngd_count:>12,} {pubtator_count:>12,} {omnicorp_count:>12,} {total_count:>12,}")
    
    # Dataset-specific top failures
    print("\n🎯 TOP 10 FAILED PREFIXES BY DATASET:")
    print("-" * 60)
    
    for dataset, prefix_counts in results.items():
        print(f"\n{dataset}:")
        top_prefixes = sorted(prefix_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        for i, (prefix, count) in enumerate(top_prefixes, 1):
            percentage = (count / sum(prefix_counts.values())) * 100
            print(f"  {i:>2}. {prefix:<15} {count:>10,} ({percentage:>5.1f}%)")
    
    # Prefixes that appear in multiple datasets
    datasets_per_prefix = defaultdict(set)
    for dataset, prefix_counts in results.items():
        for prefix in prefix_counts.keys():
            datasets_per_prefix[prefix].add(dataset)
    
    multi_dataset_prefixes = {prefix: datasets for prefix, datasets in datasets_per_prefix.items() 
                             if len(datasets) > 1}
    
    if multi_dataset_prefixes:
        print(f"\n🔗 PREFIXES FAILING IN MULTIPLE DATASETS ({len(multi_dataset_prefixes)} total):")
        print("-" * 60)
        
        # Sort by how many datasets they appear in, then by total count
        sorted_multi = sorted(multi_dataset_prefixes.items(), 
                            key=lambda x: (len(x[1]), prefix_totals[x[0]]), 
                            reverse=True)
        
        for prefix, datasets in sorted_multi:
            dataset_list = ", ".join(sorted(datasets))
            total = prefix_totals[prefix]
            print(f"  {prefix:<15} in {len(datasets)} datasets ({dataset_list:>25}) - {total:>8,} total")
    
    print("\n" + "="*80)


def write_detailed_csv_report(results: Dict[str, Dict[str, int]], output_path: Path):
    """Write a detailed CSV report for further analysis."""
    import csv
    
    # Collect all unique prefixes
    all_prefixes = set()
    for prefix_counts in results.values():
        all_prefixes.update(prefix_counts.keys())
    
    sorted_prefixes = sorted(all_prefixes)
    
    logger.info(f"Writing detailed CSV report to {output_path}")
    
    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Header
        writer.writerow(['Prefix', 'NGD', 'PubTator', 'OmniCorp', 'Total'])
        
        # Data rows
        for prefix in sorted_prefixes:
            ngd_count = results.get('NGD', {}).get(prefix, 0)
            pubtator_count = results.get('PubTator', {}).get(prefix, 0)
            omnicorp_count = results.get('OmniCorp', {}).get(prefix, 0)
            total_count = ngd_count + pubtator_count + omnicorp_count
            
            writer.writerow([prefix, ngd_count, pubtator_count, omnicorp_count, total_count])
    
    logger.info(f"CSV report written with {len(sorted_prefixes)} prefixes")


def main():
    """Main analysis function."""
    logger.info("Starting failed normalization analysis")
    
    # Analyze all datasets
    results = analyze_all_datasets()
    
    # Print summary report
    print_summary_report(results)
    
    # Write detailed CSV for further analysis
    output_dir = Path('../../analysis_results/failed_normalizations')
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_output = output_dir / 'failed_normalizations_by_prefix.csv'
    write_detailed_csv_report(results, csv_output)
    
    logger.info("Analysis complete!")


if __name__ == "__main__":
    main()