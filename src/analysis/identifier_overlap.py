#!/usr/bin/env python3
"""
Plan and implementation for analyzing identifier overlap across NGD, PubTator, and OmniCorp.

This script will analyze the normalized identifiers (CURIEs) across all three datasets
to understand:
1. Which identifiers appear in all three datasets (core overlap)
2. Which identifiers appear in exactly two datasets (pairwise overlaps)
3. Which identifiers are unique to each dataset
4. Patterns by biolink type/prefix
5. Coverage statistics and implications
"""

import json
import logging
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, Set, List, Tuple
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class IdentifierOverlapAnalyzer:
    """Analyze identifier overlap patterns across NGD, PubTator, and OmniCorp."""
    
    def __init__(self):
        self.datasets = ['NGD', 'PubTator', 'OmniCorp']
        self.data_paths = {
            'NGD': Path('../../cleaned/ngd/ngd_cleaned.jsonl'),
            'PubTator': Path('../../cleaned/pubtator/pubtator_cleaned.jsonl'),
            'OmniCorp': Path('../../cleaned/omnicorp/omnicorp_cleaned.jsonl')
        }
        
        # Storage for analysis results
        self.identifiers_by_dataset = {}  # dataset_name -> set of CURIEs
        self.curie_details = {}  # curie -> {datasets: [list], prefix: str, total_pmids: int}
        self.biolink_data = {}  # curie -> biolink_types (from biolink classes files)
        
    def load_dataset_identifiers(self, dataset_name: str, file_path: Path) -> Set[str]:
        """Load all normalized CURIEs from a dataset."""
        logger.info(f"Loading identifiers from {dataset_name}: {file_path}")
        
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return set()
        
        identifiers = set()
        pmid_counts = {}
        
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                if line_num % 1000000 == 0:
                    logger.info(f"  Processed {line_num:,} records from {dataset_name}")
                
                try:
                    record = json.loads(line.strip())
                    curie = record['curie']
                    identifiers.add(curie)
                    
                    # Track PMID counts for later analysis
                    pmid_count = len(record.get('publications', []))
                    pmid_counts[curie] = pmid_count
                    
                    # Store details for this CURIE
                    if curie not in self.curie_details:
                        self.curie_details[curie] = {
                            'datasets': [],
                            'prefix': curie.split(':', 1)[0] if ':' in curie else 'NO_PREFIX',
                            'pmid_counts': {}
                        }
                    
                    self.curie_details[curie]['datasets'].append(dataset_name)
                    self.curie_details[curie]['pmid_counts'][dataset_name] = pmid_count
                    
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON on line {line_num} in {dataset_name}")
                except KeyError as e:
                    logger.warning(f"Missing key {e} on line {line_num} in {dataset_name}")
        
        logger.info(f"  Loaded {len(identifiers):,} unique identifiers from {dataset_name}")
        return identifiers
    
    def load_biolink_classes(self):
        """Load biolink type information for CURIEs."""
        logger.info("Loading biolink type classifications")
        
        biolink_files = {
            'NGD': Path('../../cleaned/ngd/ngd_biolink_classes.json'),
            'PubTator': Path('../../cleaned/pubtator/pubtator_biolink_classes.json'),
            'OmniCorp': Path('../../cleaned/omnicorp/omnicorp_biolink_classes.json')
        }
        
        for dataset, file_path in biolink_files.items():
            if not file_path.exists():
                logger.warning(f"Biolink classes file not found: {file_path}")
                continue
                
            logger.info(f"  Loading biolink classes from {dataset}")
            with open(file_path, 'r') as f:
                data = json.load(f)
                
                # Handle different possible structures
                if 'curie_to_classes' in data:
                    curie_classes = data['curie_to_classes']
                else:
                    curie_classes = data
                
                for curie, biolink_types in curie_classes.items():
                    if curie not in self.biolink_data:
                        self.biolink_data[curie] = set()
                    
                    # Add biolink types (might be list or single item)
                    if isinstance(biolink_types, list):
                        self.biolink_data[curie].update(biolink_types)
                    else:
                        self.biolink_data[curie].add(biolink_types)
    
    def analyze_overlaps(self):
        """Compute overlap statistics between datasets."""
        logger.info("Analyzing identifier overlaps")
        
        # Load all datasets
        for dataset_name, file_path in self.data_paths.items():
            self.identifiers_by_dataset[dataset_name] = self.load_dataset_identifiers(dataset_name, file_path)
        
        # Load biolink classifications
        self.load_biolink_classes()
        
        # Compute overlaps
        ngd_ids = self.identifiers_by_dataset['NGD']
        pubtator_ids = self.identifiers_by_dataset['PubTator']
        omnicorp_ids = self.identifiers_by_dataset['OmniCorp']
        
        # All possible intersections
        all_three = ngd_ids & pubtator_ids & omnicorp_ids
        ngd_pubtator_only = (ngd_ids & pubtator_ids) - omnicorp_ids
        ngd_omnicorp_only = (ngd_ids & omnicorp_ids) - pubtator_ids
        pubtator_omnicorp_only = (pubtator_ids & omnicorp_ids) - ngd_ids
        ngd_only = ngd_ids - pubtator_ids - omnicorp_ids
        pubtator_only = pubtator_ids - ngd_ids - omnicorp_ids
        omnicorp_only = omnicorp_ids - ngd_ids - pubtator_ids
        
        # Store overlap results
        self.overlaps = {
            'all_three': all_three,
            'ngd_pubtator_only': ngd_pubtator_only,
            'ngd_omnicorp_only': ngd_omnicorp_only,
            'pubtator_omnicorp_only': pubtator_omnicorp_only,
            'ngd_only': ngd_only,
            'pubtator_only': pubtator_only,
            'omnicorp_only': omnicorp_only
        }
        
        # Compute totals for verification
        total_unique = len(ngd_ids | pubtator_ids | omnicorp_ids)
        computed_total = len(all_three) + len(ngd_pubtator_only) + len(ngd_omnicorp_only) + \
                        len(pubtator_omnicorp_only) + len(ngd_only) + len(pubtator_only) + len(omnicorp_only)
        
        logger.info(f"Total unique identifiers across all datasets: {total_unique:,}")
        logger.info(f"Sum of overlap categories: {computed_total:,}")
        assert total_unique == computed_total, "Overlap calculation error"
    
    def analyze_by_prefix(self):
        """Analyze overlaps by identifier prefix."""
        logger.info("Analyzing overlaps by prefix")
        
        prefix_overlaps = defaultdict(lambda: defaultdict(int))
        
        for overlap_type, curie_set in self.overlaps.items():
            for curie in curie_set:
                prefix = curie.split(':', 1)[0] if ':' in curie else 'NO_PREFIX'
                prefix_overlaps[overlap_type][prefix] += 1
        
        return prefix_overlaps
    
    def analyze_by_biolink_type(self):
        """Analyze overlaps by biolink type."""
        logger.info("Analyzing overlaps by biolink type")
        
        biolink_overlaps = defaultdict(lambda: defaultdict(int))
        
        for overlap_type, curie_set in self.overlaps.items():
            for curie in curie_set:
                biolink_types = self.biolink_data.get(curie, ['Unknown'])
                for biolink_type in biolink_types:
                    biolink_overlaps[overlap_type][biolink_type] += 1
        
        return biolink_overlaps
    
    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        logger.info("Generating summary report")
        
        total_ids = {name: len(ids) for name, ids in self.identifiers_by_dataset.items()}
        
        print("\n" + "="*80)
        print("IDENTIFIER OVERLAP ANALYSIS REPORT")
        print("="*80)
        
        # Dataset sizes
        print(f"\n📊 DATASET SIZES:")
        print("-" * 40)
        for dataset, count in total_ids.items():
            print(f"{dataset:>12}: {count:>10,} unique normalized identifiers")
        
        # Overlap statistics
        print(f"\n🔗 OVERLAP PATTERNS:")
        print("-" * 60)
        print(f"{'Category':<25} {'Count':>12} {'% of Total':>12}")
        print("-" * 60)
        
        total_unique = sum(len(ids) for ids in self.overlaps.values())
        
        categories = [
            ('All three datasets', 'all_three'),
            ('NGD + PubTator only', 'ngd_pubtator_only'),
            ('NGD + OmniCorp only', 'ngd_omnicorp_only'),
            ('PubTator + OmniCorp only', 'pubtator_omnicorp_only'),
            ('NGD only', 'ngd_only'),
            ('PubTator only', 'pubtator_only'),
            ('OmniCorp only', 'omnicorp_only')
        ]
        
        for category_name, category_key in categories:
            count = len(self.overlaps[category_key])
            percentage = (count / total_unique) * 100 if total_unique > 0 else 0
            print(f"{category_name:<25} {count:>12,} {percentage:>11.1f}%")
        
        # Coverage analysis
        print(f"\n📈 COVERAGE ANALYSIS:")
        print("-" * 40)
        
        # What percentage of each dataset's identifiers appear in others?
        for dataset in self.datasets:
            dataset_ids = self.identifiers_by_dataset[dataset]
            in_any_other = len(dataset_ids) - len(self.overlaps[f'{dataset.lower()}_only'])
            coverage = (in_any_other / len(dataset_ids)) * 100 if dataset_ids else 0
            print(f"{dataset} identifiers also in other datasets: {in_any_other:,}/{len(dataset_ids):,} ({coverage:.1f}%)")
    
    def generate_detailed_reports(self, output_dir: Path):
        """Generate detailed CSV reports for further analysis."""
        logger.info(f"Generating detailed reports in {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. Overlap summary CSV
        overlap_summary = []
        for category_name, category_key in [
            ('All_Three', 'all_three'),
            ('NGD_PubTator_Only', 'ngd_pubtator_only'),
            ('NGD_OmniCorp_Only', 'ngd_omnicorp_only'),
            ('PubTator_OmniCorp_Only', 'pubtator_omnicorp_only'),
            ('NGD_Only', 'ngd_only'),
            ('PubTator_Only', 'pubtator_only'),
            ('OmniCorp_Only', 'omnicorp_only')
        ]:
            overlap_summary.append({
                'Category': category_name,
                'Count': len(self.overlaps[category_key])
            })
        
        pd.DataFrame(overlap_summary).to_csv(output_dir / 'overlap_summary.csv', index=False)
        
        # 2. Prefix analysis CSV
        prefix_data = self.analyze_by_prefix()
        prefix_rows = []
        
        for overlap_type, prefix_counts in prefix_data.items():
            for prefix, count in prefix_counts.items():
                prefix_rows.append({
                    'Overlap_Type': overlap_type,
                    'Prefix': prefix,
                    'Count': count
                })
        
        pd.DataFrame(prefix_rows).to_csv(output_dir / 'overlap_by_prefix.csv', index=False)
        
        # 3. Biolink type analysis CSV
        biolink_data = self.analyze_by_biolink_type()
        biolink_rows = []
        
        for overlap_type, biolink_counts in biolink_data.items():
            for biolink_type, count in biolink_counts.items():
                biolink_rows.append({
                    'Overlap_Type': overlap_type,
                    'Biolink_Type': biolink_type,
                    'Count': count
                })
        
        pd.DataFrame(biolink_rows).to_csv(output_dir / 'overlap_by_biolink_type.csv', index=False)
        
        # 4. Sample identifiers from each overlap category
        samples = {}
        for category_key, curie_set in self.overlaps.items():
            # Sample up to 100 identifiers from each category
            sample_size = min(100, len(curie_set))
            samples[category_key] = list(curie_set)[:sample_size]
        
        # Write samples to separate files for inspection
        for category_key, sample_curies in samples.items():
            sample_file = output_dir / f'sample_identifiers_{category_key}.txt'
            with open(sample_file, 'w') as f:
                for curie in sample_curies:
                    f.write(f"{curie}\n")
        
        logger.info(f"Generated detailed reports in {output_dir}")
    
    def run_analysis(self):
        """Run the complete overlap analysis."""
        logger.info("Starting identifier overlap analysis")
        
        # Load data and compute overlaps
        self.analyze_overlaps()
        
        # Generate reports
        self.generate_summary_report()
        
        # Generate detailed CSV reports
        output_dir = Path('../../analysis_results/identifier_overlap')
        self.generate_detailed_reports(output_dir)
        
        logger.info("Identifier overlap analysis complete!")


def main():
    """Main analysis function."""
    analyzer = IdentifierOverlapAnalyzer()
    analyzer.run_analysis()


if __name__ == "__main__":
    main()