#!/usr/bin/env python3
"""
Analyze coverage of drug-disease pairs from Indications and Contraindications lists
across NGD, PubTator, and OmniCorp datasets.

Adds columns for each dataset showing:
- Whether drug ID is present
- Whether disease ID is present  
- Whether both drug and disease IDs are present

Outputs summary statistics about coverage fractions.
"""

import pickle
import pandas as pd
from pathlib import Path
import logging
from typing import Dict, Set, List, Optional
import sys
sys.path.append('..')
from normalization import CurieNormalizer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)




def load_identifiers() -> Dict[str, Set[str]]:
    """Load the pre-extracted identifier sets from pickle files."""
    logger.info("Loading identifier sets from pickle files")
    
    pickle_path = Path('../../analysis_results/raw_data_extracts/all_identifiers.pkl')
    if not pickle_path.exists():
        logger.error(f"Pickle file not found: {pickle_path}")
        logger.error("Run 'uv run python src/extract_identifiers.py' first")
        raise FileNotFoundError(f"Missing pickle file: {pickle_path}")
    
    with open(pickle_path, 'rb') as f:
        identifiers = pickle.load(f)
    
    logger.info("Loaded identifier sets:")
    for dataset, ids in identifiers.items():
        logger.info(f"  {dataset.upper()}: {len(ids):,} identifiers")
    
    return identifiers


def normalize_entities_in_df(df: pd.DataFrame, drug_col: str, disease_col: str, 
                            normalizer: CurieNormalizer) -> pd.DataFrame:
    """Add renormalized columns to the dataframe."""
    logger.info("Normalizing entities in dataframe")
    
    # Create a copy to avoid modifying the original
    result_df = df.copy()
    
    # Collect all unique CURIEs to normalize
    all_drug_curies = set(df[drug_col].dropna().unique())
    all_disease_curies = set(df[disease_col].dropna().unique())
    
    logger.info(f"Found {len(all_drug_curies)} unique drug CURIEs and {len(all_disease_curies)} unique disease CURIEs")
    
    # Normalize drugs (pmid_data only used for failure tracking, so pass empty dict)
    logger.info("Normalizing drug CURIEs")
    drug_normalizations = normalizer.normalize_curies(list(all_drug_curies), {})
    
    # Normalize diseases
    logger.info("Normalizing disease CURIEs")
    disease_normalizations = normalizer.normalize_curies(list(all_disease_curies), {})
    
    # Add renormalized columns
    result_df['renormalized_drug_id'] = result_df[drug_col].map(drug_normalizations)
    result_df['renormalized_disease_id'] = result_df[disease_col].map(disease_normalizations)
    
    return result_df


def analyze_coverage(df: pd.DataFrame, identifiers: Dict[str, Set[str]], 
                    drug_col: str, disease_col: str) -> pd.DataFrame:
    """Add coverage columns to the dataframe for both original and renormalized CURIEs."""
    logger.info(f"Analyzing coverage for {len(df)} drug-disease pairs")
    
    # Create a copy to avoid modifying the original
    result_df = df.copy()
    
    datasets = ['ngd', 'pubtator', 'omnicorp']
    
    for dataset in datasets:
        dataset_ids = identifiers[dataset]
        
        # Renormalized CURIE coverage (only check if normalization succeeded)
        result_df[f'{dataset}_renorm_drug_present'] = (
            result_df['renormalized_drug_id'].notna() & 
            result_df['renormalized_drug_id'].isin(dataset_ids)
        )
        result_df[f'{dataset}_renorm_disease_present'] = (
            result_df['renormalized_disease_id'].notna() & 
            result_df['renormalized_disease_id'].isin(dataset_ids)
        )
        result_df[f'{dataset}_renorm_both_present'] = (
            result_df[f'{dataset}_renorm_drug_present'] & result_df[f'{dataset}_renorm_disease_present']
        )
    
    return result_df


def print_coverage_summary(df: pd.DataFrame, file_type: str):
    """Print summary statistics about coverage."""
    total_pairs = len(df)
    
    print(f"\n📊 {file_type.upper()} COVERAGE SUMMARY")
    print("=" * 60)
    print(f"Total drug-disease pairs: {total_pairs:,}")
    
    datasets = ['ngd', 'pubtator', 'omnicorp']
    
    # Normalized CURIEs coverage 
    print(f"\n🔄 NORMALIZED CURIE COVERAGE:")
    print(f"{'Dataset':<12} {'Drug':>8} {'Disease':>8} {'Both':>8} {'Both %':>8}")
    print("-" * 60)
    
    for dataset in datasets:
        drug_count = df[f'{dataset}_renorm_drug_present'].sum()
        disease_count = df[f'{dataset}_renorm_disease_present'].sum()
        both_count = df[f'{dataset}_renorm_both_present'].sum()
        both_pct = (both_count / total_pairs * 100) if total_pairs > 0 else 0
        
        print(f"{dataset.upper():<12} {drug_count:>8,} {disease_count:>8,} {both_count:>8,} {both_pct:>7.1f}%")
    
    # Cross-dataset analysis (normalized data only)
    suffix = '_renorm_both_present'
    coverage_type = "NORMALIZED"
    
    print(f"\n🔗 CROSS-DATASET COVERAGE ({coverage_type}):")
    print("-" * 50)
    
    # Pairs covered by at least one dataset
    any_coverage = (
        df[f'ngd{suffix}'] | 
        df[f'pubtator{suffix}'] | 
        df[f'omnicorp{suffix}']
    )
    any_count = any_coverage.sum()
    any_pct = (any_count / total_pairs * 100) if total_pairs > 0 else 0
    print(f"At least one dataset: {any_count:,}/{total_pairs:,} ({any_pct:.1f}%)")
    
    # Pairs covered by all datasets
    all_coverage = (
        df[f'ngd{suffix}'] & 
        df[f'pubtator{suffix}'] & 
        df[f'omnicorp{suffix}']
    )
    all_count = all_coverage.sum()
    all_pct = (all_count / total_pairs * 100) if total_pairs > 0 else 0
    print(f"All three datasets: {all_count:,}/{total_pairs:,} ({all_pct:.1f}%)")
    
    # Unique coverage by dataset
    print(f"\n🎯 UNIQUE COVERAGE ({coverage_type}, only in this dataset):")
    print("-" * 60)
    
    ngd_only = df[f'ngd{suffix}'] & ~df[f'pubtator{suffix}'] & ~df[f'omnicorp{suffix}']
    pubtator_only = ~df[f'ngd{suffix}'] & df[f'pubtator{suffix}'] & ~df[f'omnicorp{suffix}']
    omnicorp_only = ~df[f'ngd{suffix}'] & ~df[f'pubtator{suffix}'] & df[f'omnicorp{suffix}']
    
    print(f"NGD only: {ngd_only.sum():,} pairs")
    print(f"PubTator only: {pubtator_only.sum():,} pairs")
    print(f"OmniCorp only: {omnicorp_only.sum():,} pairs")
    



def main():
    """Main analysis function."""
    logger.info("Starting drug-disease coverage analysis with normalization")
    
    # Load identifier sets
    try:
        identifiers = load_identifiers()
    except FileNotFoundError:
        return
    
    # Initialize normalizer
    normalizer = CurieNormalizer()
    
    # Define input files
    input_files = {
        'indications': '../../Indications List.csv',
        'contraindications': '../../Contraindications List.csv'
    }
    
    
    # Process each file
    for file_type, filename in input_files.items():
        logger.info(f"\n=== Processing {file_type} ===")
        
        file_path = Path(filename)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            continue
        
        # Load the CSV
        logger.info(f"Loading {filename}")
        df = pd.read_csv(file_path)
        
        # Determine column names based on file type
        if file_type == 'indications':
            drug_col = 'final normalized drug id'
            disease_col = 'final normalized disease id'
        else:  # contraindications
            drug_col = 'final normalized drug id'
            disease_col = 'final normalized disease id'
        
        # Verify columns exist
        if drug_col not in df.columns or disease_col not in df.columns:
            logger.error(f"Missing required columns in {filename}")
            logger.error(f"Expected: {drug_col}, {disease_col}")
            logger.error(f"Found: {list(df.columns)}")
            continue
        
        # Remove rows with missing drug or disease IDs
        original_len = len(df)
        df = df.dropna(subset=[drug_col, disease_col])
        if len(df) < original_len:
            logger.info(f"Removed {original_len - len(df)} rows with missing IDs")
        
        # Normalize entities in the dataframe
        df = normalize_entities_in_df(df, drug_col, disease_col, normalizer)
        
        # Analyze coverage (now includes both original and renormalized)
        result_df = analyze_coverage(df, identifiers, drug_col, disease_col)
        
        
        # Save results
        output_filename = f"../../analysis_results/drug_disease_coverage/{file_type}_coverage_analysis.csv"
        
        # Create output directory if it doesn't exist
        Path(output_filename).parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Saving results to {output_filename}")
        result_df.to_csv(output_filename, index=False)
        
        # Print summary
        print_coverage_summary(result_df, file_type)
    
    logger.info("\nAnalysis complete!")
    logger.info("For detailed entity coverage with RoboKOP data, run: python medi_inspection.py")


if __name__ == "__main__":
    main()