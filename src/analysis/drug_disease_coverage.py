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
import logging
from pathlib import Path
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
        
        # Original CURIE coverage
        result_df[f'{dataset}_drug_present'] = result_df[drug_col].isin(dataset_ids)
        result_df[f'{dataset}_disease_present'] = result_df[disease_col].isin(dataset_ids)
        result_df[f'{dataset}_both_present'] = (
            result_df[f'{dataset}_drug_present'] & result_df[f'{dataset}_disease_present']
        )
        
        # Renormalized CURIE coverage (handle NaN values)
        result_df[f'{dataset}_renorm_drug_present'] = result_df['renormalized_drug_id'].fillna('').isin(dataset_ids)
        result_df[f'{dataset}_renorm_disease_present'] = result_df['renormalized_disease_id'].fillna('').isin(dataset_ids)
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
    
    # Original CURIEs coverage
    print(f"\n🔤 ORIGINAL CURIE COVERAGE:")
    print(f"{'Dataset':<12} {'Drug':>8} {'Disease':>8} {'Both':>8} {'Both %':>8}")
    print("-" * 60)
    
    for dataset in datasets:
        drug_count = df[f'{dataset}_drug_present'].sum()
        disease_count = df[f'{dataset}_disease_present'].sum()
        both_count = df[f'{dataset}_both_present'].sum()
        both_pct = (both_count / total_pairs * 100) if total_pairs > 0 else 0
        
        print(f"{dataset.upper():<12} {drug_count:>8,} {disease_count:>8,} {both_count:>8,} {both_pct:>7.1f}%")
    
    # Renormalized CURIEs coverage (only if renormalized columns exist)
    if 'renormalized_drug_id' in df.columns:
        print(f"\n🔄 RENORMALIZED CURIE COVERAGE:")
        print(f"{'Dataset':<12} {'Drug':>8} {'Disease':>8} {'Both':>8} {'Both %':>8}")
        print("-" * 60)
        
        for dataset in datasets:
            drug_count = df[f'{dataset}_renorm_drug_present'].sum()
            disease_count = df[f'{dataset}_renorm_disease_present'].sum()
            both_count = df[f'{dataset}_renorm_both_present'].sum()
            both_pct = (both_count / total_pairs * 100) if total_pairs > 0 else 0
            
            print(f"{dataset.upper():<12} {drug_count:>8,} {disease_count:>8,} {both_count:>8,} {both_pct:>7.1f}%")
    
    # Cross-dataset analysis (use renormalized if available, otherwise original)
    suffix = '_renorm_both_present' if 'renormalized_drug_id' in df.columns else '_both_present'
    coverage_type = "RENORMALIZED" if 'renormalized_drug_id' in df.columns else "ORIGINAL"
    
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
    
    # Improvement analysis (only if renormalized columns exist)
    if 'renormalized_drug_id' in df.columns:
        print(f"\n📈 IMPROVEMENT FROM RENORMALIZATION:")
        print("-" * 50)
        
        for dataset in datasets:
            orig_both = df[f'{dataset}_both_present'].sum()
            renorm_both = df[f'{dataset}_renorm_both_present'].sum()
            improvement = renorm_both - orig_both
            improvement_pct = (improvement / orig_both * 100) if orig_both > 0 else 0
            
            print(f"{dataset.upper():<12}: {orig_both:>6,} → {renorm_both:>6,} (+{improvement:>4,}, {improvement_pct:>+5.1f}%)")


def create_unique_entity_files(all_dfs: list, identifiers: Dict[str, Set[str]], 
                              normalizer: CurieNormalizer):
    """Create files with unique drugs and diseases and their dataset coverage."""
    logger.info("Creating unique entity coverage files")
    
    # Collect all unique drugs and diseases
    all_drugs = set()
    all_diseases = set()
    drug_labels = {}
    disease_labels = {}
    
    for df in all_dfs:
        drug_col = 'final normalized drug id'
        drug_label_col = 'final normalized drug label'
        disease_col = 'final normalized disease id'
        disease_label_col = 'final normalized disease label'
        
        # Collect drug IDs and labels
        for _, row in df.iterrows():
            if pd.notna(row[drug_col]):
                drug_id = row[drug_col]
                all_drugs.add(drug_id)
                if pd.notna(row[drug_label_col]):
                    drug_labels[drug_id] = row[drug_label_col]
        
        # Collect disease IDs and labels
        for _, row in df.iterrows():
            if pd.notna(row[disease_col]):
                disease_id = row[disease_col]
                all_diseases.add(disease_id)
                if pd.notna(row[disease_label_col]):
                    disease_labels[disease_id] = row[disease_label_col]
    
    logger.info(f"Found {len(all_drugs)} unique drugs and {len(all_diseases)} unique diseases")
    
    # Normalize the unique entities
    logger.info("Normalizing unique drugs")
    drug_normalizations = normalizer.normalize_curies(list(all_drugs), {})
    
    logger.info("Normalizing unique diseases")
    disease_normalizations = normalizer.normalize_curies(list(all_diseases), {})
    
    # Create drugs coverage file
    drugs_data = []
    for drug_id in sorted(all_drugs):
        renorm_drug_id = drug_normalizations.get(drug_id)
        row = {
            'drug_id': drug_id,
            'drug_label': drug_labels.get(drug_id, ''),
            'renormalized_drug_id': renorm_drug_id,
            'ngd_present': drug_id in identifiers['ngd'],
            'pubtator_present': drug_id in identifiers['pubtator'],
            'omnicorp_present': drug_id in identifiers['omnicorp'],
            'ngd_renorm_present': renorm_drug_id in identifiers['ngd'] if renorm_drug_id else False,
            'pubtator_renorm_present': renorm_drug_id in identifiers['pubtator'] if renorm_drug_id else False,
            'omnicorp_renorm_present': renorm_drug_id in identifiers['omnicorp'] if renorm_drug_id else False
        }
        drugs_data.append(row)
    
    drugs_df = pd.DataFrame(drugs_data)
    drugs_output = '../../analysis_results/drug_disease_coverage/unique_drugs_coverage.csv'
    logger.info(f"Saving unique drugs coverage to {drugs_output}")
    drugs_df.to_csv(drugs_output, index=False)
    
    # Create diseases coverage file
    diseases_data = []
    for disease_id in sorted(all_diseases):
        renorm_disease_id = disease_normalizations.get(disease_id)
        row = {
            'disease_id': disease_id,
            'disease_label': disease_labels.get(disease_id, ''),
            'renormalized_disease_id': renorm_disease_id,
            'ngd_present': disease_id in identifiers['ngd'],
            'pubtator_present': disease_id in identifiers['pubtator'],
            'omnicorp_present': disease_id in identifiers['omnicorp'],
            'ngd_renorm_present': renorm_disease_id in identifiers['ngd'] if renorm_disease_id else False,
            'pubtator_renorm_present': renorm_disease_id in identifiers['pubtator'] if renorm_disease_id else False,
            'omnicorp_renorm_present': renorm_disease_id in identifiers['omnicorp'] if renorm_disease_id else False
        }
        diseases_data.append(row)
    
    diseases_df = pd.DataFrame(diseases_data)
    diseases_output = '../../analysis_results/drug_disease_coverage/unique_diseases_coverage.csv'
    logger.info(f"Saving unique diseases coverage to {diseases_output}")
    diseases_df.to_csv(diseases_output, index=False)
    
    # Print summary stats for unique entities
    print(f"\n📋 UNIQUE ENTITY COVERAGE SUMMARY")
    print("=" * 60)
    print(f"Unique drugs: {len(all_drugs):,}")
    print(f"Unique diseases: {len(all_diseases):,}")
    
    datasets = ['ngd', 'pubtator', 'omnicorp']
    
    print(f"\n🔬 DRUG COVERAGE BY DATASET (ORIGINAL):")
    print("-" * 50)
    for dataset in datasets:
        count = drugs_df[f'{dataset}_present'].sum()
        pct = (count / len(all_drugs) * 100) if all_drugs else 0
        print(f"{dataset.upper():<12}: {count:>6,}/{len(all_drugs):,} ({pct:>5.1f}%)")
    
    print(f"\n🔬 DRUG COVERAGE BY DATASET (RENORMALIZED):")
    print("-" * 50)
    for dataset in datasets:
        count = drugs_df[f'{dataset}_renorm_present'].sum()
        pct = (count / len(all_drugs) * 100) if all_drugs else 0
        print(f"{dataset.upper():<12}: {count:>6,}/{len(all_drugs):,} ({pct:>5.1f}%)")
    
    print(f"\n🦠 DISEASE COVERAGE BY DATASET (ORIGINAL):")
    print("-" * 50)
    for dataset in datasets:
        count = diseases_df[f'{dataset}_present'].sum()
        pct = (count / len(all_diseases) * 100) if all_diseases else 0
        print(f"{dataset.upper():<12}: {count:>6,}/{len(all_diseases):,} ({pct:>5.1f}%)")
    
    print(f"\n🦠 DISEASE COVERAGE BY DATASET (RENORMALIZED):")
    print("-" * 50)
    for dataset in datasets:
        count = diseases_df[f'{dataset}_renorm_present'].sum()
        pct = (count / len(all_diseases) * 100) if all_diseases else 0
        print(f"{dataset.upper():<12}: {count:>6,}/{len(all_diseases):,} ({pct:>5.1f}%)")


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
    
    # Store all dataframes for unique entity analysis
    all_dfs = []
    
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
        
        # Store original df for unique entity analysis
        all_dfs.append(df.copy())
        
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
        logger.info(f"Saving results to {output_filename}")
        result_df.to_csv(output_filename, index=False)
        
        # Print summary
        print_coverage_summary(result_df, file_type)
    
    # Create unique entity coverage files
    if all_dfs:
        create_unique_entity_files(all_dfs, identifiers, normalizer)
    
    logger.info("\nAnalysis complete!")


if __name__ == "__main__":
    main()