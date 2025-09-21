#!/usr/bin/env python3
"""
MEDI Inspection: Cross-reference MEDI entities with RoboKOP and literature databases.

This script analyzes which MEDI drugs/diseases are:
1. Present/absent in RoboKOP knowledge graph
2. Present/absent in literature databases (NGD, PubTator, OmniCorp)
3. Found/missing in PubMed search results

Provides a comprehensive view of data availability across knowledge sources.
"""

import json
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Set, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_normalized_nodes(robokop_nodefile: str) -> Set[str]:
    """Load node identifiers from RoboKOP nodes file (supports .jsonl and .csv formats)."""
    logger.info(f"Loading RoboKOP nodes from {robokop_nodefile}")
    
    nodes = set()
    
    if robokop_nodefile.endswith('.jsonl'):
        # New format: JSONL with 'id' field
        with open(robokop_nodefile, "r") as inf:
            for line_num, line in enumerate(inf, 1):
                try:
                    node = json.loads(line.strip())
                    node_id = node.get('id')
                    
                    if node_id:
                        nodes.add(node_id)
                    
                    if line_num % 100000 == 0:
                        logger.info(f"  Processed {line_num:,} lines, found {len(nodes):,} unique nodes")
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Error parsing JSON on line {line_num}: {e}")
                except Exception as e:
                    logger.warning(f"Error processing line {line_num}: {e}")
    else:
        # Legacy format: CSV with OriginalCURIE,NormalizedCURIE columns
        with open(robokop_nodefile, "r") as inf:
            header = inf.readline()  # Skip header: OriginalCURIE,NormalizedCURIE
            for line_num, line in enumerate(inf, 1):
                try:
                    parts = line.strip().split(",")
                    if len(parts) >= 2:
                        normalized_curie = parts[1].strip()
                        if normalized_curie:
                            nodes.add(normalized_curie)
                    
                    if line_num % 100000 == 0:
                        logger.info(f"  Processed {line_num:,} lines, found {len(nodes):,} unique nodes")
                        
                except Exception as e:
                    logger.warning(f"Error parsing line {line_num}: {e}")
    
    logger.info(f"Loaded {len(nodes):,} unique node identifiers from RoboKOP")
    return nodes


def load_medi_coverage(identifiers: Dict[str, Set[str]]) -> Tuple[Dict, Dict]:
    """Generate MEDI drug and disease coverage data from source files."""
    logger.info("Generating MEDI coverage data from source files")
    
    # Import the normalization tools we need
    import sys
    sys.path.append('..')
    from normalization import CurieNormalizer
    
    normalizer = CurieNormalizer()
    
    # Process the same files as drug_disease_coverage.py
    input_files = [
        'input/medi/Indications List.csv',
        'input/medi/Contraindications List.csv'
    ]
    
    all_drugs = set()
    all_diseases = set() 
    drug_labels = {}
    disease_labels = {}
    
    for filename in input_files:
        logger.info(f"Processing {filename}")
        df = pd.read_csv(filename)
        
        # Determine column names based on file
        if 'Indications' in filename:
            drug_col = 'final normalized drug id'
            disease_col = 'final normalized disease id'
        else:  # Contraindications
            drug_col = 'final normalized drug id'  
            disease_col = 'final normalized disease id'
            
        drug_label_col = 'final normalized drug label'
        disease_label_col = 'final normalized disease label'
        
        # Remove rows with missing IDs
        df = df.dropna(subset=[drug_col, disease_col])
        
        # Collect unique entities for normalization
        unique_drugs = set(df[drug_col].dropna().unique())
        unique_diseases = set(df[disease_col].dropna().unique())
        
        # Normalize entities
        logger.info(f"Normalizing {len(unique_drugs)} drugs and {len(unique_diseases)} diseases from {filename}")
        drug_normalizations = normalizer.normalize_curies(list(unique_drugs), {})
        disease_normalizations = normalizer.normalize_curies(list(unique_diseases), {})
        
        # Collect normalized entities and labels
        for _, row in df.iterrows():
            if pd.notna(row[drug_col]):
                orig_drug_id = row[drug_col]
                norm_drug_id = drug_normalizations.get(orig_drug_id)
                if norm_drug_id:
                    all_drugs.add(norm_drug_id)
                    if drug_label_col in df.columns and pd.notna(row[drug_label_col]):
                        drug_labels[norm_drug_id] = row[drug_label_col]
                        
            if pd.notna(row[disease_col]):
                orig_disease_id = row[disease_col]
                norm_disease_id = disease_normalizations.get(orig_disease_id)
                if norm_disease_id:
                    all_diseases.add(norm_disease_id)
                    if disease_label_col in df.columns and pd.notna(row[disease_label_col]):
                        disease_labels[norm_disease_id] = row[disease_label_col]
    
    # Build coverage dictionaries
    drugs = {}
    for drug_id in all_drugs:
        drugs[drug_id] = {
            'label': drug_labels.get(drug_id, ''),
            'ngd_renorm_present': drug_id in identifiers['ngd'] if drug_id is not None else False,
            'pubtator_renorm_present': drug_id in identifiers['pubtator'] if drug_id is not None else False,
            'omnicorp_renorm_present': drug_id in identifiers['omnicorp'] if drug_id is not None else False
        }
    
    diseases = {}
    for disease_id in all_diseases:
        diseases[disease_id] = {
            'label': disease_labels.get(disease_id, ''),
            'ngd_renorm_present': disease_id in identifiers['ngd'] if disease_id is not None else False,
            'pubtator_renorm_present': disease_id in identifiers['pubtator'] if disease_id is not None else False,
            'omnicorp_renorm_present': disease_id in identifiers['omnicorp'] if disease_id is not None else False
        }
    
    logger.info(f"Generated coverage for {len(drugs)} drugs and {len(diseases)} diseases")
    return drugs, diseases


def load_missing_entity_analysis(missing_entity_file: str) -> Dict:
    """Load missing entity analysis results."""
    logger.info(f"Loading missing entity analysis from {missing_entity_file}")
    
    if not Path(missing_entity_file).exists():
        logger.warning(f"Missing entity file not found: {missing_entity_file}")
        return {}
    
    with open(missing_entity_file, "r") as inf:
        entity_analysis = json.load(inf)
    
    logger.info(f"Loaded missing entity analysis with {len(entity_analysis.get('entity_gaps', []))} analyzed entities")
    return entity_analysis


def cross_reference_with_robokop(entities: Dict, robokop_nodes: Set[str], entity_type: str) -> Dict:
    """Cross-reference MEDI entities with RoboKOP presence."""
    logger.info(f"Cross-referencing {len(entities)} {entity_type} with RoboKOP")
    
    results = {}
    in_robokop = 0
    
    for entity_id, entity_data in entities.items():
        # If we have a renormalized ID, use that; otherwise use original ID
        renorm_id = entity_data.get('renormalized_id')
        id_to_check = renorm_id if renorm_id else entity_id
        
        # Check if the ID (renormalized or original) is in RoboKOP
        robokop_present = id_to_check in robokop_nodes
        
        if robokop_present:
            in_robokop += 1
        
        results[entity_id] = {
            'label': entity_data['label'],
            'ngd_renorm_present': entity_data['ngd_renorm_present'],
            'pubtator_renorm_present': entity_data['pubtator_renorm_present'],
            'omnicorp_renorm_present': entity_data['omnicorp_renorm_present'],
            'robokop_renorm_present': robokop_present
        }
    
    logger.info(f"  {in_robokop}/{len(entities)} {entity_type} found in RoboKOP ({in_robokop/len(entities)*100:.1f}%)")
    return results


def generate_summary_report(drugs: Dict, diseases: Dict, output_dir: str):
    """Generate comprehensive summary report."""
    logger.info("Generating summary report")
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Overall statistics
    total_drugs = len(drugs)
    total_diseases = len(diseases)
    
    # RoboKOP presence
    drugs_in_robokop = sum(1 for d in drugs.values() if d['robokop_renorm_present'])
    diseases_in_robokop = sum(1 for d in diseases.values() if d['robokop_renorm_present'])
    
    # Literature database presence (any database)
    drugs_in_lit = sum(1 for d in drugs.values() if d['ngd_renorm_present'] or d['pubtator_renorm_present'] or d['omnicorp_renorm_present'])
    diseases_in_lit = sum(1 for d in diseases.values() if d['ngd_renorm_present'] or d['pubtator_renorm_present'] or d['omnicorp_renorm_present'])
    
    # Both RoboKOP and literature
    drugs_in_both = sum(1 for d in drugs.values() if d['robokop_renorm_present'] and (d['ngd_renorm_present'] or d['pubtator_renorm_present'] or d['omnicorp_renorm_present']))
    diseases_in_both = sum(1 for d in diseases.values() if d['robokop_renorm_present'] and (d['ngd_renorm_present'] or d['pubtator_renorm_present'] or d['omnicorp_renorm_present']))
    
    # Neither source
    drugs_in_neither = sum(1 for d in drugs.values() if not d['robokop_renorm_present'] and not (d['ngd_renorm_present'] or d['pubtator_renorm_present'] or d['omnicorp_renorm_present']))
    diseases_in_neither = sum(1 for d in diseases.values() if not d['robokop_renorm_present'] and not (d['ngd_renorm_present'] or d['pubtator_renorm_present'] or d['omnicorp_renorm_present']))
    
    # Generate report
    report_lines = [
        "=" * 80,
        "MEDI INSPECTION SUMMARY REPORT", 
        "=" * 80,
        "",
        "📊 OVERALL STATISTICS:",
        f"  Total MEDI drugs:     {total_drugs:,}",
        f"  Total MEDI diseases:  {total_diseases:,}",
        f"  Total MEDI entities:  {total_drugs + total_diseases:,}",
        "",
        "🔍 ROBOKOP KNOWLEDGE GRAPH PRESENCE:",
        f"  Drugs in RoboKOP:     {drugs_in_robokop:,}/{total_drugs:,} ({drugs_in_robokop/total_drugs*100:.1f}%)",
        f"  Diseases in RoboKOP:  {diseases_in_robokop:,}/{total_diseases:,} ({diseases_in_robokop/total_diseases*100:.1f}%)",
        "",
        "📚 LITERATURE DATABASE PRESENCE:",
        f"  Drugs in literature:     {drugs_in_lit:,}/{total_drugs:,} ({drugs_in_lit/total_drugs*100:.1f}%)",
        f"  Diseases in literature:  {diseases_in_lit:,}/{total_diseases:,} ({diseases_in_lit/total_diseases*100:.1f}%)",
        "",
        "🔗 CROSS-SOURCE COVERAGE:",
        f"  Drugs in both sources:     {drugs_in_both:,}/{total_drugs:,} ({drugs_in_both/total_drugs*100:.1f}%)",
        f"  Diseases in both sources:  {diseases_in_both:,}/{total_diseases:,} ({diseases_in_both/total_diseases*100:.1f}%)",
        "",
        "❌ MISSING FROM ALL SOURCES:",
        f"  Drugs missing everywhere:     {drugs_in_neither:,}/{total_drugs:,} ({drugs_in_neither/total_drugs*100:.1f}%)",
        f"  Diseases missing everywhere:  {diseases_in_neither:,}/{total_diseases:,} ({diseases_in_neither/total_diseases*100:.1f}%)",
        "",
        "=" * 80
    ]
    
    # Save text report
    report_file = output_path / 'medi_inspection_summary.txt'
    with open(report_file, 'w') as f:
        f.write('\n'.join(report_lines))
    
    # Print to console
    print('\n'.join(report_lines))
    
    logger.info(f"Summary report saved to {report_file}")


def save_detailed_results(drugs: Dict, diseases: Dict, output_dir: str):
    """Save detailed results to CSV files."""
    logger.info("Saving detailed results")
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Convert to DataFrames and save
    drugs_df = pd.DataFrame.from_dict(drugs, orient='index')
    drugs_df.index.name = 'drug_id'
    drugs_df.to_csv(output_path / 'medi_drugs_robokop_comparison.csv')
    
    diseases_df = pd.DataFrame.from_dict(diseases, orient='index') 
    diseases_df.index.name = 'disease_id'
    diseases_df.to_csv(output_path / 'medi_diseases_robokop_comparison.csv')
    
    logger.info(f"Detailed results saved to {output_path}")


def main():
    """Main function to run MEDI inspection analysis."""
    logger.info("Starting MEDI inspection analysis")
    
    # File paths
    robokop_nodefile = "input/robokop/nodes.jsonl"  # New format
    robokop_nodefile_legacy = "analysis_results/raw_data_extracts/normalized_robokop_nodes.csv"  # Legacy format
    missing_entity_file = "analysis_results/missing_entities/gap_analysis_report.json"
    output_dir = "analysis_results/medi_inspection"
    
    # Load data - try new format first, fallback to legacy
    from pathlib import Path
    if Path(robokop_nodefile).exists():
        robokop_nodes = load_normalized_nodes(robokop_nodefile)
    elif Path(robokop_nodefile_legacy).exists():
        logger.info(f"New nodes.jsonl not found, using legacy format: {robokop_nodefile_legacy}")
        robokop_nodes = load_normalized_nodes(robokop_nodefile_legacy)
    else:
        logger.error(f"No RoboKOP nodes file found. Expected: {robokop_nodefile} or {robokop_nodefile_legacy}")
        return
    # Load identifier sets for coverage analysis
    logger.info("Loading identifier sets")
    import pickle
    identifiers = {}
    identifiers_path = Path("../../analysis_results/raw_data_extracts")
    
    for dataset in ['ngd', 'pubtator', 'omnicorp']:
        pickle_file = identifiers_path / f"{dataset}_identifiers.pkl"
        if pickle_file.exists():
            with open(pickle_file, 'rb') as f:
                identifiers[dataset] = pickle.load(f)
            logger.info(f"  {dataset.upper()}: {len(identifiers[dataset]):,} identifiers")
        else:
            logger.error(f"Identifier file not found: {pickle_file}")
            return
    
    drugs, diseases = load_medi_coverage(identifiers)
    # missing_analysis = load_missing_entity_analysis(missing_entity_file)  # For future use
    
    # Cross-reference with RoboKOP
    drugs_with_robokop = cross_reference_with_robokop(drugs, robokop_nodes, "drugs")
    diseases_with_robokop = cross_reference_with_robokop(diseases, robokop_nodes, "diseases")
    
    # Generate reports
    generate_summary_report(drugs_with_robokop, diseases_with_robokop, output_dir)
    save_detailed_results(drugs_with_robokop, diseases_with_robokop, output_dir)
    
    logger.info("MEDI inspection analysis complete!")
    logger.info(f"Results saved in: {output_dir}")


if __name__ == "__main__":
    main()