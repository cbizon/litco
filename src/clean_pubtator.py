#!/usr/bin/env python3
"""
Clean PubTator data by normalizing CURIEs and converting to JSONLINES format.

This script:
1. Processes gzipped PubTator TSV file with PMID, Type, Concept ID columns
2. Converts bare numbers to proper CURIEs based on Type context
3. Filters out invalid concept IDs (- entries)
4. Normalizes CURIEs using the node normalizer API
5. Handles merging when normalized CURIEs collapse to the same identifier
6. Outputs cleaned data as JSONLINES with 'publications' key
7. Tracks normalization failures to identify CURIE construction issues
"""

import gzip
import logging
import json
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set

from normalization import (
    CurieNormalizer,
    merge_normalized_data,
    convert_to_output_format,
    convert_failed_to_output_format,
    write_biolink_classes
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PubTatorCleaner:
    """Clean and normalize PubTator data."""
    
    def __init__(self, input_file: str, output_dir: str):
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize CURIE normalizer
        self.normalizer = CurieNormalizer()
        
        # Track data and statistics
        self.curie_to_pmids = defaultdict(set)
        self.concept_id_stats = defaultdict(int)
        self.type_stats = defaultdict(int)
        self.invalid_concept_ids = defaultdict(int)
        self.constructed_curies = defaultdict(int)
        
    def convert_concept_id_to_curie(self, concept_id: str, entity_type: str) -> str:
        """Convert concept ID to proper CURIE format based on type context."""
        # Skip invalid/missing concept IDs
        if concept_id == "-" or not concept_id.strip():
            return None
            
        # Already in CURIE format (contains colon)
        if ":" in concept_id:
            return concept_id
        
        # Bare number - need to infer prefix based on entity type
        if concept_id.isdigit():
            if entity_type.lower() == "species":
                curie = f"NCBITaxon:{concept_id}"
                self.constructed_curies[f"Species->NCBITaxon"] += 1
                return curie
            elif entity_type.lower() == "gene":
                curie = f"NCBIGene:{concept_id}"
                self.constructed_curies[f"Gene->NCBIGene"] += 1
                return curie
            elif entity_type.lower() == "disease":
                # Some diseases might be OMIM numbers
                curie = f"OMIM:{concept_id}"
                self.constructed_curies[f"Disease->OMIM"] += 1
                return curie
            elif entity_type.lower() == "chemical":
                # Chemicals as numbers are tricky - could be CAS, PubChem, etc.
                # Let's try as bare number first and see normalization results
                curie = f"UNKNOWN_CHEMICAL:{concept_id}"
                self.constructed_curies[f"Chemical->UNKNOWN"] += 1
                return curie
            else:
                # Unknown type with number
                curie = f"UNKNOWN_{entity_type.upper()}:{concept_id}"
                self.constructed_curies[f"{entity_type}->UNKNOWN"] += 1
                return curie
        
        # Other formats - log and return as-is with warning
        logger.warning(f"Unusual concept ID format: '{concept_id}' (type: {entity_type})")
        self.constructed_curies[f"{entity_type}->UNUSUAL"] += 1
        return concept_id
    
    def process_file_streaming(self):
        """Process the gzipped file in streaming fashion for memory efficiency."""
        logger.info(f"Processing {self.input_file} in streaming mode")
        
        total_lines = 0
        valid_lines = 0
        
        with gzip.open(self.input_file, 'rt') as f:
            for line_num, line in enumerate(f, 1):
                total_lines += 1
                
                if total_lines % 10000000 == 0:  # Progress every 10M lines
                    logger.info(f"Processed {total_lines:,} lines, valid: {valid_lines:,}")
                
                line = line.strip()
                if not line:
                    continue
                
                try:
                    # Split on tab - expect 5 columns
                    parts = line.split('\t')
                    if len(parts) != 5:
                        logger.warning(f"Line {line_num} has {len(parts)} columns, expected 5")
                        continue
                    
                    pmid_str, entity_type, concept_id, mentions, resource = parts
                    
                    # Parse PMID
                    try:
                        pmid = int(pmid_str)
                    except ValueError:
                        logger.warning(f"Invalid PMID on line {line_num}: '{pmid_str}'")
                        continue
                    
                    # Track statistics
                    self.type_stats[entity_type] += 1
                    self.concept_id_stats[concept_id] += 1
                    
                    # Convert concept ID to CURIE
                    curie = self.convert_concept_id_to_curie(concept_id, entity_type)
                    
                    if curie is None:
                        # Track invalid concept IDs
                        self.invalid_concept_ids[concept_id] += 1
                        continue
                    
                    # Store in our data structure
                    self.curie_to_pmids[curie].add(pmid)
                    valid_lines += 1
                    
                except Exception as e:
                    logger.error(f"Error processing line {line_num}: {e}")
                    logger.error(f"Line content: {line}")
                    
        logger.info(f"Finished processing. Total lines: {total_lines:,}, Valid: {valid_lines:,}")
        logger.info(f"Extracted {len(self.curie_to_pmids):,} unique CURIEs")
        
        # Convert sets to lists for compatibility with normalizer
        curie_to_pmids_list = {curie: list(pmids) for curie, pmids in self.curie_to_pmids.items()}
        return curie_to_pmids_list
    
    def log_statistics(self):
        """Log detailed statistics about the data processing."""
        logger.info("=== PROCESSING STATISTICS ===")
        
        logger.info(f"Entity types found:")
        for entity_type, count in sorted(self.type_stats.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {entity_type}: {count:,}")
        
        logger.info(f"CURIE construction patterns:")
        for pattern, count in sorted(self.constructed_curies.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {pattern}: {count:,}")
        
        logger.info(f"Most common invalid concept IDs:")
        for concept_id, count in list(sorted(self.invalid_concept_ids.items(), key=lambda x: x[1], reverse=True))[:10]:
            logger.info(f"  '{concept_id}': {count:,}")
        
        # Show some example constructed CURIEs for verification
        logger.info(f"Example CURIEs constructed:")
        example_count = 0
        for curie in self.curie_to_pmids.keys():
            if any(prefix in curie for prefix in ["NCBITaxon:", "NCBIGene:", "OMIM:", "UNKNOWN_"]):
                logger.info(f"  {curie} -> {len(self.curie_to_pmids[curie])} PMIDs")
                example_count += 1
                if example_count >= 10:
                    break
    
    def analyze_normalization_failures(self):
        """Analyze normalization failures to identify CURIE construction issues."""
        failed_normalizations = self.normalizer.get_failed_normalizations()
        
        if not failed_normalizations:
            logger.info("No normalization failures to analyze")
            return
        
        logger.info("=== NORMALIZATION FAILURE ANALYSIS ===")
        logger.info(f"Total failed normalizations: {len(failed_normalizations):,}")
        
        # Categorize failures by prefix
        failure_patterns = defaultdict(int)
        prefix_failures = defaultdict(list)
        
        for failed_curie in failed_normalizations.keys():
            if ":" in failed_curie:
                prefix = failed_curie.split(":")[0]
                failure_patterns[prefix] += 1
                prefix_failures[prefix].append(failed_curie)
            else:
                failure_patterns["NO_PREFIX"] += 1
                prefix_failures["NO_PREFIX"].append(failed_curie)
        
        logger.info("Failure patterns by prefix:")
        for prefix, count in sorted(failure_patterns.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {prefix}: {count:,}")
            
            # Show examples
            examples = prefix_failures[prefix][:5]
            for example in examples:
                pmid_count = len(failed_normalizations[example])
                logger.info(f"    Example: {example} ({pmid_count} PMIDs)")
        
        # Special analysis for our constructed CURIEs
        constructed_failures = {k: v for k, v in failed_normalizations.items() 
                              if any(pattern in k for pattern in ["UNKNOWN_", "OMIM:", "NCBITaxon:", "NCBIGene:"])}
        
        if constructed_failures:
            logger.info(f"\nConstructed CURIE failures: {len(constructed_failures):,}")
            for curie, pmids in list(constructed_failures.items())[:10]:
                logger.info(f"  {curie}: {len(pmids)} PMIDs")
    
    def write_jsonlines(self, data: List[Dict], filename: str):
        """Write data to JSONLINES format."""
        output_path = self.output_dir / filename
        logger.info(f"Writing {len(data)} records to {output_path}")
        
        with open(output_path, 'w') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')
                
        logger.info(f"Successfully wrote {output_path}")
    
    def write_failed_normalizations(self, filename: str):
        """Write failed normalization data to simple text format."""
        output_path = self.output_dir / filename.replace('.jsonl', '.txt')
        failed_normalizations = self.normalizer.get_failed_normalizations()
        failed_data = convert_failed_to_output_format(failed_normalizations)
        
        logger.info(f"Writing {len(failed_data)} failed normalization records to {output_path}")
        
        with open(output_path, 'w') as f:
            for curie in failed_data:
                f.write(curie + '\n')
                
        logger.info(f"Successfully wrote {output_path}")
    
    def write_statistics_report(self, filename: str):
        """Write detailed statistics and analysis to a report file."""
        output_path = self.output_dir / filename
        
        report = {
            "total_unique_curies": len(self.curie_to_pmids),
            "entity_type_distribution": dict(self.type_stats),
            "curie_construction_patterns": dict(self.constructed_curies),
            "invalid_concept_ids": dict(self.invalid_concept_ids),
            "normalization_failure_analysis": {}
        }
        
        # Add normalization failure analysis
        failed_normalizations = self.normalizer.get_failed_normalizations()
        if failed_normalizations:
            failure_patterns = defaultdict(int)
            for failed_curie in failed_normalizations.keys():
                if ":" in failed_curie:
                    prefix = failed_curie.split(":")[0]
                    failure_patterns[prefix] += 1
                else:
                    failure_patterns["NO_PREFIX"] += 1
            
            report["normalization_failure_analysis"] = {
                "total_failures": len(failed_normalizations),
                "failure_patterns_by_prefix": dict(failure_patterns)
            }
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
            
        logger.info(f"Successfully wrote statistics report to {output_path}")
    
    def clean(self):
        """Run the complete cleaning pipeline."""
        logger.info("Starting PubTator data cleaning pipeline")
        
        # Step 1: Process file and extract data
        curie_to_pmids = self.process_file_streaming()
        
        if not curie_to_pmids:
            logger.error("No data extracted from file")
            return
        
        # Step 2: Log processing statistics
        self.log_statistics()
        
        # Step 3: Normalize CURIEs
        logger.info("Starting CURIE normalization...")
        normalized_mapping = self.normalizer.normalize_all_curies(curie_to_pmids)
        
        # Step 4: Analyze normalization failures
        self.analyze_normalization_failures()
        
        # Step 5: Merge data based on normalized CURIEs
        merged_data, original_curies_by_normalized = merge_normalized_data(curie_to_pmids, normalized_mapping)
        
        # Step 6: Convert to output format
        output_data = convert_to_output_format(merged_data, original_curies_by_normalized)
        
        # Step 7: Write outputs
        self.write_jsonlines(output_data, "pubtator_cleaned.jsonl")
        self.write_failed_normalizations("pubtator_failed_normalizations.jsonl")
        self.write_statistics_report("pubtator_processing_report.json")
        
        # Step 8: Write biolink classes
        biolink_classes = self.normalizer.get_biolink_classes()
        if biolink_classes:
            biolink_classes_path = self.output_dir / "pubtator_biolink_classes.json"
            write_biolink_classes(biolink_classes, str(biolink_classes_path))
        
        logger.info("PubTator data cleaning pipeline completed successfully")
        logger.info(f"Total records processed: {len(curie_to_pmids):,}")
        logger.info(f"Successfully normalized records: {len(output_data):,}")
        logger.info(f"Failed normalization records: {len(self.normalizer.get_failed_normalizations()):,}")


def main():
    """Main entry point."""
    input_file = "input/pubtator/bioconcepts2pubtator3.gz"
    output_dir = "cleaned/pubtator"
    
    cleaner = PubTatorCleaner(input_file, output_dir)
    cleaner.clean()


if __name__ == "__main__":
    main()