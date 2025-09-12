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
from typing import Dict, List

from normalization import (
    CurieNormalizer,
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
        
        # Track statistics only (data will be processed in chunks)
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
        
        # Handle common non-colon formats
        if concept_id.startswith("CVCL_"):
            # Cell Line Ontology identifiers - use CLO prefix
            curie = concept_id.replace("CVCL_", "CLO:", 1)
            self.constructed_curies[f"CVCL_->CLO"] += 1
            return curie
        
        # Other unusual formats - return as-is without warning
        self.constructed_curies[f"{entity_type}->OTHER"] += 1
        return concept_id
    
    def _parse_line(self, line: str, line_num: int) -> tuple:
        """Parse a single line and return (pmid, curie) or (None, None) if invalid."""
        line = line.strip()
        if not line:
            return None, None
            
        try:
            parts = line.split('\t')
            if len(parts) != 5:
                return None, None
            
            pmid_str, entity_type, concept_id, mentions, resource = parts
            
            # Parse PMID
            try:
                pmid = int(pmid_str)
            except ValueError:
                return None, None
            
            # Track statistics
            self.type_stats[entity_type] += 1
            self.concept_id_stats[concept_id] += 1
            
            # Convert concept ID to CURIE
            curie = self.convert_concept_id_to_curie(concept_id, entity_type)
            
            if curie is None:
                self.invalid_concept_ids[concept_id] += 1
                return None, None
            
            return pmid, curie
            
        except Exception as e:
            logger.error(f"Error processing line {line_num}: {e}")
            return None, None
    
    def process_file_streaming(self) -> Dict[str, List[int]]:
        """Process file and return curie_to_pmids mapping (for testing)."""
        curie_to_pmids = defaultdict(list)
        total_lines = 0
        
        with gzip.open(self.input_file, 'rt') as f:
            for line_num, line in enumerate(f, 1):
                total_lines += 1
                
                pmid, curie = self._parse_line(line, line_num)
                if pmid is not None and curie is not None:
                    curie_to_pmids[curie].append(pmid)
                    
        return dict(curie_to_pmids)
    
    def build_complete_normalization_mapping(self) -> Dict[str, str]:
        """First pass: collect all CURIEs and build complete normalization mapping."""
        logger.info("Building complete normalization mapping (Pass 1)")
        
        all_curies = set()
        total_lines = 0
        
        with gzip.open(self.input_file, 'rt') as f:
            for line_num, line in enumerate(f, 1):
                total_lines += 1
                
                if total_lines % 5000000 == 0:
                    logger.info(f"Pass 1: {total_lines:,} lines processed, {len(all_curies):,} unique CURIEs found")
                
                pmid, curie = self._parse_line(line, line_num)
                if curie is not None:
                    all_curies.add(curie)
                    
        logger.info(f"Pass 1 complete: {total_lines:,} lines, {len(all_curies):,} unique CURIEs")
        
        # Normalize all CURIEs
        dummy_pmid_data = {curie: [] for curie in all_curies}
        complete_mapping = self.normalizer.normalize_all_curies(dummy_pmid_data)
        
        logger.info(f"Normalized {len(complete_mapping):,} CURIEs successfully")
        return complete_mapping
    
    def process_file_streaming_chunked(self, normalization_mapping: Dict[str, str]):
        """Process file in streaming chunks, writing complete records immediately."""
        logger.info("Starting Pass 2: chunked processing")
        
        # Build reverse mapping: normalized_curie -> set of original_curies
        normalized_to_originals = defaultdict(set)
        for orig, norm in normalization_mapping.items():
            normalized_to_originals[norm].add(orig)
        
        logger.info(f"Found {len(normalized_to_originals):,} normalized CURIEs")
        
        # Track progress: normalized_curie -> (seen_originals, pmids_so_far)
        progress_tracker = {}
        for norm_curie in normalized_to_originals:
            progress_tracker[norm_curie] = (set(), set())  # (seen_originals, pmids)
        
        records_written = 0
        total_lines = 0
        
        # Open output file for streaming writes
        output_path = self.output_dir / "pubtator_cleaned.jsonl"
        
        with open(output_path, 'w') as output_file:
            with gzip.open(self.input_file, 'rt') as f:
                for line_num, line in enumerate(f, 1):
                    total_lines += 1
                    
                    if total_lines % 10000000 == 0:
                        logger.info(f"Pass 2: {total_lines:,} lines, {records_written:,} records written")
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        # Split on tab - expect 5 columns
                        parts = line.split('\t')
                        if len(parts) != 5:
                            continue
                        
                        pmid_str, entity_type, concept_id, mentions, resource = parts
                        
                        # Parse PMID
                        try:
                            pmid = int(pmid_str)
                        except ValueError:
                            continue
                        
                        # Convert concept ID to CURIE
                        curie = self.convert_concept_id_to_curie(concept_id, entity_type)
                        
                        if curie is None or curie not in normalization_mapping:
                            continue
                        
                        normalized_curie = normalization_mapping[curie]
                        seen_originals, accumulated_pmids = progress_tracker[normalized_curie]
                        
                        # Add this original CURIE and its PMID
                        seen_originals.add(curie)
                        accumulated_pmids.add(pmid)
                        
                        # Check if we've seen all original CURIEs for this normalized CURIE
                        expected_originals = normalized_to_originals[normalized_curie]
                        if seen_originals == expected_originals:
                            # Complete! Write record and free memory
                            record = {
                                "curie": normalized_curie,
                                "original_curies": sorted(expected_originals),
                                "publications": [f"PMID:{pmid}" for pmid in sorted(accumulated_pmids)]
                            }
                            output_file.write(json.dumps(record) + '\n')
                            del progress_tracker[normalized_curie]  # Free memory
                            records_written += 1
                            
                            if records_written % 50000 == 0:
                                logger.info(f"Written {records_written:,} records")
                        
                    except Exception as e:
                        logger.error(f"Error processing line {line_num}: {e}")
        
        logger.info(f"Pass 2 complete: {total_lines:,} lines, {records_written:,} records written")
        return records_written
    
    def log_statistics(self):
        """Log processing statistics."""
        logger.info("=== PROCESSING STATISTICS ===")
        
        logger.info("Entity type distribution:")
        for entity_type, count in sorted(self.type_stats.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {entity_type}: {count:,}")
        
        if self.constructed_curies:
            logger.info("CURIE construction patterns:")
            for pattern, count in sorted(self.constructed_curies.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {pattern}: {count:,}")
        
        if self.invalid_concept_ids:
            invalid_count = sum(self.invalid_concept_ids.values())
            logger.info(f"Invalid concept IDs: {invalid_count:,} total")
    
    def analyze_normalization_failures(self):
        """Analyze normalization failures."""
        failed_normalizations = self.normalizer.get_failed_normalizations_dict()
        
        if not failed_normalizations:
            logger.info("No normalization failures")
            return
        
        logger.info(f"=== NORMALIZATION FAILURES: {len(failed_normalizations):,} total ===")
        
        # Categorize failures by prefix
        failure_patterns = defaultdict(int)
        for failed_curie in failed_normalizations.keys():
            prefix = failed_curie.split(":")[0] if ":" in failed_curie else "NO_PREFIX"
            failure_patterns[prefix] += 1
        
        logger.info("Failure patterns:")
        for prefix, count in sorted(failure_patterns.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {prefix}: {count:,}")
        
        # Check constructed CURIE failures
        constructed_failures = {k: v for k, v in failed_normalizations.items() 
                              if any(pattern in k for pattern in ["UNKNOWN_", "OMIM:", "NCBITaxon:", "NCBIGene:"])}
        
        if constructed_failures:
            logger.info(f"Constructed CURIE failures: {len(constructed_failures):,}")
    
    def write_failed_normalizations(self, filename: str):
        """Write failed normalization data to simple text format."""
        output_path = self.output_dir / filename
        failed_normalizations_dict = self.normalizer.get_failed_normalizations_dict()
        
        if failed_normalizations_dict:
            logger.info(f"Writing {len(failed_normalizations_dict)} failed normalizations")
            with open(output_path, 'w') as f:
                for curie in sorted(failed_normalizations_dict.keys()):
                    f.write(curie + '\n')
        else:
            logger.info("No normalization failures to write")
    
    def write_statistics_report(self, filename: str):
        """Write detailed statistics and analysis to a report file."""
        output_path = self.output_dir / filename
        
        # Calculate total unique CURIEs processed
        total_unique_curies = sum(self.concept_id_stats.values()) - sum(self.invalid_concept_ids.values())
        
        report = {
            "total_unique_curies": total_unique_curies,
            "entity_type_distribution": dict(self.type_stats),
            "curie_construction_patterns": dict(self.constructed_curies),
            "invalid_concept_ids": dict(self.invalid_concept_ids),
            "normalization_failure_analysis": {}
        }
        
        # Add normalization failure analysis
        failed_normalizations = self.normalizer.get_failed_normalizations_dict()
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
            
        logger.info("Statistics report written")
    
    def clean(self):
        """Run memory-efficient chunked cleaning pipeline."""
        logger.info("Starting PubTator cleaning pipeline")
        
        # Pass 1: Build normalization mapping
        normalization_mapping = self.build_complete_normalization_mapping()
        if not normalization_mapping:
            logger.error("No CURIEs normalized successfully")
            return
        
        # Log processing statistics
        self.log_statistics()
        
        # Pass 2: Process and write records
        records_written = self.process_file_streaming_chunked(normalization_mapping)
        
        # Analyze and write outputs
        self.analyze_normalization_failures()
        self.write_failed_normalizations("pubtator_failed_normalizations.jsonl")
        self.write_statistics_report("pubtator_processing_report.json")
        
        # Write biolink classes if available
        biolink_classes = self.normalizer.get_biolink_classes()
        if biolink_classes:
            biolink_classes_path = self.output_dir / "pubtator_biolink_classes.json"
            write_biolink_classes(biolink_classes, str(biolink_classes_path))
        
        logger.info("Pipeline completed successfully")
        logger.info(f"Records written: {records_written:,}")
        failed_count = len(self.normalizer.get_failed_normalizations())
        if failed_count > 0:
            logger.info(f"Failed normalizations: {failed_count:,}")


def main():
    """Main entry point."""
    input_file = "input/pubtator/bioconcepts2pubtator3.gz"
    output_dir = "cleaned/pubtator"
    
    cleaner = PubTatorCleaner(input_file, output_dir)
    cleaner.clean()


if __name__ == "__main__":
    main()