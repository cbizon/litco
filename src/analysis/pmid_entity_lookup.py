#!/usr/bin/env python3
"""
PMID entity lookup utilities for cross-referencing with our datasets.

This module provides functionality to look up what entities our datasets
contain for specific PMIDs, helping understand why certain entities
might be missing from our data.
"""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Dict, List, Set, Optional
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PMIDEntityLookup:
    """Look up entities in our datasets by PMID."""
    
    def __init__(self):
        """Initialize with paths to our cleaned datasets."""
        self.dataset_paths = {
            'NGD': {
                'jsonl': Path('../../cleaned/ngd/ngd_cleaned.jsonl'),
                'sqlite': Path('../../cleaned/ngd/ngd_cleaned.sqlite')
            },
            'PubTator': {
                'jsonl': Path('../../cleaned/pubtator/pubtator_cleaned.jsonl'),
                'sqlite': Path('../../cleaned/pubtator/pubtator_cleaned.sqlite')
            },
            'OmniCorp': {
                'jsonl': Path('../../cleaned/omnicorp/omnicorp_cleaned.jsonl'),
                'sqlite': Path('../../cleaned/omnicorp/omnicorp_cleaned.sqlite')
            }
        }
        
        # Cache for PMID->entities mappings
        self.pmid_cache = {}
    
    def lookup_entities_for_pmid_sqlite(self, dataset_name: str, pmid: str) -> Set[str]:
        """
        Look up entities for a PMID using SQLite database (faster for single lookups).
        
        Args:
            dataset_name: Name of dataset (NGD, PubTator, OmniCorp)
            pmid: PMID to look up (without PMID: prefix)
            
        Returns:
            Set of CURIEs found for this PMID
        """
        sqlite_path = self.dataset_paths[dataset_name]['sqlite']
        
        if not sqlite_path.exists():
            logger.warning(f"SQLite file not found for {dataset_name}: {sqlite_path}")
            return set()
        
        try:
            conn = sqlite3.connect(sqlite_path)
            cursor = conn.cursor()
            
            # Query for entities containing this PMID
            # The publications column contains JSON array of PMIDs
            cursor.execute("""
                SELECT curie FROM curie_to_pmids 
                WHERE publications LIKE ?
            """, (f'%"PMID:{pmid}"%',))
            
            entities = {row[0] for row in cursor.fetchall()}
            conn.close()
            
            return entities
            
        except Exception as e:
            logger.error(f"Error querying {dataset_name} SQLite for PMID {pmid}: {e}")
            return set()
    
    def lookup_entities_for_pmid_jsonl(self, dataset_name: str, pmid: str) -> Set[str]:
        """
        Look up entities for a PMID using JSONL file (for datasets without SQLite).
        
        Args:
            dataset_name: Name of dataset (NGD, PubTator, OmniCorp)
            pmid: PMID to look up (without PMID: prefix)
            
        Returns:
            Set of CURIEs found for this PMID
        """
        jsonl_path = self.dataset_paths[dataset_name]['jsonl']
        
        if not jsonl_path.exists():
            logger.warning(f"JSONL file not found for {dataset_name}: {jsonl_path}")
            return set()
        
        entities = set()
        target_pmid = f"PMID:{pmid}"
        
        try:
            with open(jsonl_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        record = json.loads(line.strip())
                        publications = record.get('publications', [])
                        
                        if target_pmid in publications:
                            entities.add(record['curie'])
                            
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON on line {line_num} in {dataset_name}")
                    except KeyError as e:
                        logger.warning(f"Missing key {e} on line {line_num} in {dataset_name}")
        
        except Exception as e:
            logger.error(f"Error reading {dataset_name} JSONL for PMID {pmid}: {e}")
        
        return entities
    
    def lookup_entities_for_pmid(self, dataset_name: str, pmid: str) -> Set[str]:
        """
        Look up entities for a PMID using JSONL files (more reliable than SQLite for this use case).
        
        Args:
            dataset_name: Name of dataset (NGD, PubTator, OmniCorp)
            pmid: PMID to look up (can include or exclude PMID: prefix)
            
        Returns:
            Set of CURIEs found for this PMID
        """
        # Clean PMID (remove PMID: prefix if present)
        clean_pmid = pmid.replace("PMID:", "")
        
        # Check cache first
        cache_key = f"{dataset_name}:{clean_pmid}"
        if cache_key in self.pmid_cache:
            return self.pmid_cache[cache_key]
        
        # Use JSONL files for reliable searching
        entities = self.lookup_entities_for_pmid_jsonl(dataset_name, clean_pmid)
        
        # Cache results
        self.pmid_cache[cache_key] = entities
        
        return entities
    
    def lookup_entities_across_datasets(self, pmid: str) -> Dict[str, Set[str]]:
        """
        Look up entities for a PMID across all datasets.
        
        Args:
            pmid: PMID to look up
            
        Returns:
            Dictionary mapping dataset name to set of entities
        """
        results = {}
        
        for dataset_name in self.dataset_paths.keys():
            entities = self.lookup_entities_for_pmid(dataset_name, pmid)
            results[dataset_name] = entities
        
        return results
    
    def batch_lookup_pmids(self, pmids: List[str]) -> Dict[str, Dict[str, Set[str]]]:
        """
        Look up entities for multiple PMIDs across all datasets efficiently.
        
        Scans each dataset file once, collecting all target PMIDs in a single pass.
        Much more efficient than looking up PMIDs individually.
        
        Args:
            pmids: List of PMIDs to look up
            
        Returns:
            Dictionary mapping PMID to dataset results
        """
        logger.info(f"Starting efficient batch lookup for {len(pmids)} PMIDs")
        
        # Clean PMIDs and create target set
        clean_pmids = [pmid.replace("PMID:", "") for pmid in pmids]
        target_pmids = {f"PMID:{pmid}" for pmid in clean_pmids}
        
        # Initialize results structure
        results = {pmid: {dataset: set() for dataset in self.dataset_paths.keys()} 
                  for pmid in clean_pmids}
        
        # Scan each dataset once
        for dataset_name in self.dataset_paths.keys():
            logger.info(f"Scanning {dataset_name} for {len(target_pmids)} target PMIDs")
            dataset_results = self._scan_dataset_for_pmids(dataset_name, target_pmids)
            
            # Merge results
            for pmid, entities in dataset_results.items():
                clean_pmid = pmid.replace("PMID:", "")
                if clean_pmid in results:
                    results[clean_pmid][dataset_name] = entities
        
        logger.info(f"Batch lookup complete for {len(results)} PMIDs")
        return results
    
    def _scan_dataset_for_pmids(self, dataset_name: str, target_pmids: Set[str]) -> Dict[str, Set[str]]:
        """
        Scan a single dataset for all target PMIDs in one pass.
        
        Args:
            dataset_name: Name of dataset to scan
            target_pmids: Set of PMIDs to look for (with PMID: prefix)
            
        Returns:
            Dictionary mapping PMID to set of entities found
        """
        jsonl_path = self.dataset_paths[dataset_name]['jsonl']
        
        if not jsonl_path.exists():
            logger.warning(f"JSONL file not found for {dataset_name}: {jsonl_path}")
            return {}
        
        pmid_to_entities = defaultdict(set)
        entities_found = 0
        lines_processed = 0
        
        try:
            with open(jsonl_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    lines_processed += 1
                    
                    try:
                        record = json.loads(line.strip())
                        publications = record.get('publications', [])
                        curie = record.get('curie')
                        
                        if not curie:
                            continue
                        
                        # Check if any target PMIDs are in this record's publications
                        matching_pmids = target_pmids.intersection(set(publications))
                        
                        for pmid in matching_pmids:
                            pmid_to_entities[pmid].add(curie)
                            entities_found += 1
                            
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON on line {line_num} in {dataset_name}")
                    except KeyError as e:
                        logger.warning(f"Missing key {e} on line {line_num} in {dataset_name}")
                    
                    # Progress logging for large files
                    if lines_processed % 100000 == 0:
                        logger.info(f"  {dataset_name}: Processed {lines_processed:,} lines, found {entities_found} entity matches")
        
        except Exception as e:
            logger.error(f"Error scanning {dataset_name} for PMIDs: {e}")
            return {}
        
        logger.info(f"  {dataset_name}: Found {entities_found} entity matches across {len(pmid_to_entities)} PMIDs")
        return dict(pmid_to_entities)
    
    def analyze_pmid_entity_patterns(self, pmid_results: Dict[str, Dict[str, Set[str]]]) -> Dict:
        """
        Analyze patterns in PMID entity lookup results.
        
        Args:
            pmid_results: Results from batch_lookup_pmids
            
        Returns:
            Analysis summary
        """
        analysis = {
            'total_pmids': len(pmid_results),
            'pmids_with_entities': {dataset: 0 for dataset in self.dataset_paths.keys()},
            'total_entities_found': {dataset: 0 for dataset in self.dataset_paths.keys()},
            'entities_by_prefix': defaultdict(lambda: defaultdict(int)),
            'sample_entities': {dataset: set() for dataset in self.dataset_paths.keys()}
        }
        
        for pmid, dataset_results in pmid_results.items():
            for dataset, entities in dataset_results.items():
                if entities:
                    analysis['pmids_with_entities'][dataset] += 1
                    analysis['total_entities_found'][dataset] += len(entities)
                    
                    # Collect sample entities
                    analysis['sample_entities'][dataset].update(list(entities)[:5])
                    
                    # Analyze by prefix
                    for entity in entities:
                        prefix = entity.split(':', 1)[0] if ':' in entity else 'NO_PREFIX'
                        analysis['entities_by_prefix'][dataset][prefix] += 1
        
        # Convert sets to lists for JSON serialization
        for dataset in analysis['sample_entities']:
            analysis['sample_entities'][dataset] = list(analysis['sample_entities'][dataset])
        
        return analysis


def main():
    """Test the PMID entity lookup functionality."""
    lookup = PMIDEntityLookup()
    
    # Test with a few sample PMIDs
    test_pmids = ["12345678", "23456789", "34567890"]
    
    for pmid in test_pmids:
        results = lookup.lookup_entities_across_datasets(pmid)
        print(f"\nPMID {pmid}:")
        for dataset, entities in results.items():
            print(f"  {dataset}: {len(entities)} entities")
            if entities:
                print(f"    Sample: {list(entities)[:3]}")


if __name__ == "__main__":
    main()