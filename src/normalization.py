#!/usr/bin/env python3
"""
Shared normalization functionality for CURIE normalization.

This module provides:
- Robust API client with exponential backoff
- CURIE normalization using the node normalizer API
- Failure tracking and logging
"""

import logging
import time
import random
import json
from typing import Dict, List
from collections import defaultdict
import requests

# Configure logging
logger = logging.getLogger(__name__)


class RobustAPIClient:
    """API client with exponential backoff and retry logic."""
    
    def __init__(self, base_url: str, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 300.0):
        self.base_url = base_url
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.session = requests.Session()
        
    def post_with_retry(self, payload: dict, timeout: int = 60) -> dict:
        """Make POST request with exponential backoff retry logic."""
        for attempt in range(self.max_retries):
            try:
                response = self.session.post(
                    self.base_url, 
                    json=payload, 
                    timeout=timeout,
                    headers={'Content-Type': 'application/json'}
                )
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.Timeout as e:
                logger.warning(f"Timeout on attempt {attempt + 1}/{self.max_retries}: {e}")
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP error on attempt {attempt + 1}/{self.max_retries}: {e}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error on attempt {attempt + 1}/{self.max_retries}: {e}")
                
            if attempt < self.max_retries - 1:
                # Exponential backoff with jitter
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                jitter = random.uniform(0, delay * 0.1)
                sleep_time = delay + jitter
                logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
        
        # All retries failed
        raise Exception(f"Failed to complete request after {self.max_retries} attempts")


class CurieNormalizer:
    """CURIE normalization with failure tracking and biolink class extraction."""
    
    def __init__(self):
        # Initialize robust API client
        self.api_client = RobustAPIClient(
            "https://nodenormalization-sri.renci.org/get_normalized_nodes",
            max_retries=5,
            base_delay=2.0,
            max_delay=300.0
        )
        
        # Track normalization failures
        self.failed_normalizations = {}
        
        # Track biolink classes from normalization
        self.normalized_biolink_classes = {}
    
    def normalize_curies(self, curies: List[str], pmid_data: Dict[str, List[int]]) -> Dict[str, str]:
        """Normalize a batch of CURIEs using the node normalizer API with robust retry logic."""
        logger.info(f"Normalizing {len(curies)} CURIEs")
        
        payload = {
            "curies": curies,
            "conflate": True,
            "drug_chemical_conflate": True
        }
        
        try:
            result = self.api_client.post_with_retry(payload, timeout=120)
            
            # Extract normalized mappings and biolink classes
            normalized_mapping = {}
            for original_curie in curies:
                if original_curie in result and result[original_curie]:
                    normalized_id = result[original_curie].get('id', {}).get('identifier')
                    biolink_types = result[original_curie].get('type', [])
                    
                    if normalized_id:
                        normalized_mapping[original_curie] = normalized_id
                        
                        # Store biolink classes for the normalized CURIE
                        if biolink_types:
                            self.normalized_biolink_classes[normalized_id] = biolink_types
                            logger.debug(f"Captured biolink classes for {normalized_id}: {biolink_types}")
                    else:
                        # Track failure - do NOT add to normalized_mapping
                        if original_curie in pmid_data:
                            self.failed_normalizations[original_curie] = pmid_data[original_curie]
                        logger.warning(f"No normalized identifier for {original_curie}")
                else:
                    # Track failure - do NOT add to normalized_mapping
                    if original_curie in pmid_data:
                        self.failed_normalizations[original_curie] = pmid_data[original_curie]
                    logger.warning(f"CURIE not found in normalization response: {original_curie}")
                    
            return normalized_mapping
            
        except Exception as e:
            logger.error(f"All API retry attempts failed for batch: {e}")
            # Track all as failures - do not add to normalized mapping
            for curie in curies:
                if curie in pmid_data:
                    self.failed_normalizations[curie] = pmid_data[curie]
            return {}
    
    def normalize_all_curies(self, curie_to_pmids: Dict[str, List[int]], batch_size: int = 10000) -> Dict[str, str]:
        """Normalize all CURIEs in batches."""
        all_curies = list(curie_to_pmids.keys())
        normalized_mapping = {}
        
        # Process in batches
        for i in range(0, len(all_curies), batch_size):
            batch = all_curies[i:i + batch_size]
            batch_mapping = self.normalize_curies(batch, curie_to_pmids)
            normalized_mapping.update(batch_mapping)
            logger.info(f"Processed batch {i//batch_size + 1}/{(len(all_curies) + batch_size - 1)//batch_size}")
            
        return normalized_mapping
    
    def get_failed_normalizations(self) -> List[str]:
        """Get list of CURIEs that failed to normalize."""
        return list(self.failed_normalizations.keys())
    
    def get_failed_normalizations_dict(self) -> Dict[str, List[int]]:
        """Get full dictionary of failed normalizations with PMIDs."""
        return self.failed_normalizations.copy()
    
    def clear_failed_normalizations(self):
        """Clear the failed normalizations tracker."""
        self.failed_normalizations.clear()
    
    def get_biolink_classes(self) -> Dict[str, List[str]]:
        """Get dictionary of normalized CURIEs to their biolink classes."""
        return self.normalized_biolink_classes.copy()
    
    def clear_biolink_classes(self):
        """Clear the biolink classes tracker."""
        self.normalized_biolink_classes.clear()


def merge_normalized_data(curie_to_pmids: Dict[str, List[int]], 
                         normalized_mapping: Dict[str, str]) -> tuple:
    """Merge data when normalized CURIEs collapse to the same identifier."""
    logger.info("Merging normalized data")
    
    merged_data = defaultdict(set)
    original_curies_by_normalized = defaultdict(list)
    
    # Only process CURIEs that were successfully normalized
    for original_curie, pmids in curie_to_pmids.items():
        if original_curie in normalized_mapping:
            normalized_curie = normalized_mapping[original_curie]
            merged_data[normalized_curie].update(pmids)
            original_curies_by_normalized[normalized_curie].append(original_curie)
        else:
            logger.debug(f"Skipping {original_curie} - not in normalized mapping (failed normalization)")
        
    logger.info(f"Merged to {len(merged_data)} unique normalized CURIEs")
    return dict(merged_data), dict(original_curies_by_normalized)


def convert_to_output_format(merged_data: Dict[str, set], 
                           original_curies_by_normalized: Dict[str, List[str]]) -> List[Dict]:
    """Convert to final output format with PMID: prefixes and original identifiers."""
    logger.info("Converting to output format")
    
    output_data = []
    for curie, pmids in merged_data.items():
        # Convert PMIDs to PMID:XXXXX format and sort for consistency
        pmid_curies = [f"PMID:{pmid}" for pmid in sorted(pmids)]
        output_data.append({
            "curie": curie,
            "original_curies": sorted(original_curies_by_normalized[curie]),
            "publications": pmid_curies
        })
        
    # Sort by curie for consistency
    output_data.sort(key=lambda x: x["curie"])
    logger.info(f"Created {len(output_data)} output records")
    return output_data


def convert_failed_to_output_format(failed_normalizations: Dict[str, List[int]]) -> List[Dict]:
    """Convert failed normalizations to structured output format."""
    logger.info(f"Converting {len(failed_normalizations)} failed normalizations to output format")
    
    output_data = []
    for curie, pmids in failed_normalizations.items():
        # Convert PMIDs to PMID:XXXXX format and sort for consistency
        pmid_curies = [f"PMID:{pmid}" for pmid in sorted(pmids)]
        output_data.append({
            "curie": curie,
            "publications": pmid_curies
        })
        
    # Sort by curie for consistency
    output_data.sort(key=lambda x: x["curie"])
    logger.info(f"Created {len(output_data)} failed normalization records")
    return output_data


def write_biolink_classes(biolink_classes: Dict[str, List[str]], output_path: str):
    """Write biolink classes to JSON file."""
    logger.info(f"Writing biolink classes for {len(biolink_classes)} CURIEs to {output_path}")
    
    # Convert to more organized format for analysis
    class_analysis = {
        "total_normalized_curies": len(biolink_classes),
        "curie_to_classes": biolink_classes,
        "class_distribution": {}
    }
    
    # Count occurrences of each biolink class
    class_counts = defaultdict(int)
    for curie, classes in biolink_classes.items():
        for biolink_class in classes:
            class_counts[biolink_class] += 1
    
    # Sort by frequency
    class_analysis["class_distribution"] = dict(sorted(class_counts.items(), key=lambda x: x[1], reverse=True))
    
    with open(output_path, 'w') as f:
        json.dump(class_analysis, f, indent=2)
    
    logger.info(f"Successfully wrote biolink classes analysis to {output_path}")
    
    # Log summary statistics
    logger.info(f"Most common biolink classes:")
    for biolink_class, count in list(class_analysis["class_distribution"].items())[:10]:
        logger.info(f"  {biolink_class}: {count:,} CURIEs")