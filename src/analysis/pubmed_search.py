#!/usr/bin/env python3
"""
PubMed search utilities for investigating missing entities.

This module provides functionality to search PubMed for entity labels
and retrieve relevant PMIDs to understand why certain drugs/diseases
might be missing from our datasets.
"""

import time
import logging
from typing import List, Dict, Optional
from Bio import Entrez
import requests
from urllib.parse import quote

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set email for Entrez (required by NCBI)
Entrez.email = "research@example.com"  # Replace with actual email in production


class PubMedSearcher:
    """Search PubMed for entity labels and retrieve relevant PMIDs."""
    
    def __init__(self, rate_limit_delay: float = 0.5):
        """
        Initialize PubMed searcher.
        
        Args:
            rate_limit_delay: Delay between API calls to respect NCBI rate limits
        """
        self.rate_limit_delay = rate_limit_delay
        self.search_cache = {}  # Cache to avoid repeated searches
    
    def search_pubmed_for_entity(self, entity_label: str, max_results: int = 5) -> List[str]:
        """
        Search PubMed for an entity label and return top PMIDs.
        
        Args:
            entity_label: The label/name to search for
            max_results: Maximum number of PMIDs to return
            
        Returns:
            List of PMIDs as strings
        """
        # Check cache first
        cache_key = f"{entity_label}:{max_results}"
        if cache_key in self.search_cache:
            logger.debug(f"Using cached results for: {entity_label}")
            return self.search_cache[cache_key]
        
        logger.info(f"Searching PubMed for: {entity_label}")
        
        try:
            # Clean up the search term
            search_term = self._prepare_search_term(entity_label)
            
            # Search PubMed
            handle = Entrez.esearch(
                db="pubmed",
                term=search_term,
                retmax=max_results,
                sort="relevance"
            )
            
            # Parse results
            search_results = Entrez.read(handle)
            handle.close()
            
            pmids = search_results.get("IdList", [])
            
            # Cache results
            self.search_cache[cache_key] = pmids
            
            logger.info(f"Found {len(pmids)} PMIDs for '{entity_label}'")
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
            
            return pmids
            
        except Exception as e:
            logger.error(f"Error searching PubMed for '{entity_label}': {e}")
            return []
    
    def _prepare_search_term(self, entity_label: str) -> str:
        """
        Prepare and clean the search term for PubMed.
        
        Args:
            entity_label: Raw entity label
            
        Returns:
            Cleaned search term suitable for PubMed
        """
        # Remove common unwanted characters and patterns
        cleaned = entity_label.strip()
        
        # Handle very long chemical names - truncate and use key terms
        if len(cleaned) > 100:
            # For long chemical names, try to extract meaningful parts
            # Remove common chemical notation
            cleaned = cleaned.replace("(", " ").replace(")", " ")
            cleaned = cleaned.replace("[", " ").replace("]", " ")
            cleaned = cleaned.replace(",", " ")
            
            # Take first few meaningful words
            words = [w for w in cleaned.split() if len(w) > 3][:5]
            cleaned = " ".join(words)
        
        # Quote the term if it contains special characters
        if any(char in cleaned for char in [':', ';', '(', ')', '[', ']']):
            cleaned = f'"{cleaned}"'
        
        return cleaned
    
    def batch_search_entities(self, entities: List[Dict], max_results: int = 5) -> Dict[str, Dict]:
        """
        Search PubMed for multiple entities in batch.
        
        Args:
            entities: List of entity dictionaries with 'id' and 'label' keys
            max_results: Maximum PMIDs per entity
            
        Returns:
            Dictionary mapping entity_id to search results
        """
        logger.info(f"Starting batch search for {len(entities)} entities")
        
        results = {}
        total_entities = len(entities)
        
        for i, entity in enumerate(entities, 1):
            entity_id = entity.get('drug_id') or entity.get('disease_id') or entity.get('id', 'unknown')
            entity_label = entity.get('drug_label') or entity.get('disease_label') or entity.get('label', 'unknown')
            
            logger.info(f"Processing entity {i}/{total_entities}: {entity_id}")
            
            pmids = self.search_pubmed_for_entity(entity_label, max_results)
            
            results[entity_id] = {
                'label': entity_label,
                'pmids': pmids,
                'search_successful': len(pmids) > 0
            }
            
            # Progress logging
            if i % 10 == 0:
                logger.info(f"Completed {i}/{total_entities} entities")
        
        logger.info(f"Batch search complete. Processed {len(results)} entities")
        return results
    
    def search_with_fallback_strategies(self, entity_label: str, max_results: int = 5) -> List[str]:
        """
        Search with multiple fallback strategies for difficult terms.
        
        Args:
            entity_label: Entity label to search
            max_results: Maximum PMIDs to return
            
        Returns:
            List of PMIDs
        """
        # Strategy 1: Direct search
        pmids = self.search_pubmed_for_entity(entity_label, max_results)
        if pmids:
            return pmids
        
        # Strategy 2: Search with quotes
        quoted_term = f'"{entity_label}"'
        logger.info(f"Trying quoted search: {quoted_term}")
        pmids = self.search_pubmed_for_entity(quoted_term, max_results)
        if pmids:
            return pmids
        
        # Strategy 3: Search key words only
        words = entity_label.split()
        if len(words) > 1:
            key_words = " ".join(words[:3])  # First 3 words
            logger.info(f"Trying key words search: {key_words}")
            pmids = self.search_pubmed_for_entity(key_words, max_results)
            if pmids:
                return pmids
        
        # Strategy 4: Single most meaningful word
        if len(words) > 1:
            # Find longest word (likely most specific)
            longest_word = max(words, key=len)
            if len(longest_word) > 4:
                logger.info(f"Trying single word search: {longest_word}")
                pmids = self.search_pubmed_for_entity(longest_word, max_results)
                if pmids:
                    return pmids
        
        logger.warning(f"All search strategies failed for: {entity_label}")
        return []


def main():
    """Test the PubMed search functionality."""
    searcher = PubMedSearcher()
    
    # Test with a few sample entities
    test_entities = [
        {"id": "CHEBI:15377", "label": "water"},
        {"id": "MONDO:0007739", "label": "Huntington disease"},
        {"id": "TEST:001", "label": "very long chemical name that should be truncated"}
    ]
    
    for entity in test_entities:
        pmids = searcher.search_pubmed_for_entity(entity["label"])
        print(f"{entity['id']} ({entity['label']}): {pmids}")


if __name__ == "__main__":
    main()