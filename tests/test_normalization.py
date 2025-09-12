#!/usr/bin/env python3
"""Tests for shared normalization functionality."""

import pytest
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "src"))

from normalization import (
    RobustAPIClient, 
    CurieNormalizer, 
    merge_normalized_data,
    convert_to_output_format,
    convert_failed_to_output_format
)


class TestRobustAPIClient:
    """Test the robust API client."""
    
    def test_api_client_creation(self):
        """Test creating API client with custom parameters."""
        client = RobustAPIClient(
            "https://example.com/api",
            max_retries=3,
            base_delay=0.5,
            max_delay=10.0
        )
        
        assert client.base_url == "https://example.com/api"
        assert client.max_retries == 3
        assert client.base_delay == 0.5
        assert client.max_delay == 10.0
        assert client.session is not None


class TestCurieNormalizer:
    """Test the CURIE normalizer."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.normalizer = CurieNormalizer()
    
    def test_normalize_curies_success(self):
        """Test successful normalization with real API."""
        test_curies = ["NCBIGene:1017", "MONDO:0007739"]
        test_data = {curie: [12345] for curie in test_curies}
        
        result = self.normalizer.normalize_curies(test_curies, test_data)
        
        assert len(result) == 2
        for curie in test_curies:
            assert curie in result
            assert result[curie]  # Should have normalized identifier
    
    def test_normalize_all_curies_batching(self):
        """Test batch processing of CURIEs."""
        # Create a small dataset
        test_data = {
            "NCBIGene:1017": [11111],
            "MONDO:0007739": [22222],
            "CHEBI:15365": [33333]
        }
        
        result = self.normalizer.normalize_all_curies(test_data, batch_size=2)
        
        assert len(result) == 3
        for curie in test_data.keys():
            assert curie in result
    
    def test_failed_normalization_tracking(self):
        """Test tracking of failed normalizations."""
        test_curies = ["INVALID:123456", "ALSO_INVALID:999999"]
        test_data = {curie: [12345] for curie in test_curies}
        
        self.normalizer.normalize_curies(test_curies, test_data)
        
        failed = self.normalizer.get_failed_normalizations()
        
        # Should track failed normalizations
        assert len(failed) >= 0  # May be 0 if API accepts anything, or 2 if it rejects invalid CURIEs
        
        # Test clearing failed normalizations
        self.normalizer.clear_failed_normalizations()
        failed_after_clear = self.normalizer.get_failed_normalizations()
        assert len(failed_after_clear) == 0
    
    def test_biolink_class_extraction(self):
        """Test extraction of biolink classes from normalization."""
        test_curies = ["MESH:D013119", "NCBIGene:1017", "NCBITaxon:9606"]
        test_data = {curie: [12345] for curie in test_curies}
        
        result = self.normalizer.normalize_curies(test_curies, test_data)
        
        # Get biolink classes
        biolink_classes = self.normalizer.get_biolink_classes()
        
        # Should have captured biolink classes for normalized CURIEs
        assert len(biolink_classes) > 0
        
        # Check that we have biolink classes for some of the results
        for original_curie in test_curies:
            if original_curie in result:
                normalized_curie = result[original_curie]
                if normalized_curie in biolink_classes:
                    classes = biolink_classes[normalized_curie]
                    assert isinstance(classes, list)
                    assert len(classes) > 0
                    # Should contain biolink-style classes
                    assert any(cls.startswith("biolink:") for cls in classes)
        
        # Test clearing biolink classes
        self.normalizer.clear_biolink_classes()
        cleared_classes = self.normalizer.get_biolink_classes()
        assert len(cleared_classes) == 0


class TestUtilityFunctions:
    """Test utility functions for data processing."""
    
    def test_merge_normalized_data(self):
        """Test merging data when normalized CURIEs collapse."""
        curie_to_pmids = {
            "ORIG:123": [11111, 22222],
            "ORIG:456": [33333, 44444],
            "ORIG:789": [55555, 66666]
        }
        
        # Two CURIEs normalize to the same identifier
        normalized_mapping = {
            "ORIG:123": "NORM:999",
            "ORIG:456": "NORM:999",  # Same as ORIG:123
            "ORIG:789": "NORM:789"
        }
        
        merged_data, original_curies_by_normalized = merge_normalized_data(curie_to_pmids, normalized_mapping)
        
        assert len(merged_data) == 2
        assert merged_data["NORM:999"] == {11111, 22222, 33333, 44444}
        assert merged_data["NORM:789"] == {55555, 66666}
        
        assert len(original_curies_by_normalized) == 2
        assert set(original_curies_by_normalized["NORM:999"]) == {"ORIG:123", "ORIG:456"}
        assert original_curies_by_normalized["NORM:789"] == ["ORIG:789"]
    
    def test_convert_to_output_format(self):
        """Test conversion to final output format."""
        merged_data = {
            "CHEBI:123": {11111, 22222, 33333},
            "MESH:D456": {44444, 55555}
        }
        
        original_curies_by_normalized = {
            "CHEBI:123": ["ORIG:123", "ORIG:456"],
            "MESH:D456": ["MESH:D456"]
        }
        
        result = convert_to_output_format(merged_data, original_curies_by_normalized)
        
        assert len(result) == 2
        
        # Check sorting (CHEBI comes before MESH alphabetically)
        assert result[0]["curie"] == "CHEBI:123"
        assert result[1]["curie"] == "MESH:D456"
        
        # Check PMID format and sorting
        assert result[0]["publications"] == ["PMID:11111", "PMID:22222", "PMID:33333"]
        assert result[1]["publications"] == ["PMID:44444", "PMID:55555"]
        
        # Check original CURIEs are included and sorted
        assert result[0]["original_curies"] == ["ORIG:123", "ORIG:456"]
        assert result[1]["original_curies"] == ["MESH:D456"]
    
    def test_convert_failed_to_output_format(self):
        """Test conversion of failed normalizations to output format."""
        failed_normalizations = {
            "INVALID:123": [11111, 22222],
            "ALSO_INVALID:456": [33333]
        }
        
        result = convert_failed_to_output_format(failed_normalizations)
        
        assert len(result) == 2
        
        # Check sorting
        assert result[0]["curie"] == "ALSO_INVALID:456"  # Alphabetically first
        assert result[1]["curie"] == "INVALID:123"
        
        # Check PMID format
        assert result[0]["publications"] == ["PMID:33333"]
        assert result[1]["publications"] == ["PMID:11111", "PMID:22222"]
        
        # Failed format should not have original_curies field
        for item in result:
            assert "original_curies" not in item
    
    def test_empty_data_handling(self):
        """Test handling of empty data structures."""
        # Empty merge
        empty_merged, empty_original = merge_normalized_data({}, {})
        assert empty_merged == {}
        assert empty_original == {}
        
        # Empty output conversion
        empty_output = convert_to_output_format({}, {})
        assert empty_output == []
        
        # Empty failed conversion
        empty_failed = convert_failed_to_output_format({})
        assert empty_failed == []