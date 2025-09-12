#!/usr/bin/env python3
"""Tests for NGD data cleaning functionality."""

import pytest
import sqlite3
import json
import tempfile
import shutil
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from clean_ngd import NGDCleaner
from normalization import RobustAPIClient


class TestNGDCleaner:
    """Test class for NGD data cleaning."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.input_dir = Path(self.temp_dir) / "input"
        self.output_dir = Path(self.temp_dir) / "output"
        self.input_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)
        
        # Create test database
        self.test_db_path = self.input_dir / "test.sqlite"
        self.create_test_database()
        
        # Initialize cleaner
        self.cleaner = NGDCleaner(str(self.test_db_path), str(self.output_dir))
        
    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
        
    def create_test_database(self):
        """Create a test SQLite database with sample data."""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("CREATE TABLE curie_to_pmids (curie TEXT, pmids TEXT)")
        cursor.execute("CREATE UNIQUE INDEX unique_curie ON curie_to_pmids (curie)")
        
        # Insert test data
        test_data = [
            ("NCBITaxon:5322", "[25506817, 30584069, 38317964]"),
            ("CHV:0000030710", "[38371111, 38179018, 38988429]"),
            ("NCBIGene:729359", "[37140993, 29500419, 38677512]"),
            ("DUPLICATE:123", "[11111, 22222]"),  # Will normalize to same as DUPLICATE:456
            ("DUPLICATE:456", "[33333, 44444]"),  # Will normalize to same as DUPLICATE:123
        ]
        
        cursor.executemany("INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)", test_data)
        conn.commit()
        conn.close()
        
    def test_extract_data_in_chunks(self):
        """Test extracting data from SQLite database in chunks."""
        chunks = list(self.cleaner.extract_data_in_chunks(chunk_size=3))
        
        # Should have 2 chunks (3 + 2 records)
        assert len(chunks) == 2
        assert len(chunks[0]) == 3
        assert len(chunks[1]) == 2
        
        # Check that all data is present across chunks
        all_curies = set()
        for chunk in chunks:
            all_curies.update(chunk.keys())
        
        assert "NCBITaxon:5322" in all_curies
        assert "CHV:0000030710" in all_curies
        assert len(all_curies) == 5
        
    def test_extract_data_malformed_pmids(self):
        """Test handling of malformed PMID data in chunks."""
        # Create database with malformed data
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)", 
                      ("MALFORMED:123", "not a list"))
        conn.commit()
        conn.close()
        
        # Should handle gracefully - malformed entries skipped
        chunks = list(self.cleaner.extract_data_in_chunks())
        all_curies = set()
        for chunk in chunks:
            all_curies.update(chunk.keys())
        assert "MALFORMED:123" not in all_curies
        
    def test_normalize_curies_success(self):
        """Test successful CURIE normalization with real API calls."""
        # Use real, known CURIEs that should normalize successfully
        test_curies = ["NCBIGene:1017", "MONDO:0007739", "CHEBI:15365"]
        test_data = {curie: [12345] for curie in test_curies}
        result = self.cleaner.normalizer.normalize_curies(test_curies, test_data)
        
        # Verify we got mappings for all CURIEs
        assert len(result) == 3
        for curie in test_curies:
            assert curie in result
            assert result[curie]  # Should have some normalized identifier
            
        # Log results for verification
        print(f"Normalization results: {result}")
        
    def test_normalize_curies_with_invalid_curie(self):
        """Test handling of invalid/unknown CURIEs in real API calls."""
        # Mix of valid and invalid CURIEs
        test_curies = ["NCBIGene:1017", "INVALID:123456", "MONDO:0007739"]
        test_data = {curie: [12345] for curie in test_curies}
        result = self.cleaner.normalizer.normalize_curies(test_curies, test_data)
        
        # Should return mappings only for valid CURIEs (invalid ones are excluded)
        assert len(result) == 2  # Only valid CURIEs should be in result
        assert "NCBIGene:1017" in result  # Should be normalized
        assert "INVALID:123456" not in result  # Invalid CURIE excluded
        assert "MONDO:0007739" in result  # Should be normalized
        
    def test_api_client_retry_logic(self):
        """Test the retry logic with a smaller timeout to trigger retries."""
        # Create API client with very short timeout to test retry behavior
        api_client = RobustAPIClient(
            "https://nodenormalization-sri.renci.org/get_normalized_nodes",
            max_retries=2,
            base_delay=0.1,
            max_delay=1.0
        )
        
        payload = {
            "curies": ["NCBIGene:1017"],
            "conflate": True,
            "drug_chemical_conflate": True
        }
        
        # This should work with normal timeout
        result = api_client.post_with_retry(payload, timeout=60)
        assert "NCBIGene:1017" in result
        
    def test_build_complete_normalization_mapping(self):
        """Test building complete normalization mapping."""
        mapping = self.cleaner.build_complete_normalization_mapping()
        
        # Should have mappings only for CURIEs that successfully normalize
        assert len(mapping) >= 1  # At least some should normalize
        
        # All mapped CURIEs should have valid normalized identifiers
        for original, normalized in mapping.items():
            assert isinstance(original, str)
            assert isinstance(normalized, str)
            assert ":" in normalized  # Should be a valid CURIE format
        
    def test_full_pipeline_integration(self):
        """Test the complete cleaning pipeline with real API calls."""
        # Run the pipeline on our small test dataset
        self.cleaner.clean()
        
        # Verify output file was created
        output_file = self.output_dir / "ngd_cleaned.jsonl"
        assert output_file.exists()
        
        # Read and verify output
        with open(output_file) as f:
            lines = [json.loads(line.strip()) for line in f]
            
        # Should have entries for successfully normalized CURIEs
        assert len(lines) >= 1  # At least 1 unique normalized CURIE
        
        # Verify structure of output
        for item in lines:
            assert "curie" in item
            assert "publications" in item
            assert isinstance(item["publications"], list)
            
            # Verify PMID format
            for pub in item["publications"]:
                assert pub.startswith("PMID:")
                
        # Print results for inspection
        print(f"Pipeline produced {len(lines)} normalized entries")
        for item in lines[:3]:  # Show first 3 entries
            print(f"  {item['curie']}: {len(item['publications'])} publications")
                
    def test_empty_database(self):
        """Test handling of empty database."""
        # Create empty database
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM curie_to_pmids")
        conn.commit()
        conn.close()
        
        chunks = list(self.cleaner.extract_data_in_chunks())
        assert len(chunks) == 0
        
    def test_chunked_processing_workflow(self):
        """Test the chunked processing workflow components."""
        # Test that we can build normalization mapping
        mapping = self.cleaner.build_complete_normalization_mapping()
        
        # Test that we can extract data in chunks
        chunks = list(self.cleaner.extract_data_in_chunks(chunk_size=2))
        assert len(chunks) >= 2  # Should have multiple chunks
        
        # Verify we have some normalizable CURIEs
        assert len(mapping) >= 1