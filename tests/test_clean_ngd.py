#!/usr/bin/env python3
"""Tests for NGD data cleaning functionality using shared sqlite_cleaner."""

import pytest
import sqlite3
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock

import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

import clean_ngd
from sqlite_cleaner import clean_sqlite_curie_to_pmids


class TestNGDCleaningIntegration:
    """Integration tests for NGD cleaning using shared module."""
    
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
        
    def teardown_method(self):
        """Clean up test fixtures."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
        
    def create_test_database(self):
        """Create a test SQLite database with NGD format."""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Create table with NGD schema
        cursor.execute("CREATE TABLE curie_to_pmids (curie TEXT, pmids TEXT)")
        cursor.execute("CREATE UNIQUE INDEX unique_curie ON curie_to_pmids (curie)")
        
        # Insert test data matching NGD format
        test_data = [
            ("MESH:D014867", "[12345, 67890]"),  # Water
            ("CHEMBL.COMPOUND:CHEMBL1098659", "[11111]"),  # Also water (will merge)
            ("MONDO:0004976", "[33333, 44444]"),  # ALS
        ]
        
        cursor.executemany("INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)", test_data)
        conn.commit()
        conn.close()
    
    @patch('sqlite_cleaner.CurieNormalizer')
    def test_ngd_main_function(self, mock_normalizer_class):
        """Test the NGD main function integration."""
        # Setup mock
        mock_normalizer = Mock()
        mock_normalizer.normalize_all_curies.return_value = {
            "MESH:D014867": "CHEBI:15377",  # Water
            "CHEMBL.COMPOUND:CHEMBL1098659": "CHEBI:15377",  # Same concept
            "MONDO:0004976": "MONDO:0004976"  # ALS
        }
        mock_normalizer.get_failed_normalizations_dict.return_value = {}
        mock_normalizer.get_failed_normalizations.return_value = []
        mock_normalizer.get_biolink_classes.return_value = {
            "CHEBI:15377": ["biolink:SmallMolecule"],
            "MONDO:0004976": ["biolink:Disease"]
        }
        mock_normalizer_class.return_value = mock_normalizer
        
        # Test the function interface directly
        records_written = clean_sqlite_curie_to_pmids(
            input_sqlite_path=str(self.test_db_path),
            output_dir=str(self.output_dir),
            dataset_name="ngd"
        )
        
        # Should write 2 records (merged water + ALS)
        assert records_written == 2
        
        # Check output files
        output_file = self.output_dir / "ngd_cleaned.jsonl"
        assert output_file.exists()
        
        # Verify output content
        with open(output_file, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 2
            
            records = [json.loads(line) for line in lines]
            records_by_curie = {r["curie"]: r for r in records}
            
            # Check merged water record
            water_record = records_by_curie["CHEBI:15377"]
            assert set(water_record["original_curies"]) == {"MESH:D014867", "CHEMBL.COMPOUND:CHEMBL1098659"}
            # All PMIDs from both sources should be merged
            expected_pmids = {"PMID:12345", "PMID:67890", "PMID:11111"}
            assert set(water_record["publications"]) == expected_pmids
            
            # Check ALS record
            als_record = records_by_curie["MONDO:0004976"]
            assert als_record["original_curies"] == ["MONDO:0004976"]
            assert set(als_record["publications"]) == {"PMID:33333", "PMID:44444"}
    
    @patch('sqlite_cleaner.CurieNormalizer')
    @patch('clean_ngd.logger')
    def test_ngd_main_script_integration(self, mock_logger, mock_normalizer_class):
        """Test running the NGD main script with mocked paths."""
        # Setup mock
        mock_normalizer = Mock()
        mock_normalizer.normalize_all_curies.return_value = {"MESH:D014867": "CHEBI:15377"}
        mock_normalizer.get_failed_normalizations_dict.return_value = {}
        mock_normalizer.get_failed_normalizations.return_value = []
        mock_normalizer.get_biolink_classes.return_value = {"CHEBI:15377": ["biolink:SmallMolecule"]}
        mock_normalizer_class.return_value = mock_normalizer
        
        # Temporarily patch the paths in the main function
        with patch.object(clean_ngd, '__name__', '__main__'), \
             patch('clean_ngd.clean_sqlite_curie_to_pmids') as mock_clean_func:
            
            mock_clean_func.return_value = 1
            
            # This would normally be called by if __name__ == "__main__"
            # but we can test the main function directly
            clean_ngd.main()
            
            # Verify the function was called with expected parameters
            mock_clean_func.assert_called_once()
            call_args = mock_clean_func.call_args
            assert "ngd" in call_args.kwargs['dataset_name']
            assert "input/ngd/" in call_args.kwargs['input_sqlite_path']
            assert "cleaned/ngd" in call_args.kwargs['output_dir']
    
