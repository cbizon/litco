#!/usr/bin/env python3
"""Tests for OmniCorp data cleaning functionality using shared sqlite_cleaner."""

import pytest
import sqlite3
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock

import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

import clean_omnicorp
from sqlite_cleaner import clean_sqlite_curie_to_pmids


class TestOmniCorpCleaningIntegration:
    """Integration tests for OmniCorp cleaning using shared module."""
    
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
        """Create a test SQLite database with OmniCorp data in NGD format."""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Create table with NGD schema
        cursor.execute("CREATE TABLE curie_to_pmids (curie TEXT, pmids TEXT)")
        cursor.execute("CREATE UNIQUE INDEX unique_curie ON curie_to_pmids (curie)")
        
        # Insert test data that would come from omnicorp_to_sqlite.py
        test_data = [
            ("CHEBI:15377", "[12345, 67890]"),  # Water (from IRI conversion)
            ("MESH:D014867", "[11111]"),  # Also water (will merge if same concept)
            ("HP:0000001", "[33333, 44444]"),  # Phenotype
            ("RO:0002432", "[55555]"),  # Relation
        ]
        
        cursor.executemany("INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)", test_data)
        conn.commit()
        conn.close()
    
    @patch('sqlite_cleaner.CurieNormalizer')
    def test_omnicorp_main_function(self, mock_normalizer_class):
        """Test the OmniCorp main function integration."""
        # Setup mock
        mock_normalizer = Mock()
        mock_normalizer.normalize_all_curies.return_value = {
            "CHEBI:15377": "CHEBI:15377",  # Water
            "MESH:D014867": "CHEBI:15377",  # Same water concept (will merge)
            "HP:0000001": "HP:0000001",   # Phenotype
            "RO:0002432": "RO:0002432"    # Relation
        }
        mock_normalizer.get_failed_normalizations_dict.return_value = {}
        mock_normalizer.get_failed_normalizations.return_value = []
        mock_normalizer.get_biolink_classes.return_value = {
            "CHEBI:15377": ["biolink:SmallMolecule"],
            "HP:0000001": ["biolink:PhenotypicFeature"],
            "RO:0002432": ["biolink:Relation"]
        }
        mock_normalizer_class.return_value = mock_normalizer
        
        # Test the function interface directly
        records_written = clean_sqlite_curie_to_pmids(
            input_sqlite_path=str(self.test_db_path),
            output_dir=str(self.output_dir),
            dataset_name="omnicorp"
        )
        
        # Should write 3 records (merged water + phenotype + relation)
        assert records_written == 3
        
        # Check output files
        output_file = self.output_dir / "omnicorp_cleaned.jsonl"
        assert output_file.exists()
        
        # Verify output content
        with open(output_file, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 3
            
            records = [json.loads(line) for line in lines]
            records_by_curie = {r["curie"]: r for r in records}
            
            # Check merged water record
            water_record = records_by_curie["CHEBI:15377"]
            assert set(water_record["original_curies"]) == {"CHEBI:15377", "MESH:D014867"}
            # All PMIDs from both sources should be merged
            expected_pmids = {"PMID:12345", "PMID:67890", "PMID:11111"}
            assert set(water_record["publications"]) == expected_pmids
            
            # Check phenotype record
            phenotype_record = records_by_curie["HP:0000001"]
            assert phenotype_record["original_curies"] == ["HP:0000001"]
            assert set(phenotype_record["publications"]) == {"PMID:33333", "PMID:44444"}
            
            # Check relation record
            relation_record = records_by_curie["RO:0002432"]
            assert relation_record["original_curies"] == ["RO:0002432"]
            assert relation_record["publications"] == ["PMID:55555"]
    
    @patch('sqlite_cleaner.CurieNormalizer')
    @patch('clean_omnicorp.logger')
    def test_omnicorp_main_script_integration(self, mock_logger, mock_normalizer_class):
        """Test running the OmniCorp main script with mocked paths."""
        # Setup mock
        mock_normalizer = Mock()
        mock_normalizer.normalize_all_curies.return_value = {"CHEBI:15377": "CHEBI:15377"}
        mock_normalizer.get_failed_normalizations_dict.return_value = {}
        mock_normalizer.get_failed_normalizations.return_value = []
        mock_normalizer.get_biolink_classes.return_value = {"CHEBI:15377": ["biolink:SmallMolecule"]}
        mock_normalizer_class.return_value = mock_normalizer
        
        # Test the main function with mocked paths
        with patch('clean_omnicorp.clean_sqlite_curie_to_pmids') as mock_clean_func:
            mock_clean_func.return_value = 1
            
            clean_omnicorp.main()
            
            # Verify the function was called with expected parameters
            mock_clean_func.assert_called_once()
            call_args = mock_clean_func.call_args
            assert "omnicorp" in call_args.kwargs['dataset_name']
            assert "input/omnicorp/" in call_args.kwargs['input_sqlite_path']
            assert "cleaned/omnicorp" in call_args.kwargs['output_dir']
    
    @patch('sqlite_cleaner.CurieNormalizer')
    def test_typical_omnicorp_data_patterns(self, mock_normalizer_class):
        """Test with data patterns typical of OmniCorp after conversion."""
        # Setup mock for typical OmniCorp entity types
        mock_normalizer = Mock()
        mock_normalizer.normalize_all_curies.return_value = {
            "CHEBI:15377": "CHEBI:15377",  # Small molecule
            "HP:0000001": "HP:0000001",    # Phenotype
            "RO:0002432": "RO:0002432",    # Relation
        }
        mock_normalizer.get_failed_normalizations_dict.return_value = {}
        mock_normalizer.get_failed_normalizations.return_value = []
        mock_normalizer.get_biolink_classes.return_value = {
            "CHEBI:15377": ["biolink:SmallMolecule"],
            "HP:0000001": ["biolink:PhenotypicFeature"], 
            "RO:0002432": ["biolink:Relation"]
        }
        mock_normalizer_class.return_value = mock_normalizer
        
        records_written = clean_sqlite_curie_to_pmids(
            input_sqlite_path=str(self.test_db_path),
            output_dir=str(self.output_dir),
            dataset_name="omnicorp"
        )
        
        assert records_written == 3
        
        # Check that biolink classes are written correctly
        biolink_file = self.output_dir / "omnicorp_biolink_classes.json"
        assert biolink_file.exists()
        
        with open(biolink_file, 'r') as f:
            biolink_data = json.load(f)
            assert "CHEBI:15377" in biolink_data['curie_to_classes']
            assert "HP:0000001" in biolink_data['curie_to_classes']
            assert "RO:0002432" in biolink_data['curie_to_classes']