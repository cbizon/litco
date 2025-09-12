#!/usr/bin/env python3
"""Tests for shared SQLite cleaning functionality."""

import pytest
import sqlite3
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from sqlite_cleaner import SQLiteCleaner, clean_sqlite_curie_to_pmids


class TestSQLiteCleaner:
    """Test class for shared SQLite cleaning functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.input_dir = Path(self.temp_dir) / "input"
        self.output_dir = Path(self.temp_dir) / "output"
        self.input_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)
        
        # Create test database
        self.db_path = self.input_dir / "test.sqlite"
        self._create_test_database()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
    
    def _create_test_database(self):
        """Create a test SQLite database with sample data."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create table with same schema as NGD
        cursor.execute('''
            CREATE TABLE curie_to_pmids (
                curie TEXT PRIMARY KEY,
                pmids TEXT
            )
        ''')
        
        # Add test data - some CURIEs that will normalize to same concept
        test_data = [
            ("MESH:D014867", "[12345, 67890]"),  # Water - normalizes to CHEBI:15377
            ("CHEMBL.COMPOUND:CHEMBL1098659", "[11111, 22222]"),  # Also water
            ("MONDO:0004976", "[33333]"),  # ALS
            ("INVALID:FAKE123", "[99999]"),  # Won't normalize
        ]
        
        cursor.executemany(
            "INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)",
            test_data
        )
        
        conn.commit()
        conn.close()
    
    def _mock_normalizer_response(self):
        """Mock the normalization API response."""
        # Simulate normalization where MESH:D014867 and CHEMBL normalize to same concept
        return {
            "MESH:D014867": "CHEBI:15377",  # Water
            "CHEMBL.COMPOUND:CHEMBL1098659": "CHEBI:15377",  # Same water concept
            "MONDO:0004976": "MONDO:0004976",  # ALS - normalizes to itself
            # INVALID:FAKE123 not included - simulates failed normalization
        }
    
    def test_cleaner_initialization(self):
        """Test SQLiteCleaner initialization."""
        cleaner = SQLiteCleaner(
            str(self.db_path),
            str(self.output_dir),
            "test_dataset"
        )
        
        assert cleaner.input_db_path == self.db_path
        assert cleaner.output_dir == self.output_dir
        assert cleaner.dataset_name == "test_dataset"
        assert cleaner.normalizer is not None
    
    def test_extract_data_in_chunks(self):
        """Test chunked data extraction from SQLite database."""
        cleaner = SQLiteCleaner(
            str(self.db_path),
            str(self.output_dir),
            "test_dataset"
        )
        
        # Extract all data in small chunks
        chunks = list(cleaner.extract_data_in_chunks(chunk_size=2))
        
        # Should have 2 chunks (4 records, chunk size 2)
        assert len(chunks) == 2
        
        # Combine all chunks
        all_data = {}
        for chunk in chunks:
            all_data.update(chunk)
        
        # Check expected data
        assert len(all_data) == 4
        assert "MESH:D014867" in all_data
        assert all_data["MESH:D014867"] == [12345, 67890]
        assert "MONDO:0004976" in all_data
        assert all_data["MONDO:0004976"] == [33333]
    
    @patch('sqlite_cleaner.CurieNormalizer')
    def test_build_normalization_mapping(self, mock_normalizer_class):
        """Test building complete normalization mapping."""
        # Setup mock
        mock_normalizer = Mock()
        mock_normalizer.normalize_all_curies.return_value = self._mock_normalizer_response()
        mock_normalizer_class.return_value = mock_normalizer
        
        cleaner = SQLiteCleaner(
            str(self.db_path),
            str(self.output_dir),
            "test_dataset"
        )
        
        mapping = cleaner.build_complete_normalization_mapping()
        
        # Check that it extracted all CURIEs and normalized them
        assert len(mapping) == 3  # 3 successful normalizations out of 4 CURIEs
        assert mapping["MESH:D014867"] == "CHEBI:15377"
        assert mapping["CHEMBL.COMPOUND:CHEMBL1098659"] == "CHEBI:15377"
        assert mapping["MONDO:0004976"] == "MONDO:0004976"
        assert "INVALID:FAKE123" not in mapping  # Failed normalization
    
    @patch('sqlite_cleaner.CurieNormalizer')
    def test_clean_complete_pipeline(self, mock_normalizer_class):
        """Test the complete cleaning pipeline."""
        # Setup mock normalizer
        mock_normalizer = Mock()
        mock_normalizer.normalize_all_curies.return_value = self._mock_normalizer_response()
        mock_normalizer.get_failed_normalizations_dict.return_value = {"INVALID:FAKE123": []}
        mock_normalizer.get_failed_normalizations.return_value = ["INVALID:FAKE123"]
        mock_normalizer.get_biolink_classes.return_value = {
            "CHEBI:15377": ["biolink:SmallMolecule"],
            "MONDO:0004976": ["biolink:Disease"]
        }
        mock_normalizer_class.return_value = mock_normalizer
        
        cleaner = SQLiteCleaner(
            str(self.db_path),
            str(self.output_dir),
            "test_dataset"
        )
        
        records_written = cleaner.clean(chunk_size=2)
        
        # Should write 2 records (CHEBI:15377 merged from 2 sources, MONDO:0004976 from 1)
        assert records_written == 2
        
        # Check output files exist
        output_file = self.output_dir / "test_dataset_cleaned.jsonl"
        assert output_file.exists()
        
        failed_file = self.output_dir / "test_dataset_failed_normalizations.txt"
        assert failed_file.exists()
        
        biolink_file = self.output_dir / "test_dataset_biolink_classes.json"
        assert biolink_file.exists()
        
        # Check output content
        with open(output_file, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 2
            
            # Parse records
            records = [json.loads(line) for line in lines]
            records_by_curie = {r["curie"]: r for r in records}
            
            # Check water record (merged from 2 sources)
            water_record = records_by_curie["CHEBI:15377"]
            assert set(water_record["original_curies"]) == {"MESH:D014867", "CHEMBL.COMPOUND:CHEMBL1098659"}
            assert set(water_record["publications"]) == {"PMID:12345", "PMID:67890", "PMID:11111", "PMID:22222"}
            
            # Check ALS record
            als_record = records_by_curie["MONDO:0004976"]
            assert als_record["original_curies"] == ["MONDO:0004976"]
            assert als_record["publications"] == ["PMID:33333"]
        
        # Check failed normalizations file
        with open(failed_file, 'r') as f:
            failed_curies = f.read().strip().split('\n')
            assert "INVALID:FAKE123" in failed_curies


class TestCleanSQLiteCurieFunction:
    """Test the standalone clean_sqlite_curie_to_pmids function."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.input_dir = Path(self.temp_dir) / "input"
        self.output_dir = Path(self.temp_dir) / "output"
        self.input_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)
        
        # Create test database
        self.db_path = self.input_dir / "test.sqlite"
        self._create_test_database()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
    
    def _create_test_database(self):
        """Create a simple test database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE curie_to_pmids (
                curie TEXT PRIMARY KEY,
                pmids TEXT
            )
        ''')
        
        cursor.execute(
            "INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)",
            ("TEST:123", "[456, 789]")
        )
        
        conn.commit()
        conn.close()
    
    @patch('sqlite_cleaner.CurieNormalizer')
    def test_function_interface(self, mock_normalizer_class):
        """Test the standalone function interface."""
        # Setup mock
        mock_normalizer = Mock()
        mock_normalizer.normalize_all_curies.return_value = {"TEST:123": "NORM:123"}
        mock_normalizer.get_failed_normalizations_dict.return_value = {}
        mock_normalizer.get_failed_normalizations.return_value = []
        mock_normalizer.get_biolink_classes.return_value = {}
        mock_normalizer_class.return_value = mock_normalizer
        
        records_written = clean_sqlite_curie_to_pmids(
            input_sqlite_path=str(self.db_path),
            output_dir=str(self.output_dir),
            dataset_name="function_test",
            chunk_size=100
        )
        
        assert records_written == 1
        
        # Check output file exists with correct naming
        output_file = self.output_dir / "function_test_cleaned.jsonl"
        assert output_file.exists()