#!/usr/bin/env python3
"""Tests for PubTator to SQLite conversion functionality."""

import pytest
import gzip
import sqlite3
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock

import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from pubtator_to_sqlite import PubTatorToSQLiteConverter


class TestPubTatorToSQLiteConverter:
    """Test class for PubTator to SQLite conversion."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.input_dir = Path(self.temp_dir) / "input"
        self.output_dir = Path(self.temp_dir) / "output"
        self.input_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)
        
        # Create test PubTator gzipped file
        self.input_file = self.input_dir / "test_pubtator.gz"
        self.output_sqlite = self.output_dir / "test_output.sqlite"
        self._create_test_pubtator_file()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
    
    def _create_test_pubtator_file(self):
        """Create a test gzipped PubTator TSV file."""
        test_data = [
            "12345\tgene\t123\tSome gene\tgene2pubmed",
            "12345\tspecies\t9606\tHomo sapiens\tspecies_mapper",
            "12346\tgene\t123\tSame gene\tgene2pubmed",  # Same gene, different PMID
            "12346\tdisease\t456\tSome disease\tmesh",
            "12347\tchemical\tCVCL_0001\tCell line\tcello",  # Cell line test
            "12347\tchemical\t789\tSome chemical\tmesh",
            "12348\tspecies\t-\tInvalid species\tspecies_mapper",  # Invalid concept ID
            "",  # Empty line
            "invalid_line",  # Invalid format
        ]
        
        with gzip.open(self.input_file, 'wt') as f:
            f.write('\n'.join(test_data))
    
    def test_converter_initialization(self):
        """Test converter initialization."""
        converter = PubTatorToSQLiteConverter(
            str(self.input_file),
            str(self.output_sqlite)
        )
        
        assert converter.input_file == self.input_file
        assert converter.output_sqlite == self.output_sqlite
        assert 'lines_processed' in converter.stats
    
    def test_convert_concept_id_to_curie(self):
        """Test CURIE conversion logic."""
        converter = PubTatorToSQLiteConverter(
            str(self.input_file),
            str(self.output_sqlite)
        )
        
        # Test already CURIE format
        assert converter.convert_concept_id_to_curie("MESH:D123", "chemical") == "MESH:D123"
        
        # Test species number
        assert converter.convert_concept_id_to_curie("9606", "species") == "NCBITaxon:9606"
        
        # Test gene number
        assert converter.convert_concept_id_to_curie("123", "gene") == "NCBIGene:123"
        
        # Test disease number
        assert converter.convert_concept_id_to_curie("456", "disease") == "OMIM:456"
        
        # Test chemical number
        assert converter.convert_concept_id_to_curie("789", "chemical") == "UNKNOWN_CHEMICAL:789"
        
        # Test CVCL cell line
        assert converter.convert_concept_id_to_curie("CVCL_0001", "cellline") == "Cellosaurus:0001"
        
        # Test invalid concept IDs
        assert converter.convert_concept_id_to_curie("-", "gene") is None
        assert converter.convert_concept_id_to_curie("", "gene") is None
        assert converter.convert_concept_id_to_curie("  ", "gene") is None
    
    @patch('subprocess.run')
    def test_pass1_extract_and_sort(self, mock_subprocess):
        """Test Pass 1: extraction and sorting."""
        mock_subprocess.return_value = Mock(returncode=0)
        
        converter = PubTatorToSQLiteConverter(
            str(self.input_file),
            str(self.output_sqlite)
        )
        
        sorted_file = converter.pass1_extract_and_sort()
        
        # Check that subprocess.run was called for sorting
        mock_subprocess.assert_called_once()
        sort_call_args = mock_subprocess.call_args[0][0]
        assert sort_call_args[0] == 'sort'
        assert '-k1,1' in sort_call_args
        
        # Check statistics
        assert converter.stats['lines_processed'] > 0
        assert converter.stats['valid_pairs'] > 0
        assert converter.stats['invalid_concept_ids'] > 0  # Should have some invalid ones
        
        # Cleanup - the temp file should be deleted by the function
        assert not sorted_file.exists() or sorted_file.suffix == '.sorted.tsv'
    
    def test_pass2_aggregate_to_sqlite_with_mock_sorted_file(self):
        """Test Pass 2 with a mock sorted file."""
        # Create a mock sorted file
        sorted_file = Path(self.temp_dir) / "mock_sorted.tsv"
        with open(sorted_file, 'w') as f:
            # Write sorted CURIE-PMID pairs
            f.write("Cellosaurus:0001\t12347\n")
            f.write("NCBIGene:123\t12345\n")
            f.write("NCBIGene:123\t12346\n")  # Same gene, different PMID
            f.write("NCBITaxon:9606\t12345\n")
            f.write("OMIM:456\t12346\n")
            f.write("UNKNOWN_CHEMICAL:789\t12347\n")
        
        converter = PubTatorToSQLiteConverter(
            str(self.input_file),
            str(self.output_sqlite)
        )
        
        converter.pass2_aggregate_to_sqlite(Path(sorted_file))
        
        # Check that SQLite database was created
        assert self.output_sqlite.exists()
        
        # Check database contents
        conn = sqlite3.connect(self.output_sqlite)
        cursor = conn.cursor()
        
        # Check table schema
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='curie_to_pmids'")
        table_schema = cursor.fetchone()[0]
        assert 'curie TEXT PRIMARY KEY' in table_schema
        assert 'pmids TEXT' in table_schema
        
        # Check data
        cursor.execute("SELECT curie, pmids FROM curie_to_pmids ORDER BY curie")
        rows = cursor.fetchall()
        
        expected_data = {
            "Cellosaurus:0001": [12347],
            "NCBIGene:123": [12345, 12346],  # Aggregated from multiple PMIDs
            "NCBITaxon:9606": [12345],
            "OMIM:456": [12346],
            "UNKNOWN_CHEMICAL:789": [12347]
        }
        
        for curie, pmids_str in rows:
            pmids_list = json.loads(pmids_str)
            assert curie in expected_data
            assert sorted(pmids_list) == sorted(expected_data[curie])
        
        conn.close()
        
        # Check that sorted file was cleaned up
        assert not sorted_file.exists()
    
    @patch('subprocess.run')
    def test_complete_conversion_pipeline(self, mock_subprocess):
        """Test the complete conversion pipeline."""
        # Mock successful subprocess call
        mock_subprocess.return_value = Mock(returncode=0)
        
        # Create a simpler test file for end-to-end test
        simple_file = self.input_dir / "simple.gz"
        with gzip.open(simple_file, 'wt') as f:
            f.write("12345\tgene\t123\tTest gene\tgene2pubmed\n")
            f.write("12346\tgene\t123\tSame gene\tgene2pubmed\n")
        
        converter = PubTatorToSQLiteConverter(
            str(simple_file),
            str(self.output_sqlite)
        )
        
        # Mock the sorting by creating the sorted file manually
        def mock_sort_side_effect(cmd, check=True):
            if cmd[0] == 'sort':
                # Extract input and output files from command
                input_file = cmd[2]
                output_file = cmd[4]  # -o flag
                
                # Create mock sorted content
                with open(output_file, 'w') as f:
                    f.write("NCBIGene:123\t12345\n")
                    f.write("NCBIGene:123\t12346\n")
                
                return Mock(returncode=0)
        
        mock_subprocess.side_effect = mock_sort_side_effect
        
        converter.convert()
        
        # Check final output
        assert self.output_sqlite.exists()
        
        conn = sqlite3.connect(self.output_sqlite)
        cursor = conn.cursor()
        cursor.execute("SELECT curie, pmids FROM curie_to_pmids")
        rows = cursor.fetchall()
        
        assert len(rows) == 1
        curie, pmids_str = rows[0]
        assert curie == "NCBIGene:123"
        pmids = json.loads(pmids_str)
        assert sorted(pmids) == [12345, 12346]
        
        conn.close()
    
    def test_statistics_logging(self):
        """Test statistics tracking and logging."""
        converter = PubTatorToSQLiteConverter(
            str(self.input_file),
            str(self.output_sqlite)
        )
        
        # Process some concept IDs to build statistics
        converter.convert_concept_id_to_curie("123", "gene")
        converter.convert_concept_id_to_curie("456", "species")
        converter.convert_concept_id_to_curie("-", "invalid")
        
        assert converter.stats['curie_constructions']['Gene->NCBIGene'] == 1
        assert converter.stats['curie_constructions']['Species->NCBITaxon'] == 1
        
        # Test log statistics (should not raise exception)
        converter.log_statistics()