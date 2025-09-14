#!/usr/bin/env python3
"""Tests for OmniCorp to SQLite conversion functionality."""

import pytest
import sqlite3
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock

import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from omnicorp_to_sqlite import OmniCorpToSQLiteConverter


class TestOmniCorpToSQLiteConverter:
    """Test class for OmniCorp to SQLite conversion."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.input_dir = Path(self.temp_dir) / "input"
        self.output_dir = Path(self.temp_dir) / "output"
        self.input_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)
        
        # Create test TSV files
        self.output_sqlite = self.output_dir / "test_output.sqlite"
        self._create_test_tsv_files()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
    
    def _create_test_tsv_files(self):
        """Create test TSV files with sample data."""
        # Create first TSV file
        tsv1_data = [
            "https://www.ncbi.nlm.nih.gov/pubmed/12345\thttp://purl.obolibrary.org/obo/CHEBI_15377",
            "https://www.ncbi.nlm.nih.gov/pubmed/12345\thttp://id.nlm.nih.gov/mesh/D014867",
            "https://www.ncbi.nlm.nih.gov/pubmed/12346\thttp://purl.obolibrary.org/obo/CHEBI_15377",  # Same CHEBI, different PMID
            "https://www.ncbi.nlm.nih.gov/pubmed/12347\thttp://purl.obolibrary.org/obo/HP_0000001",
            "invalid_url\thttp://purl.obolibrary.org/obo/RO_0002432",  # Invalid URL
            "",  # Empty line
        ]
        
        with open(self.input_dir / "test1.tsv", 'w') as f:
            f.write('\n'.join(tsv1_data))
        
        # Create second TSV file
        tsv2_data = [
            "https://www.ncbi.nlm.nih.gov/pubmed/12348\thttp://purl.obolibrary.org/obo/RO_0002432",
            "https://www.ncbi.nlm.nih.gov/pubmed/12349\thttp://example.com/unknown/format",  # Unknown IRI format
            "invalid_format_line",  # Invalid format
        ]
        
        with open(self.input_dir / "test2.tsv", 'w') as f:
            f.write('\n'.join(tsv2_data))
    
    def test_converter_initialization(self):
        """Test converter initialization."""
        converter = OmniCorpToSQLiteConverter(
            str(self.input_dir),
            str(self.output_sqlite)
        )
        
        assert converter.input_dir == self.input_dir
        assert converter.output_sqlite == self.output_sqlite
        assert 'files_processed' in converter.stats
    
    def test_extract_pmid_from_url(self):
        """Test PMID extraction from PubMed URLs."""
        converter = OmniCorpToSQLiteConverter(
            str(self.input_dir),
            str(self.output_sqlite)
        )
        
        # Test valid URLs
        assert converter.extract_pmid_from_url("https://www.ncbi.nlm.nih.gov/pubmed/12345") == 12345
        assert converter.extract_pmid_from_url("https://www.ncbi.nlm.nih.gov/pubmed/999") == 999
        
        # Test invalid URLs
        assert converter.extract_pmid_from_url("invalid_url") is None
        assert converter.extract_pmid_from_url("https://www.ncbi.nlm.nih.gov/notpubmed/12345") is None
        assert converter.extract_pmid_from_url("https://www.ncbi.nlm.nih.gov/pubmed/") is None
    
    def test_convert_iri_to_curie(self):
        """Test IRI to CURIE conversion logic."""
        converter = OmniCorpToSQLiteConverter(
            str(self.input_dir),
            str(self.output_sqlite)
        )
        
        # Test CHEBI conversion
        assert converter.convert_iri_to_curie("http://purl.obolibrary.org/obo/CHEBI_15377") == "CHEBI:15377"
        
        # Test MESH conversion
        assert converter.convert_iri_to_curie("http://id.nlm.nih.gov/mesh/D014867") == "MESH:D014867"
        
        # Test other OBO terms
        assert converter.convert_iri_to_curie("http://purl.obolibrary.org/obo/RO_0002432") == "RO:0002432"
        assert converter.convert_iri_to_curie("http://purl.obolibrary.org/obo/HP_0000001") == "HP:0000001"
        
        # Test unknown format
        unknown_iri = "http://example.com/unknown/format"
        assert converter.convert_iri_to_curie(unknown_iri) == unknown_iri
        
        # Check statistics tracking
        assert converter.stats['iri_conversions']['CHEBI'] == 1
        assert converter.stats['iri_conversions']['MESH'] == 1
        assert converter.stats['iri_conversions']['RO'] == 1
        assert converter.stats['iri_conversions']['HP'] == 1
        assert converter.stats['iri_conversions']['UNKNOWN'] == 1
    
    @patch('subprocess.run')
    def test_pass1_extract_and_sort(self, mock_subprocess):
        """Test Pass 1: extraction and sorting."""
        mock_subprocess.return_value = Mock(returncode=0)
        
        converter = OmniCorpToSQLiteConverter(
            str(self.input_dir),
            str(self.output_sqlite)
        )
        
        sorted_file = converter.pass1_extract_and_sort()
        
        # Check that subprocess.run was called for sorting
        mock_subprocess.assert_called_once()
        sort_call_args = mock_subprocess.call_args[0][0]
        assert sort_call_args[0] == 'sort'
        assert '-k1,1' in sort_call_args
        
        # Check statistics
        assert converter.stats['files_processed'] == 2  # Two TSV files
        assert converter.stats['lines_processed'] > 0
        assert converter.stats['valid_pairs'] > 0
        assert converter.stats['invalid_urls'] > 0  # Should have some invalid URLs
        
        # Cleanup - the temp file should be deleted by the function
        assert not sorted_file.exists() or sorted_file.suffix == '.sorted.tsv'
    
    def test_pass2_aggregate_to_sqlite_with_mock_sorted_file(self):
        """Test Pass 2 with a mock sorted file."""
        # Create a mock sorted file
        sorted_file = Path(self.temp_dir) / "mock_sorted.tsv"
        with open(sorted_file, 'w') as f:
            # Write sorted CURIE-PMID pairs
            f.write("CHEBI:15377\t12345\n")
            f.write("CHEBI:15377\t12346\n")  # Same CHEBI, different PMID
            f.write("HP:0000001\t12347\n")
            f.write("MESH:D014867\t12345\n")
            f.write("RO:0002432\t12348\n")
        
        converter = OmniCorpToSQLiteConverter(
            str(self.input_dir),
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
            "CHEBI:15377": [12345, 12346],  # Aggregated from multiple PMIDs
            "HP:0000001": [12347],
            "MESH:D014867": [12345],
            "RO:0002432": [12348]
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
        
        # Create a simpler test setup for end-to-end test
        simple_dir = Path(self.temp_dir) / "simple_input"
        simple_dir.mkdir()
        
        with open(simple_dir / "simple.tsv", 'w') as f:
            f.write("https://www.ncbi.nlm.nih.gov/pubmed/12345\thttp://purl.obolibrary.org/obo/CHEBI_15377\n")
            f.write("https://www.ncbi.nlm.nih.gov/pubmed/12346\thttp://purl.obolibrary.org/obo/CHEBI_15377\n")
        
        converter = OmniCorpToSQLiteConverter(
            str(simple_dir),
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
                    f.write("CHEBI:15377\t12345\n")
                    f.write("CHEBI:15377\t12346\n")
                
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
        assert curie == "CHEBI:15377"
        pmids = json.loads(pmids_str)
        assert sorted(pmids) == [12345, 12346]
        
        conn.close()
    
    def test_statistics_logging(self):
        """Test statistics tracking and logging."""
        converter = OmniCorpToSQLiteConverter(
            str(self.input_dir),
            str(self.output_sqlite)
        )
        
        # Process some conversions to build statistics
        converter.convert_iri_to_curie("http://purl.obolibrary.org/obo/CHEBI_15377")
        converter.convert_iri_to_curie("http://id.nlm.nih.gov/mesh/D014867")
        converter.extract_pmid_from_url("invalid_url")  # This will be None
        
        assert converter.stats['iri_conversions']['CHEBI'] == 1
        assert converter.stats['iri_conversions']['MESH'] == 1
        
        # Test log statistics (should not raise exception)
        converter.log_statistics()
    
    def test_no_tsv_files_error(self):
        """Test error handling when no TSV files are found."""
        empty_dir = Path(self.temp_dir) / "empty"
        empty_dir.mkdir()
        
        converter = OmniCorpToSQLiteConverter(
            str(empty_dir),
            str(self.output_sqlite)
        )
        
        with pytest.raises(ValueError, match="No .tsv files found"):
            converter.pass1_extract_and_sort()