#!/usr/bin/env python3
"""Tests for OmniCorp data cleaning functionality."""

import pytest
import json
import tempfile
import shutil
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from clean_omnicorp import OmniCorpCleaner


class TestOmniCorpCleaner:
    """Test class for OmniCorp data cleaning."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.input_dir = Path(self.temp_dir) / "input"
        self.output_dir = Path(self.temp_dir) / "output"
        self.input_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)
        
        # Create test TSV files
        self.create_test_files()
        
        # Initialize cleaner
        self.cleaner = OmniCorpCleaner(str(self.input_dir), str(self.output_dir))
        
    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
        
    def create_test_files(self):
        """Create test TSV files with sample data."""
        # Test file 1
        test_file1 = self.input_dir / "test1.tsv"
        with open(test_file1, 'w') as f:
            f.write("https://www.ncbi.nlm.nih.gov/pubmed/4307\thttp://purl.obolibrary.org/obo/CHEBI_17822\n")
            f.write("https://www.ncbi.nlm.nih.gov/pubmed/4307\thttp://id.nlm.nih.gov/mesh/D014346\n")
            f.write("https://www.ncbi.nlm.nih.gov/pubmed/5555\thttp://purl.obolibrary.org/obo/CHEBI_17822\n")
            f.write("https://www.ncbi.nlm.nih.gov/pubmed/6666\thttp://purl.obolibrary.org/obo/RO_0002432\n")
        
        # Test file 2  
        test_file2 = self.input_dir / "test2.tsv"
        with open(test_file2, 'w') as f:
            f.write("https://www.ncbi.nlm.nih.gov/pubmed/7777\thttp://purl.obolibrary.org/obo/CHEBI_17822\n")
            f.write("https://www.ncbi.nlm.nih.gov/pubmed/8888\thttp://purl.obolibrary.org/obo/PATO_0000033\n")
            
    def test_extract_pmid_from_url(self):
        """Test extracting PMID from PubMed URLs."""
        url = "https://www.ncbi.nlm.nih.gov/pubmed/4307"
        pmid = self.cleaner.extract_pmid_from_url(url)
        assert pmid == 4307
        
        # Test with different PMID
        url2 = "https://www.ncbi.nlm.nih.gov/pubmed/12345678" 
        pmid2 = self.cleaner.extract_pmid_from_url(url2)
        assert pmid2 == 12345678
        
        # Test invalid URL
        with pytest.raises(ValueError):
            self.cleaner.extract_pmid_from_url("https://invalid.com/notpmid")
            
    def test_convert_iri_to_curie(self):
        """Test converting IRIs to CURIEs."""
        # CHEBI
        iri = "http://purl.obolibrary.org/obo/CHEBI_17822"
        curie = self.cleaner.convert_iri_to_curie(iri)
        assert curie == "CHEBI:17822"
        
        # MESH
        iri = "http://id.nlm.nih.gov/mesh/D014346"
        curie = self.cleaner.convert_iri_to_curie(iri)
        assert curie == "MESH:D014346"
        
        # Other OBO (RO)
        iri = "http://purl.obolibrary.org/obo/RO_0002432"
        curie = self.cleaner.convert_iri_to_curie(iri)
        assert curie == "RO:0002432"
        
        # Other OBO (PATO)
        iri = "http://purl.obolibrary.org/obo/PATO_0000033"
        curie = self.cleaner.convert_iri_to_curie(iri)
        assert curie == "PATO:0000033"
        
        # Unknown IRI (should return as-is with warning)
        iri = "http://unknown.example.com/entity/123"
        curie = self.cleaner.convert_iri_to_curie(iri)
        assert curie == iri
        
    def test_process_tsv_file(self):
        """Test processing a single TSV file."""
        test_file = self.input_dir / "test1.tsv"
        self.cleaner.process_tsv_file(test_file)
        
        # Check that data was extracted correctly
        assert "CHEBI:17822" in self.cleaner.curie_to_pmids
        assert "MESH:D014346" in self.cleaner.curie_to_pmids
        assert "RO:0002432" in self.cleaner.curie_to_pmids
        
        # Check PMIDs
        assert 4307 in self.cleaner.curie_to_pmids["CHEBI:17822"]
        assert 5555 in self.cleaner.curie_to_pmids["CHEBI:17822"]
        assert 4307 in self.cleaner.curie_to_pmids["MESH:D014346"]
        assert 6666 in self.cleaner.curie_to_pmids["RO:0002432"]
        
    def test_process_all_files(self):
        """Test processing all TSV files."""
        curie_to_pmids = self.cleaner.process_all_files()
        
        # Should have processed both files
        assert len(curie_to_pmids) == 4  # CHEBI:17822, MESH:D014346, RO:0002432, PATO:0000033
        
        # Check CHEBI:17822 appears in both files
        assert set(curie_to_pmids["CHEBI:17822"]) == {4307, 5555, 7777}
        
        # Check other CURIEs
        assert curie_to_pmids["MESH:D014346"] == [4307]
        assert curie_to_pmids["RO:0002432"] == [6666] 
        assert curie_to_pmids["PATO:0000033"] == [8888]
        
    def test_process_malformed_lines(self):
        """Test handling of malformed lines."""
        # Create file with malformed data
        test_file = self.input_dir / "malformed.tsv"
        with open(test_file, 'w') as f:
            f.write("https://www.ncbi.nlm.nih.gov/pubmed/4307\thttp://purl.obolibrary.org/obo/CHEBI_17822\n")  # Good line
            f.write("malformed line with no tab\n")  # Bad line
            f.write("too\tmany\ttabs\there\n")  # Too many tabs
            f.write("https://invalid.url/notpmid\thttp://purl.obolibrary.org/obo/CHEBI_99999\n")  # Bad URL
            f.write("\n")  # Empty line
            f.write("https://www.ncbi.nlm.nih.gov/pubmed/9999\thttp://purl.obolibrary.org/obo/CHEBI_55555\n")  # Good line
        
        self.cleaner.process_tsv_file(test_file)
        
        # Should have extracted only the good lines
        assert len(self.cleaner.curie_to_pmids) == 2
        assert "CHEBI:17822" in self.cleaner.curie_to_pmids
        assert "CHEBI:55555" in self.cleaner.curie_to_pmids
        
    def test_write_jsonlines(self):
        """Test writing data to JSONLINES format."""
        test_data = [
            {"curie": "CHEBI:17822", "publications": ["PMID:4307", "PMID:5555"]},
            {"curie": "MESH:D014346", "publications": ["PMID:4307"]}
        ]
        
        filename = "test_output.jsonl"
        self.cleaner.write_jsonlines(test_data, filename)
        
        # Read back and verify
        output_file = self.output_dir / filename
        assert output_file.exists()
        
        with open(output_file) as f:
            lines = f.readlines()
            
        assert len(lines) == 2
        
        line1 = json.loads(lines[0].strip())
        line2 = json.loads(lines[1].strip())
        
        assert line1 == test_data[0]
        assert line2 == test_data[1]
        
    def test_full_pipeline_integration(self):
        """Test the complete cleaning pipeline with real API calls."""
        # Run the pipeline on our small test dataset
        self.cleaner.clean()
        
        # Verify output file was created
        output_file = self.output_dir / "omnicorp_cleaned.jsonl"
        assert output_file.exists()
        
        # Read and verify output
        with open(output_file) as f:
            lines = [json.loads(line.strip()) for line in f]
            
        # Should have entries for our test CURIEs (some may be merged during normalization)
        assert len(lines) >= 1
        
        # Verify structure of output
        for item in lines:
            assert "curie" in item
            assert "publications" in item
            assert "original_curies" in item
            assert isinstance(item["publications"], list)
            assert isinstance(item["original_curies"], list)
            
            # Verify PMID format
            for pub in item["publications"]:
                assert pub.startswith("PMID:")
                
        # Print results for inspection
        print(f"Pipeline produced {len(lines)} normalized entries")
        for item in lines:
            print(f"  {item['curie']}: {len(item['publications'])} publications, original: {item['original_curies']}")
            
    def test_empty_directory(self):
        """Test handling of empty input directory."""
        # Create empty directory
        empty_dir = Path(self.temp_dir) / "empty"
        empty_dir.mkdir()
        
        cleaner = OmniCorpCleaner(str(empty_dir), str(self.output_dir))
        curie_to_pmids = cleaner.process_all_files()
        
        assert curie_to_pmids is None
        
    def test_no_tsv_files(self):
        """Test handling when no .tsv files are found."""
        # Create directory with non-TSV files
        non_tsv_dir = Path(self.temp_dir) / "no_tsv"
        non_tsv_dir.mkdir()
        
        with open(non_tsv_dir / "test.txt", 'w') as f:
            f.write("not a tsv file")
            
        cleaner = OmniCorpCleaner(str(non_tsv_dir), str(self.output_dir))
        curie_to_pmids = cleaner.process_all_files()
        
        assert curie_to_pmids is None