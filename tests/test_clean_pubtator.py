#!/usr/bin/env python3
"""Tests for PubTator data cleaning functionality."""

import pytest
import gzip
import json
import tempfile
import shutil
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from clean_pubtator import PubTatorCleaner


class TestPubTatorCleaner:
    """Test class for PubTator data cleaning."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.input_dir = Path(self.temp_dir) / "input"
        self.output_dir = Path(self.temp_dir) / "output"
        self.input_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)
        
        # Create test gzipped file
        self.test_file_path = self.input_dir / "test_pubtator.gz"
        self.create_test_file()
        
        # Initialize cleaner
        self.cleaner = PubTatorCleaner(str(self.test_file_path), str(self.output_dir))
        
    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
        
    def create_test_file(self):
        """Create a test gzipped PubTator file with sample data."""
        test_data = [
            # Standard MESH CURIEs
            "40799000\tDisease\tMESH:D013119\tspinal trauma\tPubTator3",
            "40799000\tDisease\tMESH:D013167\tankylosing spondylitis\tPubTator3",
            "40740000\tDisease\tMESH:D003141\tinfectious diseases\tPubTator3",
            
            # Bare numbers that need CURIE construction
            "40774000\tSpecies\t4932\tyeast\tPubTator3",
            "40765000\tGene\t64030\tKIT\tPubTator3",
            "40765000\tGene\t24887\tBax\tPubTator3",
            
            # Invalid concept IDs
            "40774000\tChemical\t-\tbetaine-TA\tPubTator3",
            "40765000\tChemical\t-\tADSC\tPubTator3",
            
            # Same CURIE, different PMIDs (for merging test)
            "11111111\tDisease\tMESH:D013119\tspinal trauma\tPubTator3",
            "22222222\tDisease\tMESH:D013119\ttrauma\tPubTator3",
            
            # Same PMID, different CURIEs (for grouping test)  
            "33333333\tDisease\tMESH:D001001\tdisease1\tPubTator3",
            "33333333\tChemical\tMESH:C001001\tchemical1\tPubTator3",
            
            # Malformed lines for error handling
            "incomplete line",
            "too\tmany\ttabs\there\textra\tcolumns\tmore",
            "invalid_pmid\tDisease\tMESH:D123456\ttest\tPubTator3",
            
            # Edge cases
            "44444444\tChemical\t12345\tchemical_number\tPubTator3",  # Chemical with number
            "55555555\tDisease\t678901\tdisease_number\tPubTator3",   # Disease with number (OMIM?)
        ]
        
        with gzip.open(self.test_file_path, 'wt') as f:
            for line in test_data:
                f.write(line + '\n')
    
    def test_convert_concept_id_to_curie(self):
        """Test converting concept IDs to proper CURIE format."""
        # Already proper CURIEs
        assert self.cleaner.convert_concept_id_to_curie("MESH:D013119", "Disease") == "MESH:D013119"
        assert self.cleaner.convert_concept_id_to_curie("CHEBI:12345", "Chemical") == "CHEBI:12345"
        
        # Invalid/missing concept IDs
        assert self.cleaner.convert_concept_id_to_curie("-", "Chemical") is None
        assert self.cleaner.convert_concept_id_to_curie("", "Disease") is None
        assert self.cleaner.convert_concept_id_to_curie("   ", "Gene") is None
        
        # Bare numbers with type context
        assert self.cleaner.convert_concept_id_to_curie("4932", "Species") == "NCBITaxon:4932"
        assert self.cleaner.convert_concept_id_to_curie("64030", "Gene") == "NCBIGene:64030"
        assert self.cleaner.convert_concept_id_to_curie("123456", "Disease") == "OMIM:123456"
        assert self.cleaner.convert_concept_id_to_curie("98765", "Chemical") == "UNKNOWN_CHEMICAL:98765"
        
        # Unknown type
        assert self.cleaner.convert_concept_id_to_curie("12345", "UnknownType") == "UNKNOWN_UNKNOWNTYPE:12345"
        
        # Non-numeric unusual formats
        result = self.cleaner.convert_concept_id_to_curie("weird_format", "Disease")
        assert result == "weird_format"  # Should return as-is with warning
    
    def test_process_file_streaming(self):
        """Test streaming file processing."""
        curie_to_pmids = self.cleaner.process_file_streaming()
        
        # Verify data extraction
        assert len(curie_to_pmids) > 0
        
        # Check specific CURIEs were extracted
        assert "MESH:D013119" in curie_to_pmids
        assert "MESH:D013167" in curie_to_pmids
        assert "NCBITaxon:4932" in curie_to_pmids
        assert "NCBIGene:64030" in curie_to_pmids
        assert "NCBIGene:24887" in curie_to_pmids
        
        # Check PMID grouping
        assert 40799000 in curie_to_pmids["MESH:D013119"]
        assert 11111111 in curie_to_pmids["MESH:D013119"] 
        assert 22222222 in curie_to_pmids["MESH:D013119"]
        
        # Verify invalid concept IDs were filtered out
        assert "-" not in curie_to_pmids
        
        # Check constructed CURIEs
        assert curie_to_pmids["NCBITaxon:4932"] == [40774000]
        assert curie_to_pmids["NCBIGene:64030"] == [40765000]
        
    def test_statistics_tracking(self):
        """Test that statistics are properly tracked."""
        self.cleaner.process_file_streaming()
        
        # Check type statistics
        assert self.cleaner.type_stats["Disease"] > 0
        assert self.cleaner.type_stats["Species"] > 0
        assert self.cleaner.type_stats["Gene"] > 0
        assert self.cleaner.type_stats["Chemical"] > 0
        
        # Check constructed CURIE tracking
        assert self.cleaner.constructed_curies["Species->NCBITaxon"] > 0
        assert self.cleaner.constructed_curies["Gene->NCBIGene"] > 0
        
        # Check invalid concept ID tracking
        assert self.cleaner.invalid_concept_ids["-"] > 0
    
    def test_malformed_line_handling(self):
        """Test handling of malformed lines."""
        # This should not crash and should log warnings
        curie_to_pmids = self.cleaner.process_file_streaming()
        
        # Should have extracted valid data despite malformed lines
        assert len(curie_to_pmids) > 0
        assert "MESH:D013119" in curie_to_pmids
    
    def test_write_jsonlines(self):
        """Test writing data to JSONLINES format."""
        test_data = [
            {"curie": "MESH:D013119", "original_curies": ["MESH:D013119"], "publications": ["PMID:40799000"]},
            {"curie": "NCBITaxon:4932", "original_curies": ["NCBITaxon:4932"], "publications": ["PMID:40774000"]}
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
    
    def test_write_statistics_report(self):
        """Test writing statistics report."""
        # Process data first to populate statistics
        self.cleaner.process_file_streaming()
        
        filename = "test_report.json"
        self.cleaner.write_statistics_report(filename)
        
        # Verify report file was created
        report_file = self.output_dir / filename
        assert report_file.exists()
        
        # Read and verify report structure
        with open(report_file) as f:
            report = json.load(f)
        
        assert "total_unique_curies" in report
        assert "entity_type_distribution" in report
        assert "curie_construction_patterns" in report
        assert "invalid_concept_ids" in report
        assert "normalization_failure_analysis" in report
        
        assert report["total_unique_curies"] > 0
        assert "Disease" in report["entity_type_distribution"]
        assert "Species->NCBITaxon" in report["curie_construction_patterns"]
    
    def test_full_pipeline_integration(self):
        """Test the complete cleaning pipeline with real API calls."""
        # Run the pipeline on our small test dataset
        self.cleaner.clean()
        
        # Verify output files were created
        cleaned_file = self.output_dir / "pubtator_cleaned.jsonl"
        failed_file = self.output_dir / "pubtator_failed_normalizations.jsonl"
        report_file = self.output_dir / "pubtator_processing_report.json"
        
        assert cleaned_file.exists()
        assert failed_file.exists()
        assert report_file.exists()
        
        # Read and verify cleaned output
        with open(cleaned_file) as f:
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
        
        # Read and verify report
        with open(report_file) as f:
            report = json.load(f)
            
        assert report["total_unique_curies"] > 0
        
        # Print results for inspection
        print(f"Pipeline produced {len(lines)} normalized entries")
        for item in lines[:5]:  # Show first 5 entries
            print(f"  {item['curie']}: {len(item['publications'])} publications, original: {item['original_curies']}")
        
        print(f"Total unique CURIEs processed: {report['total_unique_curies']}")
        print(f"Entity type distribution: {report['entity_type_distribution']}")
        print(f"CURIE construction patterns: {report['curie_construction_patterns']}")
        
    def test_empty_file(self):
        """Test handling of empty gzipped file."""
        # Create empty gzipped file
        empty_file = self.input_dir / "empty.gz"
        with gzip.open(empty_file, 'wt') as f:
            pass  # Write nothing
            
        cleaner = PubTatorCleaner(str(empty_file), str(self.output_dir))
        curie_to_pmids = cleaner.process_file_streaming()
        
        assert curie_to_pmids == {}
        
    def test_concept_id_edge_cases(self):
        """Test edge cases in concept ID conversion."""
        # Test various edge cases
        cleaner = self.cleaner
        
        # Whitespace handling
        assert cleaner.convert_concept_id_to_curie("  ", "Disease") is None
        assert cleaner.convert_concept_id_to_curie("\t", "Gene") is None
        
        # Mixed alphanumeric
        result = cleaner.convert_concept_id_to_curie("ABC123", "Chemical")
        assert result == "ABC123"  # Should return as-is
        
        # Numbers with leading zeros
        assert cleaner.convert_concept_id_to_curie("00123", "Species") == "NCBITaxon:00123"
        
        # Very long numbers
        assert cleaner.convert_concept_id_to_curie("999999999999", "Gene") == "NCBIGene:999999999999"
    
    def test_large_pmid_grouping(self):
        """Test that PMIDs are properly grouped by CURIE."""
        # Create file with many PMIDs for same CURIE
        test_file = self.input_dir / "grouping_test.gz"
        test_data = [
            f"{pmid}\tDisease\tMESH:D123456\ttest_disease\tPubTator3" 
            for pmid in range(1000, 1100)  # 100 PMIDs for same CURIE
        ]
        
        with gzip.open(test_file, 'wt') as f:
            for line in test_data:
                f.write(line + '\n')
        
        cleaner = PubTatorCleaner(str(test_file), str(self.output_dir))
        curie_to_pmids = cleaner.process_file_streaming()
        
        assert "MESH:D123456" in curie_to_pmids
        assert len(curie_to_pmids["MESH:D123456"]) == 100
        assert all(1000 <= pmid <= 1099 for pmid in curie_to_pmids["MESH:D123456"])