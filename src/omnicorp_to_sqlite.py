#!/usr/bin/env python3
"""
Convert OmniCorp TSV files to NGD-compatible SQLite format.

This script uses a memory-efficient sort-based approach:
1. Pass 1: Stream through all TSV files, convert IRIs to CURIEs, write to temp file, sort
2. Pass 2: Stream through sorted file to aggregate PMIDs by CURIE into SQLite

Output SQLite has same schema as NGD: curie_to_pmids table with curie|pmids columns.
"""

import json
import logging
import re
import sqlite3
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class OmniCorpToSQLiteConverter:
    """Convert OmniCorp TSV files to NGD-compatible SQLite format."""
    
    def __init__(self, input_dir: str, output_sqlite: str):
        self.input_dir = Path(input_dir)
        self.output_sqlite = Path(output_sqlite)
        
        # Statistics tracking
        self.stats = {
            'files_processed': 0,
            'lines_processed': 0,
            'valid_pairs': 0,
            'invalid_urls': 0,
            'iri_conversions': defaultdict(int)
        }
        
        # Track unknown patterns for analysis
        self.unknown_iris = set()
    
    def extract_pmid_from_url(self, pubmed_url: str) -> Optional[int]:
        """Extract PMID from PubMed URL."""
        # URL format: https://www.ncbi.nlm.nih.gov/pubmed/3963809
        match = re.search(r'/pubmed/(\d+)$', pubmed_url)
        if match:
            return int(match.group(1))
        return None
    
    def convert_iri_to_curie(self, iri: str) -> str:
        """Convert IRI to CURIE format based on current omnicorp logic."""
        # CHEBI: http://purl.obolibrary.org/obo/CHEBI_17822 -> CHEBI:17822
        if 'purl.obolibrary.org/obo/CHEBI_' in iri:
            self.stats['iri_conversions']['CHEBI'] += 1
            return iri.replace('http://purl.obolibrary.org/obo/CHEBI_', 'CHEBI:')
        
        # MESH: http://id.nlm.nih.gov/mesh/D014346 -> MESH:D014346  
        if 'id.nlm.nih.gov/mesh/' in iri:
            self.stats['iri_conversions']['MESH'] += 1
            return iri.replace('http://id.nlm.nih.gov/mesh/', 'MESH:')
        
        # Other OBO terms: http://purl.obolibrary.org/obo/RO_0002432 -> RO:0002432
        obo_match = re.search(r'purl\.obolibrary\.org/obo/([A-Z]+)_(\d+)', iri)
        if obo_match:
            prefix = obo_match.group(1)
            number = obo_match.group(2)
            self.stats['iri_conversions'][prefix] += 1
            return f"{prefix}:{number}"
        
        # Additional OBO patterns with different separators
        # NCBITaxon: http://purl.obolibrary.org/obo/NCBITaxon_10 -> NCBITaxon:10
        ncbi_taxon_match = re.search(r'purl\.obolibrary\.org/obo/NCBITaxon_(\d+)', iri)
        if ncbi_taxon_match:
            number = ncbi_taxon_match.group(1)
            self.stats['iri_conversions']['NCBITaxon'] += 1
            return f"NCBITaxon:{number}"
        
        # NCIT: http://purl.obolibrary.org/obo/NCIT_C12378 -> NCIT:C12378
        ncit_match = re.search(r'purl\.obolibrary\.org/obo/NCIT_(C\d+)', iri)
        if ncit_match:
            code = ncit_match.group(1)
            self.stats['iri_conversions']['NCIT'] += 1
            return f"NCIT:{code}"
        
        # PR (Protein Ontology): http://purl.obolibrary.org/obo/PR_A0A0R4IGV4 -> PR:A0A0R4IGV4
        pr_match = re.search(r'purl\.obolibrary\.org/obo/PR_([A-Z0-9_]+)', iri)
        if pr_match:
            code = pr_match.group(1)
            self.stats['iri_conversions']['PR'] += 1
            return f"PR:{code}"
        
        # dictyBase: http://dictybase.org/gene/DDB_G0268618 -> dictyBase:DDB_G0268618
        if 'dictybase.org/gene/' in iri:
            gene_id = iri.split('/gene/')[-1]
            self.stats['iri_conversions']['dictyBase'] += 1
            return f"dictyBase:{gene_id}"
        
        # FlyBase: http://flybase.org/reports/FBgn0013717 -> FB:FBgn0013717
        if 'flybase.org/reports/' in iri:
            gene_id = iri.split('/reports/')[-1]
            self.stats['iri_conversions']['FB'] += 1
            return f"FB:{gene_id}"
        
        # HGNC: http://identifiers.org/hgnc/10001 -> HGNC:10001
        if 'identifiers.org/hgnc/' in iri:
            gene_id = iri.split('/hgnc/')[-1]
            self.stats['iri_conversions']['HGNC'] += 1
            return f"HGNC:{gene_id}"
        
        # HGNC (genenames.org old format): http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=10044 -> HGNC:10044
        hgnc_old_match = re.search(r'genenames\.org/cgi-bin/gene_symbol_report\?hgnc_id=(\d+)', iri)
        if hgnc_old_match:
            gene_id = hgnc_old_match.group(1)
            self.stats['iri_conversions']['HGNC'] += 1
            return f"HGNC:{gene_id}"
        
        # HGNC (genenames.org new format): https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/HGNC:10383 -> HGNC:10383
        hgnc_new_match = re.search(r'genenames\.org/data/gene-symbol-report/#!/hgnc_id/HGNC:(\d+)', iri)
        if hgnc_new_match:
            gene_id = hgnc_new_match.group(1)
            self.stats['iri_conversions']['HGNC'] += 1
            return f"HGNC:{gene_id}"
        
        # RGD: http://rgd.mcw.edu/rgdweb/report/gene/main.html?id=11414885 -> RGD:11414885
        rgd_match = re.search(r'rgd\.mcw\.edu/rgdweb/report/gene/main\.html\?id=(\d+)', iri)
        if rgd_match:
            gene_id = rgd_match.group(1)
            self.stats['iri_conversions']['RGD'] += 1
            return f"RGD:{gene_id}"
        
        # EFO: http://www.ebi.ac.uk/efo/EFO_0000174 -> EFO:0000174
        if 'ebi.ac.uk/efo/EFO_' in iri:
            efo_id = iri.split('/EFO_')[-1]
            self.stats['iri_conversions']['EFO'] += 1
            return f"EFO:{efo_id}"
        
        # MGI: http://www.informatics.jax.org/marker/MGI:101783 -> MGI:101783
        if 'informatics.jax.org/marker/MGI:' in iri:
            mgi_id = iri.split('/marker/')[-1]  # Already includes MGI: prefix
            self.stats['iri_conversions']['MGI'] += 1
            return mgi_id
        
        # NCBIGene: http://www.ncbi.nlm.nih.gov/gene/100135518 -> NCBIGene:100135518
        if 'ncbi.nlm.nih.gov/gene/' in iri:
            gene_id = iri.split('/gene/')[-1]
            self.stats['iri_conversions']['NCBIGene'] += 1
            return f"NCBIGene:{gene_id}"
        
        # Orphanet: http://www.orpha.net/ORDO/Orphanet_101000 -> orphanet:101000
        orphanet_match = re.search(r'orpha\.net/ORDO/Orphanet_(\d+)', iri)
        if orphanet_match:
            orpha_id = orphanet_match.group(1)
            self.stats['iri_conversions']['orphanet'] += 1
            return f"orphanet:{orpha_id}"
        
        # WormBase: http://www.wormbase.org/species/c_elegans/gene/WBGene00007403 -> WormBase:WBGene00007403
        wormbase_match = re.search(r'wormbase\.org/species/c_elegans/gene/(WBGene\d+)', iri)
        if wormbase_match:
            gene_id = wormbase_match.group(1)
            self.stats['iri_conversions']['WormBase'] += 1
            return f"WormBase:{gene_id}"
        
        # SGD (Yeast): http://www.yeastgenome.org/locus/S000003272 -> SGD:S000003272
        if 'yeastgenome.org/locus/' in iri:
            locus_id = iri.split('/locus/')[-1]
            self.stats['iri_conversions']['SGD'] += 1
            return f"SGD:{locus_id}"
        
        # ZFIN: http://zfin.org/action/marker/view/ZDB-GENE-001222-1 -> ZFIN:ZDB-GENE-001222-1
        if 'zfin.org/action/marker/view/' in iri:
            gene_id = iri.split('/view/')[-1]
            self.stats['iri_conversions']['ZFIN'] += 1
            return f"ZFIN:{gene_id}"
        
        # Unknown format - collect for analysis and return as-is
        self.stats['iri_conversions']['UNKNOWN'] += 1
        self.unknown_iris.add(iri)
        
        return iri
    
    def pass1_extract_and_sort(self) -> Path:
        """Pass 1: Extract CURIE-PMID pairs from all TSV files using per-file sorting and merge."""
        logger.info("Pass 1: Extracting and sorting CURIE-PMID pairs from all TSV files")
        
        # Find all TSV files
        tsv_files = list(self.input_dir.glob("*.tsv"))
        if not tsv_files:
            raise ValueError(f"No .tsv files found in {self.input_dir}")
        
        self.stats['files_processed'] = len(tsv_files)
        logger.info(f"Found {self.stats['files_processed']} TSV files to process")
        
        sorted_temp_files = []
        
        try:
            # Process each TSV file individually
            for file_idx, file_path in enumerate(sorted(tsv_files), 1):
                logger.info(f"Processing {file_path.name} ({file_idx}/{len(tsv_files)})...")
                
                # Collect all CURIE-PMID pairs from this file in memory
                file_pairs = []
                
                with open(file_path, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        self.stats['lines_processed'] += 1
                        
                        line = line.strip()
                        if not line:
                            continue
                        
                        # Parse TSV line: pubmed_url \t entity_iri
                        parts = line.split('\t')
                        if len(parts) != 2:
                            continue
                        
                        pubmed_url, entity_iri = parts
                        
                        # Extract PMID from URL
                        pmid = self.extract_pmid_from_url(pubmed_url)
                        if pmid is None:
                            self.stats['invalid_urls'] += 1
                            continue
                        
                        # Convert IRI to CURIE
                        curie = self.convert_iri_to_curie(entity_iri)
                        
                        # Store pair in memory for this file
                        file_pairs.append((curie, pmid))
                        self.stats['valid_pairs'] += 1
                
                # Sort pairs from this file in memory
                file_pairs.sort(key=lambda x: x[0])  # Sort by CURIE
                
                # Write sorted pairs to temporary file for this input file
                temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.sorted.tsv', delete=False)
                temp_path = Path(temp_file.name)
                
                for curie, pmid in file_pairs:
                    temp_file.write(f"{curie}\t{pmid}\n")
                
                temp_file.close()
                sorted_temp_files.append(temp_path)
                
                logger.info(f"  Processed {len(file_pairs):,} pairs from {file_path.name}")
            
            logger.info(f"Pass 1a complete: {len(sorted_temp_files)} sorted temp files created")
            logger.info(f"Pass 1a stats: {self.stats['lines_processed']:,} lines processed, {self.stats['valid_pairs']:,} valid pairs extracted")
            
            # Multi-way merge all sorted temp files
            final_sorted = Path(tempfile.mktemp(suffix='.final_sorted.tsv'))
            logger.info(f"Pass 1b: Merging {len(sorted_temp_files)} sorted files...")
            
            # Use sort --merge to efficiently merge all sorted temp files
            subprocess.run([
                'sort', '--merge', '-k1,1'
            ] + [str(f) for f in sorted_temp_files] + [
                '-o', str(final_sorted)
            ], check=True)
            
            # Clean up individual sorted temp files
            for temp_file in sorted_temp_files:
                temp_file.unlink()
            
            logger.info(f"Pass 1 complete: final sorted file created at {final_sorted}")
            return final_sorted
            
        except Exception as e:
            # Clean up any temp files on error
            for temp_file in sorted_temp_files:
                if temp_file.exists():
                    temp_file.unlink()
            raise e
    
    def pass2_aggregate_to_sqlite(self, sorted_file: Path):
        """Pass 2: Aggregate sorted CURIE-PMID pairs into SQLite database."""
        logger.info("Pass 2: Aggregating to SQLite format")
        
        # Create output SQLite database
        self.output_sqlite.parent.mkdir(parents=True, exist_ok=True)
        if self.output_sqlite.exists():
            self.output_sqlite.unlink()
        
        conn = sqlite3.connect(str(self.output_sqlite))
        cursor = conn.cursor()
        
        # Create table with same schema as NGD
        cursor.execute('''
            CREATE TABLE curie_to_pmids (
                curie TEXT PRIMARY KEY,
                pmids TEXT
            )
        ''')
        
        current_curie = None
        current_pmids = []
        records_written = 0
        
        try:
            with open(sorted_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    if line_num % 10000000 == 0:
                        logger.info(f"Pass 2: {line_num:,} lines processed, {records_written:,} records written")
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split('\t')
                    if len(parts) != 2:
                        continue
                    
                    curie, pmid_str = parts
                    try:
                        pmid = int(pmid_str)
                    except ValueError:
                        continue
                    
                    # Check if we're still on the same CURIE
                    if curie == current_curie:
                        current_pmids.append(pmid)
                    else:
                        # Write previous CURIE if we have one
                        if current_curie is not None:
                            pmids_json = json.dumps(sorted(set(current_pmids)))  # Dedupe and sort
                            cursor.execute(
                                'INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)',
                                (current_curie, pmids_json)
                            )
                            records_written += 1
                            
                            if records_written % 1000000 == 0:
                                conn.commit()  # Periodic commits
                        
                        # Start new CURIE
                        current_curie = curie
                        current_pmids = [pmid]
                
                # Don't forget the last CURIE
                if current_curie is not None:
                    pmids_json = json.dumps(sorted(set(current_pmids)))
                    cursor.execute(
                        'INSERT INTO curie_to_pmids (curie, pmids) VALUES (?, ?)',
                        (current_curie, pmids_json)
                    )
                    records_written += 1
            
            conn.commit()
            logger.info(f"Pass 2 complete: {records_written:,} records written to SQLite")
            
        finally:
            conn.close()
            # Clean up sorted temp file
            if sorted_file.exists():
                sorted_file.unlink()
    
    def log_statistics(self):
        """Log processing statistics."""
        logger.info("=== CONVERSION STATISTICS ===")
        logger.info(f"Files processed: {self.stats['files_processed']:,}")
        logger.info(f"Total lines processed: {self.stats['lines_processed']:,}")
        logger.info(f"Valid CURIE-PMID pairs: {self.stats['valid_pairs']:,}")
        logger.info(f"Invalid PubMed URLs: {self.stats['invalid_urls']:,}")
        
        if self.stats['iri_conversions']:
            logger.info("IRI conversion patterns:")
            for pattern, count in sorted(self.stats['iri_conversions'].items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {pattern}: {count:,}")
    
    def convert(self):
        """Run the complete conversion pipeline."""
        logger.info(f"Converting OmniCorp TSV files to SQLite: {self.input_dir} -> {self.output_sqlite}")
        
        # Pass 1: Extract and sort
        sorted_file = self.pass1_extract_and_sort()
        
        # Write unknown patterns after Pass 1 (before Pass 2 in case it fails)
        self.write_unknown_patterns()
        
        # Pass 2: Aggregate to SQLite
        self.pass2_aggregate_to_sqlite(sorted_file)
        
        # Log statistics
        self.log_statistics()
        
        logger.info("Conversion completed successfully")
    
    def write_unknown_patterns(self):
        """Write unknown IRI patterns to file for analysis."""
        if self.unknown_iris:
            output_dir = Path("cleaned/omnicorp")
            output_dir.mkdir(parents=True, exist_ok=True)
            unknown_file = output_dir / "omnicorp_unknown_iris.txt"
            
            logger.info(f"Writing {len(self.unknown_iris)} unknown IRI patterns to {unknown_file}")
            with open(unknown_file, 'w') as f:
                for iri in sorted(self.unknown_iris):
                    f.write(iri + '\n')
        else:
            logger.info("No unknown IRI patterns to write")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert OmniCorp TSV files to NGD-compatible SQLite')
    parser.add_argument('input_dir', help='Directory containing OmniCorp TSV files')
    parser.add_argument('output_sqlite', help='Output SQLite database file')
    
    args = parser.parse_args()
    
    converter = OmniCorpToSQLiteConverter(args.input_dir, args.output_sqlite)
    converter.convert()


if __name__ == "__main__":
    main()