#!/usr/bin/env python3
"""
Analyze completely missing drugs and diseases from indications/contraindications lists.

This script identifies entities that are not found in any of our three datasets
(NGD, PubTator, OmniCorp) even after normalization, then investigates why
they might be missing by searching PubMed for their labels.
"""

import pandas as pd
import logging
import json
from pathlib import Path
from typing import List, Dict, Tuple

try:
    from .pubmed_search import PubMedSearcher
    from .pmid_entity_lookup import PMIDEntityLookup
except ImportError:
    # Running as script directly
    from pubmed_search import PubMedSearcher
    from pmid_entity_lookup import PMIDEntityLookup

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MissingEntityAnalyzer:
    """Analyze completely missing drugs and diseases."""
    
    def __init__(self):
        self.output_dir = Path('analysis_results/missing_entities')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Storage for missing entities
        self.missing_drugs = []
        self.missing_diseases = []
        
        # Initialize search and lookup utilities
        self.pubmed_searcher = PubMedSearcher()
        self.pmid_lookup = PMIDEntityLookup()
    
    def extract_missing_entities(self):
        """Extract drugs and diseases that are completely missing from all datasets."""
        logger.info("Extracting completely missing entities from coverage results")
        
        # Process drugs
        drugs_file = Path("analysis_results/medi_inspection/medi_drugs_robokop_comparison.csv")
        if drugs_file.exists():
            logger.info(f"Processing {drugs_file}")
            drugs_df = pd.read_csv(drugs_file)
            
            # Find drugs where all renormalized presence columns are False
            missing_mask = (
                (drugs_df['ngd_renorm_present'] == False) & 
                (drugs_df['pubtator_renorm_present'] == False) & 
                (drugs_df['omnicorp_renorm_present'] == False)
            )
            
            missing_drugs_df = drugs_df[missing_mask]
            self.missing_drugs = missing_drugs_df[['drug_id', 'label']].to_dict('records')
            
            logger.info(f"Found {len(self.missing_drugs)} completely missing drugs")
        else:
            logger.warning(f"Drugs coverage file not found: {drugs_file}")
        
        # Process diseases
        diseases_file = Path("analysis_results/medi_inspection/medi_diseases_robokop_comparison.csv")
        if diseases_file.exists():
            logger.info(f"Processing {diseases_file}")
            diseases_df = pd.read_csv(diseases_file)
            
            # Find diseases where all renormalized presence columns are False
            missing_mask = (
                (diseases_df['ngd_renorm_present'] == False) & 
                (diseases_df['pubtator_renorm_present'] == False) & 
                (diseases_df['omnicorp_renorm_present'] == False)
            )
            
            missing_diseases_df = diseases_df[missing_mask]
            self.missing_diseases = missing_diseases_df[['disease_id', 'label']].to_dict('records')
            
            logger.info(f"Found {len(self.missing_diseases)} completely missing diseases")
        else:
            logger.warning(f"Diseases coverage file not found: {diseases_file}")
    
    def save_missing_entities_summary(self):
        """Save summary of missing entities for review."""
        logger.info("Saving missing entities summary")
        
        # Save missing drugs
        if self.missing_drugs:
            drugs_df = pd.DataFrame(self.missing_drugs)
            drugs_output = self.output_dir / 'missing_drugs.csv'
            drugs_df.to_csv(drugs_output, index=False)
            logger.info(f"Saved {len(self.missing_drugs)} missing drugs to {drugs_output}")
        
        # Save missing diseases
        if self.missing_diseases:
            diseases_df = pd.DataFrame(self.missing_diseases)
            diseases_output = self.output_dir / 'missing_diseases.csv'
            diseases_df.to_csv(diseases_output, index=False)
            logger.info(f"Saved {len(self.missing_diseases)} missing diseases to {diseases_output}")
        
        # Generate summary report
        summary_lines = [
            "=" * 80,
            "MISSING ENTITIES SUMMARY",
            "=" * 80,
            "",
            f"📊 OVERALL STATISTICS:",
            f"  Missing drugs:    {len(self.missing_drugs):>6}",
            f"  Missing diseases: {len(self.missing_diseases):>6}",
            f"  Total missing:    {len(self.missing_drugs) + len(self.missing_diseases):>6}",
            "",
            "🔍 NEXT STEPS:",
            "1. Run PubMed search for these entity labels",
            "2. Get top 5 PMIDs for each missing entity",
            "3. Check what entities our datasets contain for those PMIDs",
            "4. Generate gap analysis report",
            "",
        ]
        
        # Add sample missing entities for review
        if self.missing_drugs:
            summary_lines.extend([
                "📋 SAMPLE MISSING DRUGS:",
                "-" * 40
            ])
            for i, drug in enumerate(self.missing_drugs[:10]):
                summary_lines.append(f"  {drug['drug_id']}: {drug['label']}")
            if len(self.missing_drugs) > 10:
                summary_lines.append(f"  ... and {len(self.missing_drugs) - 10} more")
            summary_lines.append("")
        
        if self.missing_diseases:
            summary_lines.extend([
                "📋 SAMPLE MISSING DISEASES:",
                "-" * 40
            ])
            for i, disease in enumerate(self.missing_diseases[:10]):
                summary_lines.append(f"  {disease['disease_id']}: {disease['label']}")
            if len(self.missing_diseases) > 10:
                summary_lines.append(f"  ... and {len(self.missing_diseases) - 10} more")
            summary_lines.append("")
        
        summary_lines.append("=" * 80)
        
        # Write summary
        summary_output = self.output_dir / 'missing_entities_summary.txt'
        with open(summary_output, 'w') as f:
            f.write('\n'.join(summary_lines))
        
        logger.info(f"Summary report saved to {summary_output}")
        
        # Print summary to console
        print('\n'.join(summary_lines))
    
    def run_phase_2(self, max_entities_per_type: int = 50):
        """Run Phase 2: Search PubMed for missing entity labels."""
        logger.info("Starting Phase 2: PubMed search for missing entities")
        
        # Limit the number of entities to process for initial testing
        drugs_to_search = self.missing_drugs[:max_entities_per_type]
        diseases_to_search = self.missing_diseases[:max_entities_per_type]
        
        logger.info(f"Searching PubMed for {len(drugs_to_search)} drugs and {len(diseases_to_search)} diseases")
        
        # Search for drugs
        drug_search_results = {}
        if drugs_to_search:
            logger.info("Searching for missing drugs...")
            drug_search_results = self.pubmed_searcher.batch_search_entities(drugs_to_search)
        
        # Search for diseases  
        disease_search_results = {}
        if diseases_to_search:
            logger.info("Searching for missing diseases...")
            disease_search_results = self.pubmed_searcher.batch_search_entities(diseases_to_search)
        
        # Save search results
        search_results = {
            'drugs': drug_search_results,
            'diseases': disease_search_results,
            'search_metadata': {
                'total_drugs_searched': len(drugs_to_search),
                'total_diseases_searched': len(diseases_to_search),
                'successful_drug_searches': sum(1 for r in drug_search_results.values() if r['search_successful']),
                'successful_disease_searches': sum(1 for r in disease_search_results.values() if r['search_successful'])
            }
        }
        
        search_output = self.output_dir / 'pubmed_search_results.json'
        with open(search_output, 'w') as f:
            json.dump(search_results, f, indent=2)
        
        logger.info(f"PubMed search results saved to {search_output}")
        logger.info("Phase 2 complete!")
        
        return search_results
    
    def run_phase_3(self, search_results: Dict):
        """Run Phase 3: Cross-reference PMIDs with our dataset contents."""
        logger.info("Starting Phase 3: Cross-reference PMIDs with dataset contents")
        
        # Collect all PMIDs from search results
        all_pmids = set()
        
        for entity_type in ['drugs', 'diseases']:
            for entity_id, result in search_results[entity_type].items():
                all_pmids.update(result['pmids'])
        
        logger.info(f"Looking up entities for {len(all_pmids)} PMIDs across our datasets")
        
        # Look up entities for all PMIDs
        pmid_entity_results = self.pmid_lookup.batch_lookup_pmids(list(all_pmids))
        
        # Analyze patterns
        analysis = self.pmid_lookup.analyze_pmid_entity_patterns(pmid_entity_results)
        
        # Convert sets to lists for JSON serialization
        serializable_pmid_results = {}
        for pmid, dataset_results in pmid_entity_results.items():
            serializable_pmid_results[pmid] = {
                dataset: list(entities) for dataset, entities in dataset_results.items()
            }
        
        # Save PMID lookup results
        pmid_results = {
            'pmid_entities': serializable_pmid_results,
            'analysis': analysis
        }
        
        pmid_output = self.output_dir / 'pmid_entity_lookup_results.json'
        with open(pmid_output, 'w') as f:
            json.dump(pmid_results, f, indent=2)
        
        logger.info(f"PMID entity lookup results saved to {pmid_output}")
        logger.info("Phase 3 complete!")
        
        return pmid_results
    
    def run_phase_4(self, search_results: Dict, pmid_results: Dict):
        """Run Phase 4: Generate comprehensive gap analysis report."""
        logger.info("Starting Phase 4: Generate gap analysis report")
        
        # Create comprehensive analysis
        gap_analysis = self._generate_gap_analysis(search_results, pmid_results)
        
        # Save detailed analysis
        analysis_output = self.output_dir / 'gap_analysis_report.json'
        with open(analysis_output, 'w') as f:
            json.dump(gap_analysis, f, indent=2)
        
        # Generate human-readable report
        self._generate_readable_report(gap_analysis)
        
        logger.info("Phase 4 complete!")
        return gap_analysis
    
    def _generate_gap_analysis(self, search_results: Dict, pmid_results: Dict) -> Dict:
        """Generate comprehensive gap analysis."""
        analysis = {
            'summary': {
                'total_missing_entities': len(self.missing_drugs) + len(self.missing_diseases),
                'missing_drugs': len(self.missing_drugs),
                'missing_diseases': len(self.missing_diseases),
                'entities_found_in_pubmed': 0,
                'pmids_analyzed': len(pmid_results.get('pmid_entities', {})),
                'entities_found_in_our_data': pmid_results.get('analysis', {}).get('total_entities_found', {})
            },
            'entity_gaps': [],
            'dataset_coverage_gaps': {},
            'recommendations': []
        }
        
        # Count entities found in PubMed
        for entity_type in ['drugs', 'diseases']:
            for entity_id, result in search_results[entity_type].items():
                if result['search_successful']:
                    analysis['summary']['entities_found_in_pubmed'] += 1
                    
                    # Analyze this specific entity
                    entity_analysis = {
                        'entity_id': entity_id,
                        'entity_type': entity_type,
                        'label': result['label'],
                        'pmids_found': result['pmids'],
                        'entities_in_our_data': {}
                    }
                    
                    # Check what we found for each PMID
                    for pmid in result['pmids']:
                        if pmid in pmid_results.get('pmid_entities', {}):
                            entity_analysis['entities_in_our_data'][pmid] = pmid_results['pmid_entities'][pmid]
                    
                    analysis['entity_gaps'].append(entity_analysis)
        
        return analysis
    
    def _generate_readable_report(self, gap_analysis: Dict):
        """Generate human-readable gap analysis report."""
        report_lines = [
            "=" * 80,
            "MISSING ENTITIES GAP ANALYSIS REPORT",
            "=" * 80,
            "",
            "📊 SUMMARY:",
            f"  Total missing entities: {gap_analysis['summary']['total_missing_entities']:,}",
            f"  Missing drugs: {gap_analysis['summary']['missing_drugs']:,}",
            f"  Missing diseases: {gap_analysis['summary']['missing_diseases']:,}",
            f"  Found in PubMed: {gap_analysis['summary']['entities_found_in_pubmed']:,}",
            f"  PMIDs analyzed: {gap_analysis['summary']['pmids_analyzed']:,}",
            "",
            "🔍 KEY FINDINGS:",
            ""
        ]
        
        # Add findings about what entities we found
        entities_found = gap_analysis['summary']['entities_found_in_our_data']
        for dataset, count in entities_found.items():
            if count > 0:
                report_lines.append(f"  {dataset}: Found {count:,} entities in our data for analyzed PMIDs")
        
        if not any(entities_found.values()):
            report_lines.append("  ⚠️  No entities found in our datasets for any analyzed PMIDs")
        
        report_lines.extend([
            "",
            "💡 NEXT STEPS:",
            "1. Review specific entity gaps in gap_analysis_report.json",
            "2. Investigate why certain entities are missing despite PubMed presence",
            "3. Consider expanding data sources or improving entity extraction",
            "",
            "=" * 80
        ])
        
        # Write report
        report_output = self.output_dir / 'gap_analysis_summary.txt'
        with open(report_output, 'w') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"Gap analysis summary saved to {report_output}")
        
        # Print to console
        print('\n'.join(report_lines))
    
    def run_phase_1(self):
        """Run Phase 1: Extract completely missing entities."""
        logger.info("Starting Phase 1: Extract completely missing entities")
        
        self.extract_missing_entities()
        self.save_missing_entities_summary()
        
        logger.info("Phase 1 complete!")
        return len(self.missing_drugs), len(self.missing_diseases)
    
    def run_complete_analysis(self, max_entities_per_type: int = 20):
        """Run the complete missing entity analysis pipeline."""
        logger.info("Starting complete missing entity analysis")
        
        # Phase 1: Extract missing entities
        self.run_phase_1()
        
        # Phase 2: Search PubMed
        search_results = self.run_phase_2(max_entities_per_type)
        
        # Phase 3: Cross-reference PMIDs
        pmid_results = self.run_phase_3(search_results)
        
        # Phase 4: Generate analysis report
        gap_analysis = self.run_phase_4(search_results, pmid_results)
        
        logger.info("Complete missing entity analysis finished!")
        return gap_analysis


def main():
    """Main function to run complete missing entity analysis."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Analyze missing drugs and diseases from indications/contraindications lists",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default 20 entities per type (40 total)
  uv run python src/analysis/missing_entity_analysis.py
  
  # Run with 50 entities per type (100 total)
  uv run python src/analysis/missing_entity_analysis.py --max-entities 50
  
  # Run on all missing entities (721 total)
  uv run python src/analysis/missing_entity_analysis.py --max-entities 500
  
  # Run individual phases
  uv run python src/analysis/missing_entity_analysis.py --phase 1
  uv run python src/analysis/missing_entity_analysis.py --phase 2 --max-entities 50
        """
    )
    
    parser.add_argument(
        "--max-entities", 
        type=int, 
        default=20,
        help="Maximum number of entities per type (drugs/diseases) to process (default: 20)"
    )
    
    parser.add_argument(
        "--phase",
        type=int,
        choices=[1, 2, 3, 4],
        help="Run only a specific phase (1=extract, 2=pubmed, 3=lookup, 4=report)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Set up logging
    if args.verbose:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.WARNING, format='%(levelname)s - %(message)s')
    
    analyzer = MissingEntityAnalyzer()
    
    if args.phase:
        # Run individual phase
        if args.phase == 1:
            print("Running Phase 1: Extract missing entities...")
            missing_drugs, missing_diseases = analyzer.run_phase_1()
            print(f"✓ Found {missing_drugs} missing drugs and {missing_diseases} missing diseases")
            
        elif args.phase == 2:
            print("Running Phase 2: PubMed search...")
            analyzer.extract_missing_entities()  # Reload entities
            search_results = analyzer.run_phase_2(args.max_entities)
            successful_searches = sum(1 for r in search_results['drugs'].values() if r['search_successful'])
            successful_searches += sum(1 for r in search_results['diseases'].values() if r['search_successful'])
            print(f"✓ Completed PubMed searches: {successful_searches} successful")
            
        elif args.phase == 3:
            print("Running Phase 3: PMID entity lookup...")
            import json
            with open(analyzer.output_dir / 'pubmed_search_results.json') as f:
                search_results = json.load(f)
            pmid_results = analyzer.run_phase_3(search_results)
            print(f"✓ Analyzed {pmid_results['analysis']['total_pmids']} PMIDs")
            
        elif args.phase == 4:
            print("Running Phase 4: Generate gap analysis report...")
            import json
            with open(analyzer.output_dir / 'pubmed_search_results.json') as f:
                search_results = json.load(f)
            with open(analyzer.output_dir / 'pmid_entity_lookup_results.json') as f:
                pmid_results = json.load(f)
            gap_analysis = analyzer.run_phase_4(search_results, pmid_results)
            print(f"✓ Generated gap analysis for {gap_analysis['summary']['entities_found_in_pubmed']} entities")
    
    else:
        # Run complete analysis
        print(f"Running complete missing entity analysis with {args.max_entities} entities per type...")
        print(f"Total entities to process: {args.max_entities * 2}")
        print("This will take approximately:")
        print(f"  - Phase 1: < 1 second")
        print(f"  - Phase 2: ~{args.max_entities * 2 * 0.5 / 60:.1f} minutes (PubMed API)")
        print(f"  - Phase 3: ~2-3 minutes (dataset lookup)")
        print(f"  - Phase 4: < 1 second")
        print()
        
        gap_analysis = analyzer.run_complete_analysis(max_entities_per_type=args.max_entities)
        
        print("\n" + "="*80)
        print("ANALYSIS COMPLETE!")
        print("="*80)
        print(f"📁 Results saved in: {analyzer.output_dir}")
        print(f"📊 Total missing entities: {gap_analysis['summary']['total_missing_entities']}")
        print(f"🔍 Entities found in PubMed: {gap_analysis['summary']['entities_found_in_pubmed']}")
        print(f"📋 PMIDs analyzed: {gap_analysis['summary']['pmids_analyzed']}")
        print()
        print("Key findings:")
        for dataset, count in gap_analysis['summary']['entities_found_in_our_data'].items():
            print(f"  {dataset}: {count} entities found in our data")
        print()
        print("📄 Review detailed results in:")
        print(f"  - gap_analysis_summary.txt")
        print(f"  - gap_analysis_report.json")
        print("="*80)


if __name__ == "__main__":
    main()