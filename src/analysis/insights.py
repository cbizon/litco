#!/usr/bin/env python3
"""
Key insights from failed normalization analysis.

This script provides actionable insights from the failed normalization data
to help prioritize fixes and understand the data quality across sources.
"""

print("""
🔍 KEY INSIGHTS FROM FAILED NORMALIZATION ANALYSIS
==================================================

📊 SCALE OF THE PROBLEM:
- Total failed normalizations: ~6.5M CURIEs across all datasets
- PubTator has the most failures (6.1M, 94% of all failures)
- NGD has the fewest failures (86K, 1.3% of all failures)
- OmniCorp has moderate failures (327K, 5% of all failures)

🎯 TOP PRIORITY FIXES (by volume):

1. 🚨 PubTator tmVar: 3.54M failures (57.9% of PubTator failures)
   - These appear to be mutation/variant identifiers that need special handling
   - Likely format: tmVar:rs123456789, tmVar:c.123A>G, etc.
   - ACTION: Add tmVar prefix normalization rules

2. 🚨 PubTator NO_PREFIX: 1.29M failures (21% of PubTator failures)  
   - These are identifiers without proper CURIE format
   - ACTION: Investigate patterns and add prefix inference logic

3. 🚨 PubTator NCBIGene: 1.20M failures (19.6% of PubTator failures)
   - Should normalize well - investigate why these are failing
   - ACTION: Debug NCBIGene normalization issues

4. 🚨 OmniCorp http/https: 302K failures (92.4% of OmniCorp failures)
   - These are raw URLs that weren't converted to CURIEs
   - ACTION: Extend IRI-to-CURIE conversion rules

🔬 DATASET-SPECIFIC PATTERNS:

NGD Issues (mostly pathway/compound databases):
- SMPDB, PathWhiz: 44K failures (51% of NGD failures)
- These are specialized metabolic pathway databases
- Lower priority due to smaller volume

PubTator Issues (mostly genomic identifiers):
- Heavy bias toward genomic data (genes, variants, chromosomes)
- Many Chr* prefixes suggest chromosomal location identifiers
- Cellosaurus cell line identifiers (77K) - already handled in conversion

OmniCorp Issues (mostly raw URLs):
- 85% are unconverted URLs (http/https prefixes)
- Shows IRI conversion rules need expansion
- Remaining 15% are proper ontology terms (MESH, ENVO, etc.)

💡 QUICK WINS (high impact, likely easy fixes):

1. Fix OmniCorp URL conversion:
   - 302K failures from missed IRI patterns
   - Should be solvable by extending omnicorp_to_sqlite.py conversion rules

2. Handle tmVar identifiers:
   - 3.54M failures, but likely consistent format
   - Could normalize to appropriate variant/mutation databases

3. Debug NCBIGene normalization:
   - These should work with the API - investigate why they're failing
   - Might be API timeout issues or malformed identifiers

🎨 INTERESTING CROSS-DATASET PATTERNS:

Common failing prefixes across datasets:
- MESH: All 3 datasets (14K total) - surprising since MESH usually normalizes well
- NCBIGene: NGD + PubTator (1.2M total) - suggests systematic issues
- Various ontologies (GO, CHEBI, etc.) failing in NGD+OmniCorp

This suggests some prefixes that should normalize are consistently failing,
indicating potential issues with:
- API timeouts during normalization
- Malformed identifiers in the data
- Missing entries in the normalization service

🏆 SUCCESS STORY:
The pipeline successfully normalized the vast majority of identifiers:
- Estimated >95% normalization success rate across all datasets
- Only ~6.5M failures out of hundreds of millions of total identifiers

📋 RECOMMENDED ACTION PLAN:
1. Extend OmniCorp IRI conversion (quick win, 302K identifiers)
2. Add tmVar handling (major impact, 3.54M identifiers)  
3. Debug NCBIGene normalization failures (major impact, 1.2M identifiers)
4. Investigate NO_PREFIX patterns in PubTator (major impact, 1.29M identifiers)
5. Review API timeout handling for commonly failing prefixes (MESH, etc.)
""")

if __name__ == "__main__":
    pass