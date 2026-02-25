// Post-load name matching for GlobalPEP entities.
// Run after OpenSanctions pipeline to link unmatched PEPs to Person nodes.
//
// Phase 1 (CPF exact match) is handled in the pipeline itself.
// This script handles Phase 2 (exact name match) as a post-load step.

// Phase 2a: Exact normalized name match against known politicians.
// Restrict to persons with election candidacy records (high confidence).
// Unique-match-only rule: skip if multiple Person candidates share the name.
MATCH (g:GlobalPEP)
WHERE g.country = 'br'
  AND g.name IS NOT NULL
  AND NOT ()-[:GLOBAL_PEP_MATCH]->(g)
WITH g
MATCH (p:Person {name: g.name})
WHERE (p)-[:CANDIDATO_EM]->(:Election)
WITH g, collect(p) AS candidates
WHERE size(candidates) = 1
WITH g, candidates[0] AS p
MERGE (p)-[r:GLOBAL_PEP_MATCH]->(g)
SET r.match_type = 'exact_name_politician',
    r.confidence = 0.90;

// Phase 2b: Exact name match against all Person nodes (lower confidence).
// For PEPs that didn't match in Phase 2a (no candidacy record).
// Still requires unique match to avoid false positives.
MATCH (g:GlobalPEP)
WHERE g.country = 'br'
  AND g.name IS NOT NULL
  AND NOT ()-[:GLOBAL_PEP_MATCH]->(g)
WITH g
MATCH (p:Person {name: g.name})
WITH g, collect(p) AS candidates
WHERE size(candidates) = 1
WITH g, candidates[0] AS p
MERGE (p)-[r:GLOBAL_PEP_MATCH]->(g)
SET r.match_type = 'exact_name_unique',
    r.confidence = 0.85;
