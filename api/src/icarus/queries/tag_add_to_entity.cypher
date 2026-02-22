MATCH (t:Tag {id: $tag_id})
MATCH (e) WHERE e.id = $entity_id
MERGE (t)-[:APPLIED_TO]->(e)
RETURN t.id AS tag_id, e.id AS entity_id
