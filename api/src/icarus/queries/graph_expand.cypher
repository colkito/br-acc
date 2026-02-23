MATCH path = (center)-[:SOCIO_DE|DOOU|CANDIDATO_EM|VENCEU|AUTOR_EMENDA|SANCIONADA*1..4]-(connected)
WHERE elementId(center) = $entity_id
  AND length(path) <= $depth
  AND NOT connected:User AND NOT connected:Investigation AND NOT connected:Annotation AND NOT connected:Tag
WITH center, relationships(path) AS rels, nodes(path) AS path_nodes
UNWIND path_nodes AS n
WITH center, rels, COLLECT(DISTINCT n) AS unique_nodes
UNWIND unique_nodes AS node
WITH center, rels, node, labels(node) AS node_labels, elementId(node) AS node_id
WHERE ($entity_types IS NULL
   OR ANY(label IN node_labels WHERE toLower(label) IN $entity_types))
  AND NOT node:User AND NOT node:Investigation AND NOT node:Annotation AND NOT node:Tag
RETURN DISTINCT
    node, node_labels, node_id,
    coalesce(node.cpf, node.cnpj, node.contract_id, node.sanction_id, node.amendment_id, elementId(node)) AS document_id,
    elementId(center) AS center_id
LIMIT 500
