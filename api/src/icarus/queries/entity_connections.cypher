MATCH (e)-[r:SOCIO_DE|DOOU|CANDIDATO_EM|VENCEU|AUTOR_EMENDA|SANCIONADA]-(connected)
WHERE elementId(e) = $entity_id
  AND NOT connected:User AND NOT connected:Investigation AND NOT connected:Annotation AND NOT connected:Tag
RETURN e, r, connected,
       labels(e) AS source_labels,
       labels(connected) AS target_labels,
       type(r) AS rel_type,
       elementId(e) AS source_id,
       elementId(connected) AS target_id,
       elementId(r) AS rel_id
