MATCH (i:Investigation {share_token: $token})
OPTIONAL MATCH (i)-[:INCLUDES]->(e)
WITH i, collect(e.id) AS eids
RETURN i.id AS id,
       i.title AS title,
       i.description AS description,
       i.created_at AS created_at,
       i.updated_at AS updated_at,
       i.share_token AS share_token,
       [x IN eids WHERE x IS NOT NULL] AS entity_ids
