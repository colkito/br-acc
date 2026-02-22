MATCH (i:Investigation {id: $id})
DETACH DELETE i
RETURN count(i) AS deleted
