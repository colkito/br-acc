MATCH (e) WHERE elementId(e) = $element_id
  AND (e:Person OR e:Company OR e:Contract OR e:Sanction OR e:Election OR e:Amendment)
RETURN e, labels(e) AS entity_labels
LIMIT 1
