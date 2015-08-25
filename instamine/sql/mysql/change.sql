SELECT id, name FROM triad
JOIN triad_type ON (triad_type_id = triad_type.id)
WHERE first_seen < (SELECT MAX(fin) FROM effort)
  AND a_id IN (%s, %s, %s)
  AND b_id IN (%s, %s, %s)
  AND c_id IN (%s, %s, %s)
