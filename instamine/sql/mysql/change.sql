SELECT id FROM triad
WHERE first_seen < (SELECT MAX(fin) FROM effort)
  AND a_id IN (%s, %s, %s)
  AND b_id IN (%s, %s, %s)
  AND c_id IN (%s, %s, %s)
