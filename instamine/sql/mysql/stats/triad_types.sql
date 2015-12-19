SELECT triad_type.name, COUNT(triad.id)
FROM triad
JOIN triad_type ON triad_type.id = triad.triad_type_id
GROUP BY triad.triad_type_id
ORDER BY triad_type.id;
