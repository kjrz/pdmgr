SELECT from_triad_type.name AS from_triad_type,
       to_triad_type.name AS to_triad_type,
       count(*)
FROM triad_change

JOIN triad AS from_triad ON triad_change.from_triad = from_triad.id
JOIN triad_type AS from_triad_type ON from_triad.triad_type_id = from_triad_type.id

JOIN triad AS to_triad ON triad_change.to_triad = to_triad.id
JOIN triad_type AS to_triad_type ON to_triad.triad_type_id = to_triad_type.id

GROUP BY from_triad_type.id, to_triad_type.id;

