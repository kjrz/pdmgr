SELECT to_triad.a_id a, to_triad.b_id b, to_triad.c_id c,
       from_type.name old, to_type.name new, to_triad.first_seen since
FROM triad_change
JOIN triad from_triad ON triad_change.from_triad=from_triad.id
JOIN triad to_triad ON triad_change.to_triad=to_triad.id
JOIN triad_type from_type ON from_triad.triad_type_id=from_type.id
JOIN triad_type to_type ON to_triad.triad_type_id=to_type.id
