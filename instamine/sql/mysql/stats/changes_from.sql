SELECT ftt.name AS from_type,
       ttt.name AS to_type,
       COUNT(*)
FROM triad_change

JOIN triad AS ft ON ft.id = triad_change.from_triad
JOIN triad AS tt ON tt.id = triad_change.to_triad

JOIN triad_type AS ftt ON ftt.id = ft.triad_type_id
JOIN triad_type AS ttt ON ttt.id = tt.triad_type_id

WHERE ftt.name = %s
GROUP BY ttt.id;
