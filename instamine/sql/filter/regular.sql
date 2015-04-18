DELETE FROM following
WHERE followee_id IN
(SELECT followee_id
 FROM following
 JOIN user ON followee_id = id
 WHERE breed IS NOT 'regular'
);

DELETE FROM user
WHERE breed IS NOT 'regular';

