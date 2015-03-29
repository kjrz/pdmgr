SELECT ab.follower_id AS a_id,
       ba.follower_id AS b_id,
       cb.follower_id AS c_id
           FROM following AS ab
           JOIN following AS ba ON ab.followee_id = ba.follower_id
                               AND ab.follower_id = ba.followee_id
           JOIN following AS bc ON ba.follower_id = bc.follower_id
           JOIN following AS cb ON cb.follower_id = bc.followee_id
                               AND cb.followee_id = bc.follower_id
           JOIN following AS ac ON ac.follower_id = ab.follower_id
                               AND ac.followee_id = cb.follower_id
LEFT OUTER JOIN following AS ca ON ca.follower_id = cb.follower_id
                               AND ca.followee_id = ab.follower_id
           JOIN user AS a ON a.id = a_id
           JOIN user AS b ON b.id = b_id
           JOIN user AS c ON c.id = c_id
WHERE ca.follower_id IS NULL
  AND a.breed = 'regular'
  AND b.breed = 'regular'
  AND c.breed = 'regular'
