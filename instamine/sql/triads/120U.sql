SELECT ab.follower_id AS a_id,
       ab.followee_id AS b_id,
       ac.followee_id AS c_id
           FROM following AS ab
           JOIN following AS ba ON ab.followee_id = ba.follower_id
                               AND ab.follower_id = ba.followee_id
           JOIN following AS ac ON ab.follower_id = ac.follower_id
LEFT OUTER JOIN following AS ca ON ac.followee_id = ca.follower_id
                               AND ac.follower_id = ca.followee_id
           JOIN following AS bc ON bc.follower_id = ba.follower_id
                               AND bc.followee_id = ac.followee_id
LEFT OUTER JOIN following AS cb ON bc.followee_id = cb.follower_id
                               AND bc.follower_id = cb.followee_id
           JOIN user AS a ON a.id = a_id
           JOIN user AS b ON b.id = b_id
           JOIN user AS c ON c.id = c_id
WHERE ab.follower_id < ba.follower_id
  AND ca.follower_id IS NULL
  AND cb.follower_id IS NULL
  AND a.breed = 'regular'
  AND b.breed = 'regular'
  AND c.breed = 'regular'
