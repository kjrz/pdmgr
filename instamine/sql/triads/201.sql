SELECT ab.follower_id AS a_id,
       ba.follower_id AS b_id,
       ca.follower_id AS c_id
           FROM following AS ab
           JOIN following AS ba ON ab.followee_id = ba.follower_id
                               AND ab.follower_id = ba.followee_id
           JOIN following AS ac ON ab.follower_id = ac.follower_id
           JOIN following AS ca ON ac.followee_id = ca.follower_id
                               AND ac.follower_id = ca.followee_id
LEFT OUTER JOIN following AS bc ON bc.follower_id = ba.follower_id
                               AND bc.followee_id = ca.follower_id
LEFT OUTER JOIN following AS cb ON cb.follower_id = ca.follower_id
                               AND cb.followee_id = ba.follower_id
           JOIN user AS a ON a.id = a_id
           JOIN user AS b ON b.id = b_id
           JOIN user AS c ON c.id = c_id
  AND (   (    ac.follower_id > ba.follower_id
       AND ba.follower_id > ca.follower_id)
       OR (    ac.follower_id < ba.follower_id
       AND ba.follower_id > ca.follower_id))
