SELECT ab.follower_id AS a_id,
       ab.followee_id AS b_id,
       ca.follower_id AS c_id
           FROM following AS ab
           JOIN following AS ba ON ab.followee_id = ba.follower_id
                               AND ab.follower_id = ba.followee_id
           JOIN following AS ca ON ab.follower_id = ca.followee_id
LEFT OUTER JOIN following AS ac ON ca.follower_id = ac.followee_id
                               AND ca.followee_id = ac.follower_id
           JOIN following AS cb ON ca.follower_id = cb.follower_id
                               AND ab.followee_id = cb.followee_id
LEFT OUTER JOIN following AS bc ON cb.followee_id = bc.follower_id
                               AND cb.follower_id = bc.followee_id
WHERE ab.follower_id < ba.follower_id
  AND ac.follower_id IS NULL
  AND bc.follower_id IS NULL