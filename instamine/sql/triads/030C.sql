SELECT ab.follower_id AS a_id,
       bc.follower_id AS b_id,
       ca.follower_id AS c_id
           FROM following AS ab
LEFT OUTER JOIN following AS ba ON ab.follower_id = ba.followee_id
                               AND ab.followee_id = ba.follower_id
           JOIN following AS bc ON bc.follower_id = ab.followee_id
LEFT OUTER JOIN following AS cb ON bc.follower_id = cb.followee_id
                               AND bc.followee_id = cb.follower_id
           JOIN following AS ca ON bc.followee_id = ca.follower_id
                               AND ab.follower_id = ca.followee_id
LEFT OUTER JOIN following AS ac ON ca.follower_id = ac.followee_id
                               AND ca.followee_id = ac.follower_id
WHERE ab.follower_id < bc.follower_id
  AND bc.follower_id < ca.follower_id
  AND ba.follower_id IS NULL
  AND cb.follower_id IS NULL
  AND ac.follower_id IS NULL
