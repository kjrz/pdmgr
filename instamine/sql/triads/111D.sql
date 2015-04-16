SELECT ab.follower_id AS a_id,
       ab.followee_id AS b_id,
       cb.follower_id AS c_id
           FROM following AS ab
           JOIN following AS ba ON ab.followee_id = ba.follower_id
                               AND ab.follower_id = ba.followee_id
           JOIN following AS cb ON ba.follower_id = cb.followee_id
LEFT OUTER JOIN following AS bc ON cb.followee_id = bc.follower_id
                               AND cb.follower_id = bc.followee_id
LEFT OUTER JOIN following AS ac ON ab.follower_id = ac.follower_id
                               AND cb.follower_id = ac.followee_id
LEFT OUTER JOIN following AS ca ON cb.follower_id = ca.follower_id
                               AND ab.follower_id = ca.followee_id
WHERE ac.follower_id IS NULL
  AND ca.follower_id IS NULL
  AND bc.follower_id IS NULL
