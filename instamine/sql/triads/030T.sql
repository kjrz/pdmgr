SELECT ab.follower_id AS a_id,
       bc.follower_id AS b_id,
       ac.followee_id AS c_id
           FROM following AS ab
LEFT OUTER JOIN following AS ba ON ab.follower_id = ba.followee_id
                               AND ab.followee_id = ba.follower_id
           JOIN following AS bc ON bc.follower_id = ab.followee_id
LEFT OUTER JOIN following AS cb ON bc.follower_id = cb.followee_id
                               AND bc.followee_id = cb.follower_id
           JOIN following AS ac ON ab.follower_id = ac.follower_id
                               AND bc.followee_id = ac.followee_id
LEFT OUTER JOIN following AS ca ON ca.follower_id = ac.followee_id
                               AND ca.followee_id = ac.follower_id
WHERE ba.follower_id IS NULL
  AND cb.follower_id IS NULL
  AND ca.follower_id IS NULL
  AND (   ab.first_seen > (SELECT max(fin) FROM effort)
       OR bc.first_seen > (SELECT max(fin) FROM effort)
       OR ac.first_seen > (SELECT max(fin) FROM effort)
      )
