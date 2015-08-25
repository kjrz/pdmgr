SELECT ab.follower_id AS a_id,
       ab.followee_id AS b_id,
       bc.followee_id AS c_id
           FROM following AS ab
           JOIN following AS ba ON ab.followee_id = ba.follower_id
                               AND ab.follower_id = ba.followee_id
           JOIN following AS bc ON ba.follower_id = bc.follower_id
LEFT OUTER JOIN following AS cb ON bc.followee_id = cb.follower_id
                               AND bc.follower_id = cb.followee_id
LEFT OUTER JOIN following AS ac ON ab.follower_id = ac.follower_id
                               AND bc.followee_id = ac.followee_id
LEFT OUTER JOIN following AS ca ON bc.followee_id = ca.follower_id
                               AND ab.follower_id = ca.followee_id
WHERE ac.follower_id IS NULL
  AND ca.follower_id IS NULL
  AND cb.follower_id IS NULL
  AND (   ab.first_seen > (SELECT max(fin) FROM effort)
       OR ba.first_seen > (SELECT max(fin) FROM effort)
       OR bc.first_seen > (SELECT max(fin) FROM effort)
      )
