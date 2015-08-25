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
JOIN following AS ca ON ca.follower_id = cb.follower_id
                    AND ca.followee_id = ab.follower_id
WHERE a_id < b_id
  AND b_id < c_id
  AND (   ab.first_seen > (SELECT max(fin) FROM effort)
       OR ba.first_seen > (SELECT max(fin) FROM effort)
       OR bc.first_seen > (SELECT max(fin) FROM effort)
       OR cb.first_seen > (SELECT max(fin) FROM effort)
       OR ac.first_seen > (SELECT max(fin) FROM effort)
       OR ca.first_seen > (SELECT max(fin) FROM effort)
      )
