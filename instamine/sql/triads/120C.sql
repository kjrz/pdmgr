SELECT ab.follower_id AS a_id,
       ab.followee_id AS b_id,
       ac.followee_id AS c_id
           FROM following AS ab
           JOIN following AS ba ON ab.followee_id = ba.follower_id
                               AND ab.follower_id = ba.followee_id
           JOIN following AS ac ON ab.follower_id = ac.follower_id
LEFT OUTER JOIN following AS ca ON ac.followee_id = ca.follower_id
                               AND ac.follower_id = ca.followee_id
           JOIN following AS cb ON ac.followee_id = cb.follower_id
                               AND ab.followee_id = cb.followee_id
LEFT OUTER JOIN following AS bc ON cb.followee_id = bc.follower_id
                               AND cb.follower_id = bc.followee_id
WHERE ca.follower_id IS NULL
  AND bc.follower_id IS NULL
  AND ab.first_seen > (SELECT max(fin) FROM effort)
  AND ba.first_seen > (SELECT max(fin) FROM effort)
  AND ac.first_seen > (SELECT max(fin) FROM effort)
  AND cb.first_seen > (SELECT max(fin) FROM effort)
