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
WHERE bc.follower_id IS NULL
  AND bc.followee_id IS NULL
  AND cb.follower_id IS NULL
  AND cb.followee_id IS NULL
  AND (   (    a_id > b_id
           AND b_id > c_id)
       OR (    a_id < b_id
           AND b_id > c_id))
  AND ab.first_seen > (SELECT max(fin) FROM effort)
  AND ba.first_seen > (SELECT max(fin) FROM effort)
  AND ac.first_seen > (SELECT max(fin) FROM effort)
  AND ca.first_seen > (SELECT max(fin) FROM effort)
