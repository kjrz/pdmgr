SELECT location.name,
       follower.username,
       followee.username
FROM meeting
JOIN user AS follower ON follower.id = follower_id
JOIN user AS followee ON followee.id = followee_id
JOIN location ON location.id = location_id
ORDER BY location.name