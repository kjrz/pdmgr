import ConfigParser
import json

import facebook

conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')

USER_ACCESS_TOKEN = conf.get('facebook', 'user_token')

graph = facebook.GraphAPI(USER_ACCESS_TOKEN)
response = graph.get_object('CudaNaKijuMultitapBar', fields='location')

print json.dumps(response, indent=4)

# SELECT FROM followings WHERE first_seen
# queue active users
# for user in users:
#   get places he's been to @

# DB
# location()
# stay(fk location) q
