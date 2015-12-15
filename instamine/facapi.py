import ConfigParser
import json

import facebook

conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')

USER_ACCESS_TOKEN = conf.get('facebook', 'user_token')

graph = facebook.GraphAPI(USER_ACCESS_TOKEN)

# cuda_na_kiju = graph.get_object(id='CudaNaKijuMultitapBar/events')
cuda_na_kiju = graph.get_object('CudaNaKijuMultitapBar', fields='location')

print json.dumps(cuda_na_kiju, indent=4)
