import ConfigParser
from datetime import datetime

from trading_places import LonelyHeartsMiner, HeartsClubFinder, HearsClubResults
from triad_mine import Mine

conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')
db_path = conf.get('db', 'path')
until = datetime.strptime(conf.get('tass', 'until'), '%Y-%m-%d %H:%M:%S')

miner = LonelyHeartsMiner(db_path, until)
mine = Mine(miner)
mine.start()
miner.close()

finder = HeartsClubFinder(db_path, until)
finder.dig()
finder.close()

results = HearsClubResults(db_path)
for l, r, e in results.get_meetings_info():
    print '{}: {}, {}'.format(l.encode('utf8'), r.encode('utf8'), e.encode('utf8'))
results.close()
