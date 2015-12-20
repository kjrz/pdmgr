import ConfigParser
import logging
from logging.handlers import RotatingFileHandler

import instapi
from mimesis import Mimesis

conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')

handler = logging.handlers.RotatingFileHandler(
    filename=conf.get('log', 'path'),
    maxBytes=1024 * 1024,
    backupCount=2)
handler.setFormatter(logging.Formatter(conf.get('log', 'format')))

LOG = logging.getLogger(conf.get('log', 'name'))
LOG.setLevel(conf.get('log', 'level'))
LOG.addHandler(handler)


class LonelyHeartsMiner:
    def dig(self):
        pass

    def attend_to(self, id):
        attendances = self.api.get_attendances(id, self.until)
        for time, loc in attendances:
            self.db.add_location(loc.id, loc.name, loc.point.latitude, loc.point.longitude)
            self.db.add_attendance(id, loc.id, time)

    def close(self):
        self.db.commit()
        self.db.close()

    def __init__(self, db_path, until):
        self.api = instapi.Session()
        self.db = Mimesis(db_path)
        self.lonely_people = self.db.get_active()
        self.until = until
