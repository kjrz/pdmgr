import ConfigParser
import logging
from logging.handlers import RotatingFileHandler
import time
from datetime import datetime, timedelta

from instagram import InstagramAPIError

from mimesis import Mimesis, Stats
from instapi import Session, UserPrivateException


conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')

DB_PATH = conf.get('db', 'path')
CYCLE = conf.getint('algorithm', 'cycle')
TIC = conf.getint('algorithm', 'tic')
USER_AMMO = conf.getint('algorithm', 'user_ammo')
CELEB_THRESHOLD = conf.getint('algorithm', 'celeb')
MANIAC_THRESHOLD = conf.getint('algorithm', 'maniac')
CLOSED_MODE = conf.get('algorithm', 'mode') == 'closed'

handler = logging.handlers.RotatingFileHandler(
    filename=conf.get('log', 'path'),
    maxBytes=1024 * 1024,
    backupCount=2)
handler.setFormatter(logging.Formatter(conf.get('log', 'format')))

LOG = logging.getLogger(conf.get('log', 'name'))
LOG.setLevel(conf.get('log', 'level'))
LOG.addHandler(handler)


# TODO: -o --open, -s --starting, -c --closed

class UsersToAttendTo:
    QUEUE_LEN = int(conf.get('algorithm', 'queue'))

    def next(self):
        return self.queue.pop(0)

    def reload(self):
        self.queue = self.db.the_unvisited(self.QUEUE_LEN)

    def __init__(self, db):
        self.db = db
        self.queue = []
        self.reload()


class Mine:
    def work(self):
        while True:
            self.dig_safe()
            self.relax()

    def dig_safe(self):
        try:
            self.dig()
        except InstagramAPIError as e:
            LOG.warn("Instagram API exception")
            LOG.exception(e)

    def dig(self):
        self.round_start = datetime.now()
        while self.api.ammo_left() > USER_AMMO:
            follower, followed_by = self.queue.next()
            LOG.info("next up ({}) -> instagram.com/{}".format(
                followed_by, follower.username))
            self.attend_to(follower)
        self.db.commit()

    def attend_to(self, follower):
        if self.celeb_or_private(follower):
            return

        followees = self.api.followees(follower.id)

        LOG.info("followees: {}".format(followees.size()))

        if self.maniac(follower, followees):
            return

        if self.inactive(follower, followees):
            return

        for id, username in followees:
            followee = self.db.user_known(id)
            if not followee:
                if CLOSED_MODE:
                    continue
                followee = self.db.add_user(id=id, username=username)
            self.db.set_follows(follower, followee)

        LOG.info("<done>")

    def celeb_or_private(self, follower):
        try:
            popularity = self.api.popularity(follower.id)
            LOG.info("followers: {}".format(popularity))
        except UserPrivateException:
            LOG.info("<private>")
            self.db.add_private(follower.id)
            return True

        if popularity >= CELEB_THRESHOLD:
            LOG.info("<celeb>")
            self.db.add_celeb(follower.id)
            return True

        return False

    def maniac(self, follower, followees):
        if len(followees) > MANIAC_THRESHOLD:
            LOG.info("<maniac>")
            self.db.add_private(follower.id)
            return True
        else:
            return False

    def inactive(self, follower, followees):
        if len(followees) == 0:
            LOG.info("<inactive>")
            self.db.add_private(follower.id)
            return True
        else:
            return False

    def relax(self):
        stop = self.set_time()
        self.reload()
        now = datetime.now()
        while now < stop:
            LOG.info("{} min left".format((stop - now).seconds / 60))
            time.sleep(TIC)
            now = datetime.now()
        self.api.reload()

    def set_time(self):
        return self.round_start + timedelta(0, CYCLE)

    def reload(self):
        LOG.info("reload...")
        self.queue.reload()
        LOG.info("done")
        Stats(DB_PATH).log().close()

    def __init__(self):
        Stats(DB_PATH).log().close()
        self.db = Mimesis(DB_PATH)
        self.queue = UsersToAttendTo(self.db)
        self.api = Session()


try:
    Mine().work()
except IndexError as e:
    LOG.warn("Queue probably empty")
    LOG.exception(e)
except Exception as e:
    LOG.warn("Something else")
    LOG.exception(e)
