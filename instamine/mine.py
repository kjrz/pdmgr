import ConfigParser
import logging
from logging.handlers import RotatingFileHandler
import time
from datetime import datetime, timedelta

from instagram import InstagramAPIError
from sqlalchemy.exc import OperationalError

from mimesis import Mimesis, Stats
from instapi import Session, UserPrivateException


conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')

DB_PATH = conf.get('db', 'path')

CYCLE = 3600 * conf.getint('algorithm', 'clip') / conf.getint('api', 'hour_max')

USER_AMMO = conf.getint('algorithm', 'user_ammo')
TIC = conf.getint('algorithm', 'tic')

CELEB_THRESHOLD = conf.getint('users', 'celeb')
MANIAC_THRESHOLD = conf.getint('users', 'maniac')
INACTIVE_THRESHOLD = conf.getint('users', 'inactive')

ORIGIN = conf.get('users', 'origin')
USERS_LIMIT = conf.getint('users', 'limit')

handler = logging.handlers.RotatingFileHandler(
    filename=conf.get('log', 'path'),
    maxBytes=1024 * 1024,
    backupCount=2)
handler.setFormatter(logging.Formatter(conf.get('log', 'format')))

LOG = logging.getLogger(conf.get('log', 'name'))
LOG.setLevel(conf.get('log', 'level'))
LOG.addHandler(handler)


class UsersToAttendTo:
    QUEUE_LEN = int(conf.get('algorithm', 'queue'))

    def next(self):
        if self.opening and len(self.queue) == 0:
            self.reload()
        return self.queue.pop(0)

    def reload(self):
        self.queue = self.db.the_unvisited(self.QUEUE_LEN)
        if len(self.queue) < 10 * self.QUEUE_LEN:
            shorter_queue = self.QUEUE_LEN / 10
            LOG.info("limit queue size to {}".format(shorter_queue))
            self.queue = self.queue[:shorter_queue]
        else:
            self.opening = False

    def __init__(self, db):
        self.db = db
        self.queue = []
        self.opening = True
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
        except IndexError as e:
            LOG.warn(e)
            LOG.info("queue empty")
            self.db.commit()
            self.check_stats()
            exit()

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
                if self.users_limit_reached:
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
            self.db.add_maniac(follower.id)
            return True
        else:
            return False

    def inactive(self, follower, followees):
        if len(followees) <= INACTIVE_THRESHOLD:
            LOG.info("<inactive>")
            self.db.add_inactive(follower.id)
            return True
        else:
            return False

    def relax(self):
        stop = self.set_time()
        self.reload()
        now = datetime.now()
        while now < stop:
            LOG.info("over {} min left".format((stop - now).seconds / 60))
            time.sleep(TIC)
            now = datetime.now()
        self.api.reload()

    def set_time(self):
        return self.round_start + timedelta(0, CYCLE)

    def reload(self):
        LOG.info("reload...")
        self.queue.reload()
        LOG.info("...reload done")
        LOG.info("mode: {}".format("open" if not self.users_limit_reached else "closed"))
        LOG.info("cycle = {}".format(CYCLE))
        self.check_stats()

    def check_stats(self):
        stats = Stats(DB_PATH)
        users_count = stats.users()
        if not self.users_limit_reached:
            self.check_users_limit(users_count)
        if not self.users_not_empty:
            self.check_users_empty(users_count)
        stats.log()
        stats.close()

    def check_users_limit(self, users_count):
        self.users_limit_reached = users_count >= USERS_LIMIT

    def check_users_empty(self, users_count):
        if users_count > 0:
            return
        LOG.info("origin: {}".format(ORIGIN))
        id = self.api.search(ORIGIN).id
        LOG.info("id: {}".format(id))
        user = self.db.add_user(id=id, username=ORIGIN)
        self.attend_to(user)
        self.db.commit()

    def __init__(self):
        self.api = Session()
        self.db = Mimesis(DB_PATH)
        self.users_limit_reached = False
        self.users_not_empty = False
        self.check_stats()
        self.queue = UsersToAttendTo(self.db)


Mine().work()
