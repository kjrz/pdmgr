import ConfigParser
import logging
from logging.handlers import RotatingFileHandler
import time
from datetime import datetime, timedelta

from instagram import InstagramAPIError

from mimesis import Mimesis, UserStats, User
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
        breed = self.check_breed(follower)
        if breed is not User.Breed.REGULAR:
            return
        followees = self.api.followees(follower.id)

        for id, username in followees:
            followee = self.db.user_known(id)
            if not followee:
                if self.users_limit_reached:
                    continue
                followee = self.db.add_user(id=id, username=username)
            self.db.set_follows(follower, followee)

        LOG.info("<done>")

    def check_breed(self, follower):
        try:
            info = self.api.info(follower.id)
        except UserPrivateException:
            LOG.info("<private>")
            self.db.set_private(follower)
            return User.Breed.PRIVATE

        followed_by = info.followers_count()
        self.db.set_followers(follower, followed_by)
        LOG.info("followed by: {}".format(followed_by))

        follows = info.followees_count()
        self.db.set_followees(follower, follows)
        LOG.info("follows: {}".format(follows))

        if followed_by >= CELEB_THRESHOLD:
            LOG.info("<celeb>")
            self.db.set_celeb(follower)
            return User.Breed.CELEB

        if follows >= MANIAC_THRESHOLD:
            LOG.info("<maniac>")
            self.db.set_maniac(follower)
            return User.Breed.MANIAC

        if follows <= INACTIVE_THRESHOLD:
            LOG.info("<inactive>")
            self.db.set_inactive(follower)
            return User.Breed.INACTIVE

        self.db.set_regular(follower)
        return User.Breed.REGULAR

    def relax(self):
        stop = self.set_time()
        self.reload()
        now = datetime.now()
        while now < stop:
            LOG.info("under {} min left".format((stop - now).seconds / 60 + 1))
            time.sleep(TIC)
            now = datetime.now()
        self.api.reload()
        LOG.info("============start over==============")

    def set_time(self):
        return self.round_start + timedelta(0, CYCLE)

    def reload(self):
        LOG.info("==============reload================")
        self.queue.reload()
        LOG.info("mode: {}".format("open" if not self.users_limit_reached else "closed"))
        LOG.info("cycle = {}".format(CYCLE))
        self.check_stats()

    def check_stats(self):
        stats = UserStats(DB_PATH)
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
        user = self.db.add_user(id=id, username=ORIGIN)
        self.attend_to(user)
        self.db.commit()

    def __init__(self):
        LOG.info("===============start=================")
        self.api = Session()
        self.db = Mimesis(DB_PATH)
        self.users_limit_reached = False
        self.users_not_empty = False
        self.check_stats()
        self.queue = UsersToAttendTo(self.db)


Mine().work()
