import ConfigParser
import logging
from logging.handlers import RotatingFileHandler
import os
import time
from datetime import datetime, timedelta

from instagram import InstagramAPIError

from instapi import Session
from mimesis import Mimesis

conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')

DB_PATH = conf.get('db', 'path')

CYCLE = 3600 * conf.getint('algorithm', 'clip') / conf.getint('api', 'hour_max')

USER_AMMO = conf.getint('algorithm', 'user_ammo')
TIC = conf.getint('algorithm', 'tic')

handler = logging.handlers.RotatingFileHandler(
    filename=conf.get('log', 'path'),
    maxBytes=1024 * 1024,
    backupCount=2)
handler.setFormatter(logging.Formatter(conf.get('log', 'format')))

LOG = logging.getLogger(conf.get('log', 'name'))
LOG.setLevel(conf.get('log', 'level'))
LOG.addHandler(handler)


class Mine:
    def start(self):
        while True:
            self.work()
            self.stop()

    def work(self):
        try:
            self.round_start = datetime.now()
            self.miner.dig()
        except InstagramAPIError as e:
            LOG.warn("Instagram API exception")
            LOG.exception(e)

    def stop(self):
        self.sit_back()
        self.miner.reload()
        LOG.info("============start over==============")

    def sit_back(self):
        stop = self.round_start + timedelta(0, CYCLE)
        now = datetime.now()
        while now < stop:
            LOG.info("under {} min left".format((stop - now).seconds / 60 + 1))
            time.sleep(TIC)
            now = datetime.now()

    def __init__(self, miner):
        LOG.info("===============start=================")
        self.miner = miner


class TriadsToAttendTo:
    def __init__(self):
        self.types = [x[:-4] for x in os.listdir('sql/triads')]


class TriadMembersToAttendTo:
    def __init__(self):
        self.users = set()


class TriadMiner:
    def reload(self):
        self.api.reload()

    def dig(self):
        while len(self.people) > 0 and self.api.ammo_left() > USER_AMMO:
            user_id = self.people.pop()
            user = self.db.user_known(user_id)
            self.attend_to(user)
        self.db.commit()

    def attend_to(self, follower):
        followees = self.api.followees(follower.id)
        for followee_id, name in followees:
            followee = self.db.user_known(followee_id)
            if followee:
                self.db.set_follows(follower, followee)

    def triadic_people(self):
        people = set()
        LOG.info('checking the unstable...')
        unstable = self.db.the_unstable()
        LOG.info('...{} unstable found'.format(len(unstable)))
        for a, b, c in unstable:
            people.add(a)
            people.add(b)
            people.add(c)
        LOG.info('...{} triadic people'.format(len(people)))
        return people

    def dig_changes(self):
        changes = self.db.the_changed()
        for from_triad, to_triad in changes:
            LOG.debug("from_triad = {}, to_triad = {}".format(from_triad, to_triad))
            self.db.add_change(from_triad, to_triad)
        self.db.commit()

    def get_fired(self):
        self.db.close()

    def effort_fin(self):
        self.db.effort_fin()

    def __init__(self):
        self.api = Session()
        self.db = Mimesis(DB_PATH)
        self.people = self.triadic_people()


class TriadFinder:
    def work(self):
        for triad_type in self.types:
            self.dig(triad_type)
        self.db.close()

    def dig(self, triad_type):
        triads = self.db.dig_triad(triad_type)
        for a_id, b_id, c_id in triads:
            self.db.add_triad(a_id, b_id, c_id, triad_type)
        self.db.commit()

    def effort_fin(self):
        time.sleep(1.0)
        self.db.effort_fin()

    def close(self):
        self.db.commit()
        self.db.close()

    def __init__(self):
        self.db = Mimesis(DB_PATH)
        self.types = [x[:-4] for x in os.listdir('sql/triads')]
