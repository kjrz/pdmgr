import ConfigParser
import logging
from logging.handlers import RotatingFileHandler
import os
import time
from datetime import datetime, timedelta

from instagram import InstagramAPIError
from instagram.bind import InstagramClientError
import sqlite3

from instapi import Session, UserPrivateException, OneHourApiCallsLimitReached
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
        while self.miner.got_work_to_do():
            self.work()
            self.stop()
        self.miner.effort_fin()

    def work(self):
        try:
            self.round_start = datetime.now()
            self.miner.dig()
        except InstagramAPIError as e:
            LOG.warn("Instagram API exception")
            LOG.exception(e)
            time.sleep(60)
        except InstagramClientError as e:
            LOG.warn("Instagram Client error")
            LOG.exception(e)
            time.sleep(60)
        except OneHourApiCallsLimitReached as e:
            LOG.warn("Over {} requests per hour"
                     .format(conf.getint('api', 'hour_max')))
            LOG.exception(e)
            time.sleep(60)

    def stop(self):
        self.miner.reload()
        self.sit_back()
        LOG.info("============mine over===============")

    def sit_back(self):
        stop = self.round_start + timedelta(0, CYCLE)
        now = datetime.now()
        while now < stop:
            LOG.info("under {} min left".format((stop - now).seconds / 60 + 1))
            time.sleep(TIC)
            now = datetime.now()

    def __init__(self, miner):
        LOG.info("====================================")
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
        self.stats()

    def dig(self):
        while self.api.ammo_left() > USER_AMMO and len(self.people):
            user_id = self.people.pop()
            user = self.db.user_known(user_id)
            LOG.info("next up -> instagram.com/{}".format(user.username))
            self.attend_to(user)
        self.db.commit()

    def attend_to(self, follower):
        followees = self.get_followees(follower)
        if not followees:
            # TODO: forget about him?
            return
        before = len(follower.follows)

        for followee_id, name in followees:
            followee = self.db.user_known(followee_id)
            if followee:
                self.db.set_follows(follower, followee)

        delta = len(follower.follows) - before
        if delta:
            LOG.info("           +{}".format(delta))
            self.game_changers[follower.username] = (before, delta)

        # TODO: remove unfollowed?

    def get_followees(self, follower):
        try:
            return self.api.followees(follower.id)
        except UserPrivateException:
            LOG.info("<private>")
            self.db.set_private(follower)

    def triadic_people(self):
        LOG.info('checking people...')
        people = self.db.all_regular()
        LOG.info('...{} people found'.format(len(people)))
        ans = [x[0] for x in people]
        self.rewind(ans)
        return ans

    def rewind(self, ans):
        last_processed_username = conf.get('users', 'last')
        last_processed_user = self.db.user_id(last_processed_username)
        if last_processed_user:
            last_processed_id = last_processed_user.id
            LOG.info("rewind to {} ({})...".format(last_processed_username, last_processed_id))
            while not ans.pop() == last_processed_id:
                pass

    def got_work_to_do(self):
        return len(self.people) > 0

    def effort_fin(self):
        self.db.close()
        LOG.info("============game changers============")
        for player, stats in self.game_changers.iteritems():
            LOG.info("{}: {}->{}".format(player, stats[0], stats[1]))
        LOG.info("===============respect===============")

    def stats(self):
        LOG.info("===========triad mine stats==========")
        LOG.info("    triadic people = {}".format(self.interesting_people))
        LOG.info("  yet to attend to = {}".format(len(self.people)))
        LOG.info("            active = {}".format(len(self.game_changers)))
        LOG.info("==============stats end==============")

    def __init__(self):
        self.api = Session()
        self.db = Mimesis(DB_PATH)
        self.people = self.triadic_people()
        self.interesting_people = len(self.people)
        self.game_changers = {}


class TriadFinder:
    def work(self):
        LOG.info("============dig triads==============")
        for triad_type in self.types:
            self.dig(triad_type)
        self.db.close()
        LOG.info("==========dig triads done===========")

    def dig(self, triad_type):
        LOG.info("triad type: {}".format(triad_type))
        triads = self.db.dig_triad(triad_type).fetchall()
        LOG.info("found: {}".format(len(triads)))

        LOG.info("writing triads to db...")
        processed = 0
        new_count = 0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        for a_id, b_id, c_id in triads:
            # TODO: move to mimesis
            try:
                c.execute("INSERT INTO triad (a_id, b_id, c_id, triad_type, first_seen)"
                          "VALUES (?, ?, ?, ?, ?)",
                          (a_id, b_id, c_id, triad_type, now))
                new_count += 1
            except sqlite3.IntegrityError:
                LOG.debug("{}({}, {}, {}) known".format(triad_type, a_id, b_id, c_id))

            processed += 1
            if processed % 500000 == 0:
                LOG.info("{}...".format(processed))

        conn.commit()
        if new_count > 0:
            LOG.info("+{}".format(new_count))
        else:
            LOG.info("<nothing new>")

    def dig_changes(self):
        LOG.info("============dig changes=============")
        new_triads = self.db.new_triads()
        LOG.info("{} new triads".format(len(new_triads)))
        # TODO: rewind?
        for to_triad in new_triads:
            LOG.info("to triad: {}, {}, {}".format(to_triad.a_id, to_triad.b_id, to_triad.c_id))  # TODO: debug?
            prev_triads = self.db.prev_triads(to_triad)
            LOG.info("prev_triads size = {}".format(len(prev_triads)))  # TODO: debug?
            if len(prev_triads) > 1:
                self.error_too_many_prev_triads(prev_triads, to_triad)
            if len(prev_triads) > 0:
                from_triad = prev_triads[0]
                self.info_triad_change(from_triad, to_triad)
                self.db.add_change(prev_triads[0].id, to_triad.id)
        self.db.commit()

    @staticmethod
    def error_too_many_prev_triads(prev_triads, to_triad):
        LOG.error("triad {} has {} predecessors"
                  .format((to_triad.a_id, to_triad.b_id, to_triad.c_id), len(prev_triads)))

    @staticmethod
    def info_triad_change(from_triad, to_triad):
        LOG.info("{}{} -> {}{}"
                 .format(from_triad.triad_type, (from_triad.a_id, from_triad.b_id, from_triad.c_id),
                         to_triad.triad_type, (to_triad.a_id, to_triad.b_id, to_triad.c_id)))

    def effort_fin(self):
        time.sleep(1.0)
        self.db.effort_fin()
        self.db.commit()
        self.db.close()
        LOG.info("================fin=================")

    def __init__(self):
        self.db = Mimesis(DB_PATH)
        self.types = [x[:-4] for x in os.listdir('sql/triads')]


if __name__ == '__main__':
    finder = TriadFinder()
    if finder.db.no_efforts_yet():
        finder.work()
        finder.effort_fin()
    else:
        mine = Mine(TriadMiner())
        mine.start()
        # finder.work()
        # finder.dig_changes()
        # finder.effort_fin()
        # TODO: work
