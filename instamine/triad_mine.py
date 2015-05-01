import ConfigParser
from httplib import IncompleteRead
import logging
import mysql.connector
from logging.handlers import RotatingFileHandler
import os
import time
from datetime import datetime, timedelta

from instagram import InstagramAPIError
from instagram.bind import InstagramClientError

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
        except IncompleteRead as e:
            LOG.warn("HTTP lib error")
            LOG.exception(e)
            time.sleep(60)
        except Exception as e:
            LOG.warn("sth else")
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
        # LOG.info("============game changers============")
        # for player, stats in self.game_changers.iteritems():
        #     LOG.info("{}: {}->{}".format(player, stats[0], stats[1]))
        # LOG.info("===============respect===============")

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

        LOG.info("set up db connection...")
        # TODO: move to mimesis
        conn = mysql.connector.connect(user='instamine',
                                       password='instamine',
                                       host='localhost',
                                       database='instamine')
        c = conn.cursor()
        c.execute('SELECT id FROM triad_type WHERE name = %s', (triad_type,))
        triad_type_id = c.fetchone()[0]
        LOG.info("appending triad type...")
        triads = [(a_id, b_id, c_id, triad_type_id) for a_id, b_id, c_id in triads]
        LOG.info("splitting into chunks...")
        triads = self.chunks(triads, 500000)
        LOG.info("writing triads to db...")

        triads_added = 0
        for portion in triads:
            c.executemany("INSERT IGNORE INTO triad (a_id, b_id, c_id, triad_type_id) "
                          "VALUES (%s, %s, %s, %s)", portion)
            triads_added += len(portion)
            LOG.info("{}...".format(triads_added))
            conn.commit()

        conn.close()
        LOG.info("<done>")

        # if new_count > 0:
        #     LOG.info("+{}".format(new_count))
        # else:
        #     LOG.info("<nothing new>")
        # TODO: check number of new

    @staticmethod
    def chunks(l, n):
        for i in xrange(0, len(l), n):
            yield l[i:i+n]

    def dig_changes(self):
        pass
        # # TODO: move to mimesis
        LOG.info("============dig changes=============")
        conn = mysql.connector.connect(user='instamine',
                                       password='instamine',
                                       host='localhost',
                                       database='instamine')
        c = conn.cursor()
        c.execute('SELECT triad.id, a_id, b_id, c_id, name '
                  'FROM triad '
                  'JOIN triad_type ON (triad_type_id = triad_type.id) '
                  'AND first_seen > (SELECT MAX(fin) FROM effort)')
        # TODO: join on triad type name an get name in one select
        new_triads = c.fetchall()

        LOG.info("{} new triads".format(len(new_triads)))

        for to_triad_id, a_id, b_id, c_id, to_triad_name in new_triads:
            c.execute(open('sql/mysql/change.sql').read(),
                      (a_id, b_id, c_id,
                       a_id, b_id, c_id,
                       a_id, b_id, c_id))
            prev_triads = c.fetchall()
            if len(prev_triads) > 1:
                LOG.error("yo it's {} for {}".format(len(prev_triads), to_triad_id))
                break
            if len(prev_triads) > 0:
                from_triad_id, = prev_triads[0]
                c.execute('SELECT name FROM triad '
                          'JOIN triad_type ON (triad_type_id = triad_type.id) '
                          'WHERE triad.id = %s', (from_triad_id, ))
                prev_triad_name, = c.fetchone()
                LOG.info('found triad change: {} -> {}'.format(prev_triad_name, to_triad_name))
                c.execute('INSERT INTO triad_change (from_triad, to_triad) VALUES (%s, %s)',
                          (from_triad_id, to_triad_id))
                # break
        conn.commit()

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
        self.mysql_effort_fin()
        self.db.commit()
        self.db.close()
        LOG.info("================fin=================")

    def __init__(self):
        self.db = Mimesis(DB_PATH)
        self.types = [x[:-4] for x in os.listdir('sql/triads')]

    def mysql_effort_fin(self):
        conn = mysql.connector.connect(user='instamine',
                                       password='instamine',
                                       host='localhost',
                                       database='instamine')
        c = conn.cursor()
        c.execute('INSERT INTO effort VALUES()')
        conn.commit()


if __name__ == '__main__':
    mine = Mine(TriadMiner())
    mine.start()
    finder = TriadFinder()
    finder.work()
    finder.dig_changes()
    finder.effort_fin()
