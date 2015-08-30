import ConfigParser
from httplib import IncompleteRead
import logging
from logging.handlers import RotatingFileHandler
import os
import time
from datetime import timedelta, datetime

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
            # TODO: forget him?
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


class TriadClassifier:
    @staticmethod
    def classify(followings):
        if len(followings) == 0:
            return "003"
        if len(followings) == 1:
            return "012"
        if len(followings) != 2:
            return None
        if followings[0][1] == followings[1][0]:
            if followings[0][0] == followings[1][1]:
                return "102"
            else:
                return "021C"
        if followings[0][0] == followings[1][0]:
            return "021D"
        if followings[0][1] == followings[1][1]:
            return "021U"
        else:
            return None

    def __init__(self):
        pass


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
        self.db2.write_triads(triads, triad_type)

    @staticmethod
    def chunks(l, n):
        for i in xrange(0, len(l), n):
            yield l[i:i+n]

    # TODO: remove
    def dig_changes(self):
        LOG.info("============dig changes=============")

        new_triads = self.db2.get_new_triads()
        LOG.info("{} new triads".format(len(new_triads)))

        for to_triad_id, a_id, b_id, c_id, to_triad_name in new_triads:
            prev_triads = self.db2.get_prev_triads(a_id, b_id, c_id)
            if len(prev_triads) > 1:
                LOG.error("yo it's {} for {}".format(len(prev_triads), to_triad_id))
                break
            if len(prev_triads) > 0:
                from_triad_id, from_triad_name = prev_triads[0]
                LOG.info('found triad change: {} -> {}'.format(from_triad_name, to_triad_name))
                self.db2.write_change(from_triad_id, to_triad_id)
        self.db2.commit()

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

    def __init__(self, triad_mimesis):
        self.db = Mimesis(DB_PATH)
        self.db2 = triad_mimesis
        self.types = [x[:-4] for x in os.listdir('sql/triads')]

    @staticmethod
    def mysql_effort_fin():
        # conn = mysql.connector.connect(user='instamine',
        #                                password='instamine',
        #                                host='localhost',
        #                                database='instamine')
        # c = conn.cursor()
        # c.execute('INSERT INTO effort VALUES()')
        # conn.commit()
        pass
        # TODO: to mimesis


class TriadChangeFinder:
    def dig_changes(self):
        LOG.info("============dig changes=============")

        new_triads = self.db2.get_new_triads()
        LOG.info("{} new triads".format(len(new_triads)))

        for to_triad_id, a_id, b_id, c_id, to_triad_name in new_triads:
            from_triad_id = self.dig_change((a_id, b_id, c_id))
            self.db2.write_change(from_triad_id, to_triad_id)

        self.db2.commit()

    def dig_change(self, nodes):
        prev_triad = self.db2.get_prev_triads(nodes)
        # TODO: log the finding
        if prev_triad is not None:
            return prev_triad[0]

        followings = self.db.get_triad(nodes, before=self.last_fin)
        classification = TriadClassifier.classify(followings)
        if classification is not None:
            first_seen = self.get_first_seen(followings)
            row_id = self.db2.insert_classified_triad(nodes, classification, first_seen)
            # TODO: log the finding
            return row_id

    def get_first_seen(self, followings):
        if len(followings) == 0:
            return self.db.first_fin() - timedelta(days=7)
        return max([following[2] for following in followings])

    def __init__(self, triad_mimesis):
        self.db = Mimesis(DB_PATH)
        self.last_fin = self.db.last_fin()
        self.db2 = triad_mimesis

if __name__ == '__main__':
    mine = Mine(TriadMiner())
    mine.start()
    finder = TriadFinder()
    finder.work()
    finder.dig_changes()
    finder.effort_fin()
