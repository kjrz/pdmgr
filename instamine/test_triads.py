import ConfigParser
import os
import unittest
import shutil
import logging
import sqlite3

from logging.handlers import RotatingFileHandler

from mimesis import Mimesis


CONF_PATH = '../instamine.conf'
TEST_CONF_PATH = '../conf/test.conf'
PROD_CONF_PATH = '../conf/prod.conf'
shutil.copyfile(TEST_CONF_PATH, CONF_PATH)

conf = ConfigParser.RawConfigParser()
conf.read('../conf/test.conf')

handler = logging.handlers.RotatingFileHandler(
    filename=conf.get('log', 'path'),
    maxBytes=1024 * 1024,
    backupCount=2)
handler.setFormatter(logging.Formatter(conf.get('log', 'format')))

LOG = logging.getLogger(conf.get('log', 'name'))
LOG.setLevel(conf.get('log', 'level'))
LOG.addHandler(handler)


class TestQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.clearDb()
        cls.populateDb()

    @classmethod
    def clearDb(cls):
        try:
            os.remove('../data/test.db')
        except OSError:
            pass

    @classmethod
    def populateDb(cls):
        db = Mimesis(db_path=conf.get('db', 'path'))
        cls.zero_two_one_c(db)
        cls.zero_two_one_d(db)
        cls.zero_two_one_u(db)
        cls.zero_three_zero_c(db)
        cls.zero_three_zero_t(db)
        cls.one_one_one_d(db)
        cls.one_one_one_u(db)
        cls.one_two_zero_c(db)
        cls.one_two_zero_d(db)
        cls.one_two_zero_u(db)
        cls.two_zero_one(db)
        cls.two_one_zero(db)
        cls.three_zero_zero(db)
        db.commit()
        db.close()

    @classmethod
    def zero_two_one_c(cls, db):
        a = db.add_user(1, 'a_021C')
        b = db.add_user(2, 'b_021C')
        c = db.add_user(3, 'c_021C')
        db.commit()

    @classmethod
    def zero_two_one_d(cls, db):
        a = db.add_user(4, 'a_021C')
        b = db.add_user(5, 'b_021C')
        c = db.add_user(6, 'c_021C')

    @classmethod
    def zero_two_one_u(cls, db):
        a = db.add_user(7, 'a_021C')
        b = db.add_user(8, 'b_021C')
        c = db.add_user(9, 'c_021C')

    @classmethod
    def zero_three_zero_c(cls, db):
        a = db.add_user(10, 'a_021C')
        b = db.add_user(11, 'b_021C')
        c = db.add_user(12, 'c_021C')

    @classmethod
    def zero_three_zero_t(cls, db):
        a = db.add_user(13, 'a_021C')
        b = db.add_user(14, 'b_021C')
        c = db.add_user(15, 'c_021C')

    @classmethod
    def one_one_one_d(cls, db):
        a = db.add_user(16, 'a_021C')
        b = db.add_user(17, 'b_021C')
        c = db.add_user(18, 'c_021C')

    @classmethod
    def one_one_one_u(cls, db):
        a = db.add_user(19, 'a_021C')
        b = db.add_user(20, 'b_021C')
        c = db.add_user(21, 'c_021C')

    @classmethod
    def one_two_zero_c(cls, db):
        a = db.add_user(22, 'a_120C')
        b = db.add_user(23, 'b_120C')
        c = db.add_user(24, 'c_120C')
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(a, c)
        db.set_follows(c, b)

    def test_one_two_zero_c_query(self):
        conn = sqlite3.connect(conf.get('db', 'path'))
        c = conn.cursor()
        rows = c.execute('''SELECT ab.follower_id AS a_id,
                                   ab.followee_id AS b_id,
                                   ac.followee_id AS c_id
                                       FROM following AS ab
                                       JOIN following AS ba ON ab.followee_id = ba.follower_id
                                                           AND ab.follower_id = ba.followee_id
                                       JOIN following AS ac ON ab.follower_id = ac.follower_id
                            LEFT OUTER JOIN following AS ca ON ac.followee_id = ca.follower_id
                                                           AND ac.follower_id = ca.followee_id
                                       JOIN following AS cb ON ac.followee_id = cb.follower_id
                                                           AND ab.followee_id = cb.followee_id
                            LEFT OUTER JOIN following AS bc ON cb.followee_id = bc.follower_id
                                                           AND cb.follower_id = bc.followee_id
                            WHERE ca.follower_id IS NULL
                              AND bc.follower_id IS NULL''').fetchall()
        self.assertListEqual(rows, [(22, 23, 24)])

    @classmethod
    def one_two_zero_d(cls, db):
        a = db.add_user(25, 'a_120D')
        b = db.add_user(26, 'b_120D')
        c = db.add_user(27, 'c_120D')
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(c, a)
        db.set_follows(c, b)

    def test_one_two_zero_d_query(self):
        conn = sqlite3.connect(conf.get('db', 'path'))
        c = conn.cursor()
        rows = c.execute('''SELECT ab.follower_id AS a_id,
                                   ab.followee_id AS b_id,
                                   ca.follower_id AS c_id
                                   FROM following AS ab
                                   JOIN following AS ba ON ab.followee_id = ba.follower_id
                                                       AND ab.follower_id = ba.followee_id
                                   JOIN following AS ca ON ab.follower_id = ca.followee_id
                        LEFT OUTER JOIN following AS ac ON ca.follower_id = ac.followee_id
                                                       AND ca.followee_id = ac.follower_id
                                   JOIN following AS cb ON ca.follower_id = cb.follower_id
                                                       AND ab.followee_id = cb.followee_id
                        LEFT OUTER JOIN following AS bc ON cb.followee_id = bc.follower_id
                                                       AND cb.follower_id = bc.followee_id
                        WHERE ab.follower_id < ba.follower_id
                          AND ac.follower_id IS NULL
                          AND bc.follower_id IS NULL''').fetchall()
        self.assertListEqual(rows, [(25, 26, 27)])

    @classmethod
    def one_two_zero_u(cls, db):
        a = db.add_user(28, 'a_120U')
        b = db.add_user(29, 'b_120U')
        c = db.add_user(30, 'c_120U')
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(a, c)
        db.set_follows(b, c)

    def test_one_two_zero_u_query(self):
        conn = sqlite3.connect(conf.get('db', 'path'))
        c = conn.cursor()
        rows = c.execute('''SELECT ab.follower_id AS a_id,
                                   ab.followee_id AS b_id,
                                   ac.followee_id AS c_id
                                   FROM following AS ab
                                   JOIN following AS ba ON ab.followee_id = ba.follower_id
                                                       AND ab.follower_id = ba.followee_id
                                   JOIN following AS ac ON ab.follower_id = ac.follower_id
                        LEFT OUTER JOIN following AS ca ON ac.followee_id = ca.follower_id
                                                       AND ac.follower_id = ca.followee_id
                                   JOIN following AS bc ON bc.follower_id = ba.follower_id
                                                       AND bc.followee_id = ac.followee_id
                        LEFT OUTER JOIN following AS cb ON ca.follower_id = cb.follower_id
                                                       AND ab.followee_id = cb.followee_id
                        WHERE ab.follower_id < ba.follower_id
                          AND ca.follower_id IS NULL
                          AND cb.follower_id IS NULL''').fetchall()
        self.assertListEqual(rows, [(28, 29, 30)])

    @classmethod
    def two_zero_one(cls, db):
        a = db.add_user(31, 'a_021C')
        b = db.add_user(32, 'b_021C')
        c = db.add_user(33, 'c_021C')

    @classmethod
    def two_one_zero(cls, db):
        a = db.add_user(34, 'a_021C')
        b = db.add_user(35, 'b_021C')
        c = db.add_user(36, 'c_021C')

    @classmethod
    def three_zero_zero(cls, db):
        a = db.add_user(37, 'a_021C')
        b = db.add_user(38, 'b_021C')
        c = db.add_user(39, 'c_021C')
        # do not test

    @classmethod
    def tearDownClass(cls):
        shutil.copyfile(PROD_CONF_PATH, CONF_PATH)


if __name__ == '__main__':
    unittest.main()
