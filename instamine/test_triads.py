import ConfigParser
import os
import unittest
import shutil
import logging
import sqlite3

from logging.handlers import RotatingFileHandler
import datetime
from mimesis import Mimesis, Effort


CONF_PATH = '../instamine.conf'
TEST_CONF_PATH = '../conf/test.conf'
PROD_CONF_PATH = '../conf/prod.conf'
shutil.copyfile(CONF_PATH, PROD_CONF_PATH)
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


class TestQueriesMimesis(Mimesis):
    def effort_a_min_ago(self):
        record = Effort(fin=datetime.datetime.now() - datetime.timedelta(minutes=1))
        LOG.debug("setting effort fin: {}".format(unicode(record.fin)))
        self.session.add(record)


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
        db = TestQueriesMimesis(db_path=conf.get('db', 'path'))
        db.effort_a_min_ago()
        cls.insert_003(db)
        cls.insert_021C(db)
        cls.insert_021D(db)
        cls.insert_021U(db)
        cls.insert_030C(db)
        cls.insert_030T(db)
        cls.insert_111D(db)
        cls.insert_111U(db)
        cls.insert_120C(db)
        cls.insert_120D(db)
        cls.insert_120U(db)
        cls.insert_201(db)
        cls.insert_210(db)
        cls.insert_300(db)
        db.commit()
        db.close()

    @classmethod
    def insert_003(cls, db):
        a = db.add_user(40, 'a_003')
        b = db.add_user(41, 'b_003')
        c = db.add_user(42, 'c_003')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)

    @classmethod
    def insert_012(cls, db):
        a = db.add_user(43, 'a_012')
        b = db.add_user(44, 'b_012')
        c = db.add_user(45, 'c_012')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)

    @classmethod
    def insert_021C(cls, db):
        a = db.add_user(1, 'a_021C')
        b = db.add_user(2, 'b_021C')
        c = db.add_user(3, 'c_021C')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, c)
        db.set_follows(c, b)

    @classmethod
    def insert_021D(cls, db):
        a = db.add_user(4, 'a_021D')
        b = db.add_user(5, 'b_021D')
        c = db.add_user(6, 'c_021D')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(c, a)
        db.set_follows(c, b)

    @classmethod
    def insert_021U(cls, db):
        a = db.add_user(7, 'a_021U')
        b = db.add_user(8, 'b_021U')
        c = db.add_user(9, 'c_021U')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, c)
        db.set_follows(b, c)

    @classmethod
    def insert_030C(cls, db):
        a = db.add_user(10, 'a_030C')
        b = db.add_user(11, 'b_030C')
        c = db.add_user(12, 'c_030C')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, c)
        db.set_follows(c, a)

    def test_030C(self):
        rows = self.run_select('030C')
        self.assertListEqual(rows, [(10, 11, 12)])

    @classmethod
    def insert_030T(cls, db):
        a = db.add_user(13, 'a_030T')
        b = db.add_user(14, 'b_030T')
        c = db.add_user(15, 'c_030T')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, c)
        db.set_follows(a, b)
        db.set_follows(b, c)

    def test_030T(self):
        rows = self.run_select('030T')
        self.assertListEqual(rows, [(13, 14, 15)])

    @classmethod
    def insert_111D(cls, db):
        a = db.add_user(16, 'a_111D')
        b = db.add_user(17, 'b_111D')
        c = db.add_user(18, 'c_111D')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(c, b)

    def test_111D(self):
        rows = self.run_select('111D')
        self.assertListEqual(rows, [(16, 17, 18)])

    @classmethod
    def insert_111U(cls, db):
        a = db.add_user(19, 'a_111U')
        b = db.add_user(20, 'b_111U')
        c = db.add_user(21, 'c_111U')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(b, c)

    def test_111U(self):
        rows = self.run_select('111U')
        self.assertListEqual(rows, [(19, 20, 21)])

    @classmethod
    def insert_120C(cls, db):
        a = db.add_user(22, 'a_120C')
        b = db.add_user(23, 'b_120C')
        c = db.add_user(24, 'c_120C')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(a, c)
        db.set_follows(c, b)

    def test_120C(self):
        rows = self.run_select('120C')
        self.assertListEqual(rows, [(22, 23, 24)])

    @classmethod
    def insert_120D(cls, db):
        a = db.add_user(25, 'a_120D')
        b = db.add_user(26, 'b_120D')
        c = db.add_user(27, 'c_120D')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(c, a)
        db.set_follows(c, b)

    def test_120D(self):
        rows = self.run_select('120D')
        self.assertListEqual(rows, [(25, 26, 27)])

    @classmethod
    def insert_120U(cls, db):
        a = db.add_user(28, 'a_120U')
        b = db.add_user(29, 'b_120U')
        c = db.add_user(30, 'c_120U')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(a, c)
        db.set_follows(b, c)

    def test_120U(self):
        rows = self.run_select('120U')
        self.assertListEqual(rows, [(28, 29, 30)])

    @classmethod
    def insert_201(cls, db):
        # a_id < b_id < c_id
        a = db.add_user(3100, 'a_201')
        b = db.add_user(3201, 'b_201')
        c = db.add_user(3302, 'c_201')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(a, c)
        db.set_follows(c, a)
        # b_id < a_id < c_id
        a = db.add_user(3210, 'a_201')
        b = db.add_user(3111, 'b_201')
        c = db.add_user(3312, 'c_201')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(a, c)
        db.set_follows(c, a)
        # b_id < c_id < a_id
        a = db.add_user(3320, 'a_201')
        b = db.add_user(3121, 'b_201')
        c = db.add_user(3222, 'c_201')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(a, c)
        db.set_follows(c, a)
        # c_id < b_id < a_id
        a = db.add_user(3330, 'a_201')
        b = db.add_user(3231, 'b_201')
        c = db.add_user(3132, 'c_201')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(a, c)
        db.set_follows(c, a)

    def test_201(self):
        rows = self.run_select('201')
        self.assertSetEqual(set(rows), {(3100, 3302, 3201),
                                        (3210, 3312, 3111),
                                        (3320, 3222, 3121),
                                        (3330, 3231, 3132)})

    @classmethod
    def insert_210(cls, db):
        a = db.add_user(34, 'a_210')
        b = db.add_user(35, 'b_210')
        c = db.add_user(36, 'c_210')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(b, c)
        db.set_follows(c, b)
        db.set_follows(a, c)

    def test_210(self):
        rows = self.run_select('210')
        self.assertListEqual(rows, [(34, 35, 36)])

    @classmethod
    def insert_300(cls, db):
        a = db.add_user(37, 'a_300')
        b = db.add_user(38, 'b_300')
        c = db.add_user(39, 'c_300')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(b, c)
        db.set_follows(c, b)
        db.set_follows(a, c)
        db.set_follows(c, a)

    def test_300(self):
        rows = self.run_select('300')
        self.assertListEqual(rows, [(37, 38, 39)])

    @classmethod
    def tearDownClass(cls):
        shutil.copyfile(PROD_CONF_PATH, CONF_PATH)

    @staticmethod
    def run_select(select):
        conn = sqlite3.connect(conf.get('db', 'path'))
        c = conn.cursor()
        c.execute(open('sql/triads/' + select + '.sql').read())
        rows = c.fetchall()
        return rows


if __name__ == '__main__':
    unittest.main()
