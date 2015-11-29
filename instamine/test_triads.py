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

# LOG = logging.getLogger(conf.get('log', 'name'))
# LOG.setLevel(conf.get('log', 'level'))
# LOG.addHandler(handler)


class TestQueriesMimesis(Mimesis):
    def effort_a_min_ago(self):
        record = Effort(fin=datetime.datetime.now() - datetime.timedelta(minutes=1))
        # LOG.debug("setting effort fin: {}".format(unicode(record.fin)))
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
        cls.insert_012(db)
        cls.insert_021C(db)
        cls.insert_021D(db)
        cls.insert_021U(db)
        cls.insert_030C(db)
        cls.insert_030T(db)
        cls.insert_102(db)
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
        # a < b < c
        cls.insert_030C_variant(db.add_user(100, 'a_030C'),
                                db.add_user(110, 'b_030C'),
                                db.add_user(120, 'c_030C'), db)
        # a < c < b
        cls.insert_030C_variant(db.add_user(101, 'a_030C'),
                                db.add_user(121, 'b_030C'),
                                db.add_user(111, 'c_030C'), db)
        # b < a < c
        cls.insert_030C_variant(db.add_user(112, 'a_030C'),
                                db.add_user(102, 'b_030C'),
                                db.add_user(122, 'c_030C'), db)
        # b < c < a
        cls.insert_030C_variant(db.add_user(123, 'a_030C'),
                                db.add_user(103, 'b_030C'),
                                db.add_user(113, 'c_030C'), db)
        # c < a < b
        cls.insert_030C_variant(db.add_user(114, 'a_030C'),
                                db.add_user(124, 'b_030C'),
                                db.add_user(104, 'c_030C'), db)
        # c < b < a
        cls.insert_030C_variant(db.add_user(125, 'a_030C'),
                                db.add_user(115, 'b_030C'),
                                db.add_user(105, 'c_030C'), db)

    @classmethod
    def insert_030C_variant(cls, a, b, c, db):
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, c)
        db.set_follows(c, a)

    def test_030C(self):
        rows = self.run_select('030C')
        # all_triads = [(100, 110, 120),
        #               (101, 121, 111),
        #               (112, 102, 122),
        #               (123, 103, 113),
        #               (114, 124, 104),
        #               (125, 115, 105)]
        all_triads = [(100, 110, 120),
                      (101, 121, 111),
                      (102, 122, 112),
                      (103, 113, 123),
                      (104, 114, 124),
                      (105, 125, 115)]
        self.assertEquals(len(rows), len(all_triads))
        self.assertSetEqual(set(rows), set(all_triads))

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
    def insert_102(cls, db):
        a = db.add_user(50, 'a_003')
        b = db.add_user(51, 'b_003')
        c = db.add_user(52, 'c_003')
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, c)
        db.set_follows(c, a)

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
        # a < b < c
        cls.insert_120D_variant(db.add_user(3200, 'a_120D'),
                                db.add_user(3210, 'a_120D'),
                                db.add_user(3220, 'a_120D'), db)
        # a < c < b
        cls.insert_120D_variant(db.add_user(3201, 'a_120D'),
                                db.add_user(3221, 'a_120D'),
                                db.add_user(3211, 'a_120D'), db)
        # b < a < c
        cls.insert_120D_variant(db.add_user(3212, 'a_120D'),
                                db.add_user(3202, 'a_120D'),
                                db.add_user(3222, 'a_120D'), db)
        # b < c < a
        cls.insert_120D_variant(db.add_user(3223, 'a_120D'),
                                db.add_user(3203, 'a_120D'),
                                db.add_user(3213, 'a_120D'), db)
        # c < a < b
        cls.insert_120D_variant(db.add_user(3214, 'a_120D'),
                                db.add_user(3224, 'a_120D'),
                                db.add_user(3204, 'a_120D'), db)
        # c < b < a
        cls.insert_120D_variant(db.add_user(3225, 'a_120D'),
                                db.add_user(3215, 'a_120D'),
                                db.add_user(3205, 'a_120D'), db)

    @classmethod
    def insert_120D_variant(cls, a, b, c, db):
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(c, a)
        db.set_follows(c, b)

    def test_120D(self):
        rows = self.run_select('120D')
        all_triads = [(3202, 3212, 3222),
                      (3214, 3224, 3204),
                      (3215, 3225, 3205),
                      (3200, 3210, 3220),
                      (3203, 3223, 3213),
                      (3201, 3221, 3211)]
        self.assertEquals(len(rows), len(all_triads))
        self.assertSetEqual(set(rows), set(all_triads))

    @classmethod
    def insert_120U(cls, db):
        # a < b < c
        cls.insert_120U_variant(db.add_user(2800, 'a_120U'),
                                db.add_user(2810, 'a_120U'),
                                db.add_user(2820, 'a_120U'), db)
        # a < c < b
        cls.insert_120U_variant(db.add_user(2801, 'a_120U'),
                                db.add_user(2821, 'a_120U'),
                                db.add_user(2811, 'a_120U'), db)
        # b < a < c
        cls.insert_120U_variant(db.add_user(2812, 'a_120U'),
                                db.add_user(2802, 'a_120U'),
                                db.add_user(2822, 'a_120U'), db)
        # b < c < a
        cls.insert_120U_variant(db.add_user(2823, 'a_120U'),
                                db.add_user(2803, 'a_120U'),
                                db.add_user(2813, 'a_120U'), db)
        # c < a < b
        cls.insert_120U_variant(db.add_user(2814, 'a_120U'),
                                db.add_user(2824, 'a_120U'),
                                db.add_user(2804, 'a_120U'), db)
        # c < b < a
        cls.insert_120U_variant(db.add_user(2825, 'a_120U'),
                                db.add_user(2815, 'a_120U'),
                                db.add_user(2805, 'a_120U'), db)

    @classmethod
    def insert_120U_variant(cls, a, b, c, db):
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(a, c)
        db.set_follows(b, c)

    def test_120U(self):
        rows = self.run_select('120U')
        all_triads = [(2815, 2825, 2805),
                      (2800, 2810, 2820),
                      (2803, 2823, 2813),
                      (2801, 2821, 2811),
                      (2802, 2812, 2822),
                      (2814, 2824, 2804)]
        self.assertEquals(len(rows), len(all_triads))
        self.assertSetEqual(set(rows), set(all_triads))

    @classmethod
    def insert_201(cls, db):
        # a < b < c
        cls.insert_201_variant(db.add_user(3100, 'a_201'),
                               db.add_user(3110, 'a_201'),
                               db.add_user(3120, 'a_201'), db)
        # a < c < b
        cls.insert_201_variant(db.add_user(3101, 'a_201'),
                               db.add_user(3121, 'a_201'),
                               db.add_user(3111, 'a_201'), db)
        # b < a < c
        cls.insert_201_variant(db.add_user(3112, 'a_201'),
                               db.add_user(3102, 'a_201'),
                               db.add_user(3122, 'a_201'), db)
        # b < c < a
        cls.insert_201_variant(db.add_user(3123, 'a_201'),
                               db.add_user(3103, 'a_201'),
                               db.add_user(3113, 'a_201'), db)
        # c < a < b
        cls.insert_201_variant(db.add_user(3114, 'a_201'),
                               db.add_user(3124, 'a_201'),
                               db.add_user(3104, 'a_201'), db)
        # c < b < a
        cls.insert_201_variant(db.add_user(3125, 'a_201'),
                               db.add_user(3115, 'a_201'),
                               db.add_user(3105, 'a_201'), db)

    @classmethod
    def insert_201_variant(cls, a, b, c, db):
        db.set_regular(a)
        db.set_regular(b)
        db.set_regular(c)
        db.set_follows(a, b)
        db.set_follows(b, a)
        db.set_follows(a, c)
        db.set_follows(c, a)

    def test_201(self):
        rows = self.run_select('201')
        all_triads = [(3125, 3115, 3105),
                      (3114, 3124, 3104),
                      (3123, 3113, 3103),
                      (3100, 3120, 3110),
                      (3101, 3121, 3111),
                      (3112, 3122, 3102)]
        self.assertEquals(len(rows), len(all_triads))
        self.assertSetEqual(set(rows), set(all_triads))

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
