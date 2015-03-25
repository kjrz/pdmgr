import ConfigParser
import logging
import os
import shutil
from time import sleep
import unittest

from mockito import *


CONF_PATH = '../instamine.conf'
TEST_CONF_PATH = '../conf/test.conf'
PROD_CONF_PATH = '../conf/prod.conf'
shutil.copyfile(TEST_CONF_PATH, CONF_PATH)

from mimesis import Mimesis
from triad_mine import TriadsToAttendTo, TriadFinder, TriadMiner

conf = ConfigParser.RawConfigParser()
conf.read('../conf/test.conf')

DB_PATH = conf.get('db', 'path')

LOG = logging.getLogger(conf.get('log', 'name'))


class TestTriadMiner(unittest.TestCase):
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
        cls.zero_three_zero_c(db)
        db.commit()
        db.close()

    @classmethod
    def zero_three_zero_c(cls, db):
        a = db.add_user(1, 'a0_030C')
        b = db.add_user(2, 'b0_030C')
        c = db.add_user(3, 'c0_030C')
        db.set_follows(a, b)
        db.set_follows(b, c)
        db.set_follows(c, a)
        a = db.add_user(4, 'a1_030C')
        b = db.add_user(5, 'b2_030C')
        c = db.add_user(6, 'c3_030C')
        db.set_follows(a, b)
        db.set_follows(b, c)
        db.set_follows(c, a)
        db.add_user(7, 'new_boy')

    def setUp(self):
        pass

    def test_triads_list(self):
        self.assertListEqual(TriadsToAttendTo().types,
                             ['111D', '030C', '030T', '111U', '201', '120D', '120U', '120C', '210'])

    def test_dig_triad_type(self):
        db = Mimesis(DB_PATH)
        res = db.dig_triad('030C').fetchall()
        db.close()
        self.assertListEqual(res, [(1, 2, 3), (4, 5, 6)])

    def test_founder(self):
        finder = TriadFinder()
        finder.work()
        finder.effort_fin()
        finder.close()

    def test_miner_init(self):
        miner = TriadMiner()
        miner.get_fired()

    def test_new_triad(self):
        # given
        miner = TriadMiner()
        miner.api = mock()
        when(miner.api).followees(any()).thenReturn([])
        when(miner.api).followees(2).thenReturn([(1, 'a0_030C'), (7, 'new_boy')])
        finder = TriadFinder()

        # when
        miner.dig()
        finder.work()
        self.assertListEqual(miner.db.the_changed(), [(1, 4)])
        miner.dig_changes()

    @classmethod
    def tearDownClass(cls):
        shutil.copyfile(PROD_CONF_PATH, CONF_PATH)


if __name__ == '__main__':
    unittest.main()
