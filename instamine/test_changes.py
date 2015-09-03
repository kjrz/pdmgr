import ConfigParser
import mysql.connector
import unittest
import datetime

from triad_mine import TriadClassifier, TriadFinder, TriadChangeFinder
from mimesis import Mimesis, Following, MySqlTriadMimesis
from mimesis import Effort
from test_triads import TestQueries

conf = ConfigParser.RawConfigParser()
conf.read('../conf/test.conf')


class TestingChangesMimesis(Mimesis):
    def effort_minutes_ago(self, m):
        record = Effort(fin=datetime.datetime.now() - datetime.timedelta(minutes=m))
        self.session.add(record)

    def new_fin(self):
        fin = self.last_fin() + datetime.timedelta(minutes=2)
        self.session.add(Effort(fin=fin))
        self.session.commit()

    def they_go_way_back(self, follower_id, followee_id, to):
        following = self.session.query(Following).filter_by(follower_id=follower_id, followee_id=followee_id).first()
        following.first_seen = to

    def add_following(self, follower_id, followee_id):
        first_seen = self.last_fin() + datetime.timedelta(minutes=1)
        self.session.add(Following(follower_id=follower_id, followee_id=followee_id, first_seen=first_seen))


class TestingMySqlTriadMimesis(MySqlTriadMimesis):
    def last_triad_id(self):
        self.c.execute("SELECT count(*) FROM triad")
        last_triad_id = self.c.fetchone()[0]
        return last_triad_id

    def move_triads_in_time(self, last_triad_id):
        fin = self.last_fin() + datetime.timedelta(minutes=1)
        self.c.execute("UPDATE triad SET first_seen = %s WHERE id > %s", (fin, last_triad_id))
        self.conn.commit()

    def write_triads(self, triads, triad_type):
        last_triad_id = self.last_triad_id()
        MySqlTriadMimesis.write_triads(self, triads, triad_type)
        self.move_triads_in_time(last_triad_id)

    def last_fin(self):
        self.c.execute("SELECT max(fin) FROM effort")
        return self.c.fetchone()[0]

    def new_fin(self):
        fin = self.last_fin() + datetime.timedelta(minutes=2)
        self.c.execute("INSERT INTO effort (fin) VALUES (%s)", (fin,))
        self.conn.commit()


class TestClassifier(unittest.TestCase):
    def test_003(self):
        self.assertEqual(TriadClassifier.classify([]), '003')

    def test_012(self):
        self.assertEqual(TriadClassifier.classify([('a', 'b')]), '012')

    def test_102(self):
        self.assertEqual(TriadClassifier.classify([('a', 'b'), ('b', 'a')]), '102')

    def test_021C(self):
        self.assertEqual(TriadClassifier.classify([('a', 'b'), ('b', 'c')]), '021C')

    def test_021D(self):
        self.assertEqual(TriadClassifier.classify([('a', 'b'), ('a', 'c')]), '021D')

    def test_021U(self):
        self.assertEqual(TriadClassifier.classify([('a', 'b'), ('c', 'b')]), '021U')

    def test_None(self):
        self.assertEqual(TriadClassifier.classify([('a', 'b'), ('c', 'd')]), None)
        self.assertEqual(TriadClassifier.classify([('a', 'a'), ('a', 'a'), ('a', 'a')]), None)


class TestChanges(TestQueries):
    def test_030C(self):
        nodes = [10, 11, 12]
        self.with_just_these_it_was(nodes, [], '003')
        self.with_just_these_it_was(nodes, [(10, 11)], '012')
        self.with_just_these_it_was(nodes, [(10, 11), (11, 12)], '021C')

    def test_030T(self):
        nodes = [13, 14, 15]
        self.with_just_these_it_was(nodes, [], '003')
        self.with_just_these_it_was(nodes, [(13, 14)], '012')
        self.with_just_these_it_was(nodes, [(13, 14), (14, 15)], '021C')
        self.with_just_these_it_was(nodes, [(13, 14), (13, 15)], '021D')
        self.with_just_these_it_was(nodes, [(13, 15), (14, 15)], '021U')

    def test_111D(self):
        nodes = [16, 17, 18]
        self.with_just_these_it_was(nodes, [(18, 17)], '012')
        self.with_just_these_it_was(nodes, [(16, 17), (17, 16)], '102')
        pass

    def test_111U(self):
        pass

    def test_120C(self):
        pass

    def test_120D(self):
        pass

    def test_120U(self):
        pass

    def test_201(self):
        pass

    def test_210(self):
        pass

    def test_300(self):
        pass

    def with_just_these_it_was(self, nodes, edges, triad_type):
        db = TestingChangesMimesis(db_path=conf.get('db', 'path'))
        last_fin = db.last_fin()
        self.set_first_seen(db, edges, last_fin - datetime.timedelta(minutes=1))
        followings = db.get_triad(nodes, last_fin)
        self.assertEqual(TriadClassifier.classify(followings), triad_type)
        self.set_first_seen(db, edges, last_fin + datetime.timedelta(minutes=1))
        db.close()

    @staticmethod
    def set_first_seen(db, edges, before):
        for edge in edges:
            db.they_go_way_back(edge[0], edge[1], before)
        db.commit()


class TestTriadChangeFinder(TestQueries):
    @classmethod
    def setUpClass(cls):
        TestQueries.setUpClass()
        triad_conn = mysql.connector.connect(user='instamine',
                                             password='instamine',
                                             host='localhost',
                                             database='test')
        c = triad_conn.cursor()
        for line in open("sql/mysql/instamine-ddl-lines.sql"):
            c.execute(line)
        triad_conn.commit()
        triad_mimesis = TestingMySqlTriadMimesis(triad_conn)
        finder = TriadFinder(triad_mimesis)
        triad_mimesis.effort_fin()
        db = TestingChangesMimesis()
        cls.first_mine_cycle(db, finder, triad_mimesis)
        triad_conn.close()

    def test_003_030C_120C_210(self):
        db = TestingChangesMimesis()
        triad_conn = mysql.connector.connect(user='instamine',
                                             password='instamine',
                                             host='localhost',
                                             database='test')
        triad_mimesis = TestingMySqlTriadMimesis(triad_conn)
        finder = TriadFinder(triad_mimesis)
        changes = TriadChangeFinder(triad_mimesis)

        self.add_followings(db, [(40, 41), (41, 42), (42, 40)])
        self.mine_cycle(changes, db, finder, triad_mimesis)
        self.assertChangesList(triad_conn, 1, {40, 41, 42, '003', '030C'})

        self.add_followings(db, [(41, 40)])
        self.mine_cycle(changes, db, finder, triad_mimesis)
        self.assertChangesList(triad_conn, 2, {40, 41, 42, '030C', '120C'})

        self.add_followings(db, [(42, 41)])
        self.mine_cycle(changes, db, finder, triad_mimesis)
        self.assertChangesList(triad_conn, 3, {40, 41, 42, '120C', '210'})

    def test_030C_300(self):
        db = TestingChangesMimesis()
        triad_conn = mysql.connector.connect(user='instamine',
                                             password='instamine',
                                             host='localhost',
                                             database='test')
        triad_mimesis = TestingMySqlTriadMimesis(triad_conn)
        finder = TriadFinder(triad_mimesis)
        changes = TriadChangeFinder(triad_mimesis)

        self.add_followings(db, [(11, 10), (12, 11), (10, 12)])
        self.mine_cycle(changes, db, finder, triad_mimesis)
        self.assertChangesList(triad_conn, 4, {10, 11, 12, '030C', '300'})

    def test_030C(self):
        pass

    def test_030T(self):
        # self.add_followings(db, [(15, 13), (14, 13), (15, 14)])
        pass

    def test_111D(self):
        pass

    def test_111U(self):
        pass

    def test_120C(self):
        pass

    def test_120D(self):
        pass

    def test_120U(self):
        pass

    def test_201(self):
        pass

    def test_210(self):
        pass

    def test_300(self):
        pass

    @staticmethod
    def add_followings(db, followings):
        for follower_id, followee_id in followings:
            db.add_following(follower_id, followee_id)
        db.commit()

    @staticmethod
    def first_mine_cycle(db, finder, triad_mimesis):
        finder.work()
        triad_mimesis.new_fin()
        db.new_fin()

    @staticmethod
    def mine_cycle(changes, db, finder, triad_mimesis):
        finder.work()
        changes.dig_changes()
        triad_mimesis.new_fin()
        db.new_fin()

    def assertChangesList(self, triad_conn, changes_count, new_change):
        changes_list = self.changes_list(triad_conn)
        self.assertEquals(len(changes_list), changes_count)
        self.assertNewChange(changes_list, new_change)

    @staticmethod
    def changes_list(triad_conn):
        c = triad_conn.cursor()
        c.execute(open("sql/mysql/list_changes.sql").read())
        changes_list = c.fetchall()
        return changes_list

    def assertNewChange(self, changes_list, new_change):
        last_change = changes_list[-1]
        self.assertSetEqual(set(last_change[:5]), new_change)

    @classmethod
    def tearDownClass(cls):
        TestQueries.tearDownClass()

    # SELECT a_id AS a, b_id AS b, c_id AS c, name AS type, first_seen AS since
    # FROM triad
    # JOIN triad_type ON triad_type_id = triad_type.id
    # ORDER BY since;

if __name__ == '__main__':
    unittest.main()
