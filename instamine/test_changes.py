import ConfigParser
import unittest
import datetime
from triad_mine import TriadClassifier

from mimesis import Mimesis, Following
from mimesis import Effort

from test_triads import TestQueries


conf = ConfigParser.RawConfigParser()
conf.read('../conf/test.conf')


class TestingChangesMimesis(Mimesis):
    def effort_minutes_ago(self, m):
        record = Effort(fin=datetime.datetime.now() - datetime.timedelta(minutes=m))
        self.session.add(record)

    def last_fin(self):
        return self.session.query(Effort).first().fin

    def they_go_way_back(self, follower_id, followee_id, to):
        following = self.session.query(Following).filter_by(follower_id=follower_id, followee_id=followee_id).first()
        following.first_seen = to


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

    def set_first_seen(self, db, edges, before):
        for edge in edges:
            db.they_go_way_back(edge[0], edge[1], before)
        db.commit()


class TestTriadChangeFinder(TestQueries):
    # TODO
    pass


if __name__ == '__main__':
    unittest.main()
