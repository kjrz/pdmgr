import ConfigParser
import datetime
import unittest
from hamcrest import assert_that, equal_to, is_, has_items

import instapi
from mimesis import Mimesis, Location, Effort, Following, User, Attendance, Meeting
from trading_places import LonelyHeartsMiner, HeartsClubFinder

conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')

TEST_DB_PATH = conf.get('test', 'db')


class TestTassMimesis(unittest.TestCase):
    def setUp(self):
        self.mimesis = Mimesis(db_path=TEST_DB_PATH)
        self.clear_db()
        self.add_users()
        self.add_effort()

    def add_users(self):
        self.user_one = self.mimesis.add_user(1, 'one')
        self.user_two = self.mimesis.add_user(2, 'two')
        self.user_three = self.mimesis.add_user(3, 'three')
        self.add_following(self.user_one.id, self.user_two.id,
                           (datetime.datetime.now() - datetime.timedelta(minutes=20)))

    def clear_db(self):
        self.mimesis.session.query(Location).delete()
        self.mimesis.session.query(Effort).delete()
        self.mimesis.session.query(User).delete()
        self.mimesis.session.query(Following).delete()

    def add_effort(self):
        self.mimesis.session.add(Effort(fin=(datetime.datetime.now() - datetime.timedelta(minutes=10))))

    def add_following(self, follower_id, followee_id, first_seen=datetime.datetime.now()):
        self.mimesis.session.add(Following(follower_id=follower_id, followee_id=followee_id, first_seen=first_seen))

    def tearDown(self):
        self.mimesis.commit()
        self.mimesis.close()

    def test_get_active(self):
        # given
        self.add_following(self.user_two.id, self.user_three.id)

        # when
        active = self.mimesis.get_active()

        # then
        assert_that(len(active), is_(equal_to(2)))
        assert_that(active, has_items(2, 3))

    def test_add_location_if_not_exists(self):
        # when
        loc = self.mimesis.add_location(id=1, name='name', latitude=1.1, longitude=2.2)

        # then
        assert_that(loc.id, is_(equal_to(1)))
        assert_that(loc.name, is_(equal_to('name')))
        assert_that(loc.latitude, is_(equal_to(1.1)))
        assert_that(loc.longitude, is_(equal_to(2.2)))

        # when
        loc2 = self.mimesis.add_location(id=1, name='name')

        # then
        assert_that(loc2, is_(equal_to(loc)))

    def test_add_attendance(self):
        self.mimesis.add_attendance(1, 1, datetime.datetime.now())


class TestTassInstapi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.api = instapi.Session()
        cls.attendant_id = cls.api.search(conf.get('test', 'user')).id
        cls.until = datetime.datetime.strptime(conf.get('test', 'recently'), '%Y-%m-%d %H:%M:%S')

    def test_recent_locations(self):
        recent_media, next = self.api.api.user_recent_media(user_id=self.attendant_id)

        for media in recent_media:
            media = vars(media)

            if media['created_time'] < self.until:
                break

            if 'location' not in media:
                continue

            print media['created_time']
            print vars(media['location'])

    def test_attendances(self):
        for i in self.api.get_attendances(self.attendant_id, self.until):
            print i

    def test_more_recent_than_effort(self):
        pass


class TestAttendanceMiner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.api = instapi.Session()
        cls.attendant_id = cls.api.search(conf.get('test', 'user')).id
        cls.private_id = conf.get('test', 'private')
        cls.until = datetime.datetime.strptime(conf.get('test', 'recently'), '%Y-%m-%d %H:%M:%S')

    def setUp(self):
        db = self.get_db()
        self.clear_db(db)
        db.session.add(Effort(fin=(datetime.datetime.now() - datetime.timedelta(minutes=70))))
        db.commit()
        db.close()

    def get_db(self):
        return Mimesis(TEST_DB_PATH)

    def clear_db(self, db):
        db.session.query(User).delete()
        db.session.query(Following).delete()
        db.session.query(Effort).delete()
        db.session.query(Location).delete()
        db.session.query(Attendance).delete()

    def test_init_miner(self):
        # given
        db = self.get_db()
        one = db.add_user(1, 'one')
        two = db.add_user(2, 'two')
        db.add_user(3, 'three')
        db.set_follows(one, two)
        db.commit()
        db.close()

        # when
        miner = LonelyHeartsMiner(TEST_DB_PATH, None)

        # then
        assert_that(miner.lonely_people, has_items(1, 2))

    def test_attend_to_user(self):
        # given
        miner = LonelyHeartsMiner(TEST_DB_PATH, self.until)

        # when
        miner.attend_to(self.attendant_id)
        miner.close()

        # then
        db = self.get_db()
        locations = db.session.query(Location).all()
        attendances = db.session.query(Attendance).all()
        locations_count = conf.getint('test', 'locations')
        attendances_count = conf.getint('test', 'attendances')
        assert_that(len(locations), is_(equal_to(locations_count)))
        assert_that(len(attendances), is_(equal_to(attendances_count)))
        db.close()

    def test_omit_and_mark_private(self):
        # given
        miner = LonelyHeartsMiner(TEST_DB_PATH, self.until)
        miner.db.session.add(User(id=self.private_id, username='name'))

        # when
        miner.attend_to(self.private_id)
        miner.db.commit()

        # then
        breed = miner.db.user_known(self.private_id).breed
        assert_that(breed, is_(equal_to(3)))

    def sth(self):
        pass


class TestHeartsClubsFinder(unittest.TestCase):
    def setUp(self):
        db = self.get_db()
        self.clear_db(db)
        self.until = datetime.datetime.now() - datetime.timedelta(minutes=70)
        db.session.add(Effort(fin=self.until))
        self.init_test_data(db)
        db.commit()
        db.close()

    def get_db(self):
        return Mimesis(TEST_DB_PATH)

    def clear_db(self, db):
        db.session.query(User).delete()
        db.session.query(Following).delete()
        db.session.query(Effort).delete()
        db.session.query(Location).delete()
        db.session.query(Attendance).delete()
        db.session.query(Meeting).delete()

    def init_test_data(self, db):
        one = db.add_user(1, 'one')
        two = db.add_user(2, 'two')
        three = db.add_user(3, 'three')
        db.set_follows(one, two)
        db.add_location(1, 'somewhere')
        db.add_attendance(1, 1, (self.until + datetime.timedelta(minutes=10)))
        db.add_attendance(2, 1, (self.until + datetime.timedelta(minutes=15)))
        db.set_follows(one, three)
        db.add_location(2, 'somewhere else')
        db.add_attendance(1, 2, (self.until + datetime.timedelta(minutes=10)))
        db.add_attendance(3, 2, (self.until - datetime.timedelta(minutes=15)))
        db.commit()

    def tearDown(self):
        db = self.get_db()
        db.commit()
        db.close()

    def test_user_attendances(self):
        # given
        db = Mimesis(TEST_DB_PATH)

        # when
        attendances = db.get_attendances(1, self.until)

        # then
        assert_that(len(attendances), is_(equal_to(2)))
        db.close()

    def test_see_the_meeting(self):
        # given
        finder = HeartsClubFinder(TEST_DB_PATH, self.until)

        # when:
        finder.dig_meeting(1, 2)

        # then:
        meetings = finder.db.session.query(Meeting).all()
        assert_that(len(meetings), is_(1))
        assert_that(meetings[0].follower_id, is_(1))
        assert_that(meetings[0].followee_id, is_(2))
        assert_that(meetings[0].location, is_(1))
        finder.close()

    def test_see_no_meeting(self):
        # given
        finder = HeartsClubFinder(TEST_DB_PATH, self.until)

        # when:
        finder.dig_meeting(1, 3)

        # then:
        meetings = finder.db.session.query(Meeting).all()
        assert_that(len(meetings), is_(0))
        finder.close()

    def test_recent_followings(self):
        # given
        db = Mimesis(TEST_DB_PATH)

        # when
        recent = db.get_followings(self.until)

        # then
        assert_that(len(recent), is_(2))
        assert_that(recent, has_items((1, 2), (1, 3)))

    def sth(self):
        pass
