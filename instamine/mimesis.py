import ConfigParser
import logging
import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, func, create_engine, Float
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')

LOG = logging.getLogger(conf.get('log', 'name'))

Base = declarative_base()


class Following(Base):
    __tablename__ = 'following'
    follower_id = Column(Integer, ForeignKey('user.id'), primary_key=True)
    followee_id = Column(Integer, ForeignKey('user.id'), primary_key=True)
    first_seen = Column(DateTime, default=func.now())


class Breed(Base):
    UNKNOWN = 'unknown'
    REGULAR = 'regular'
    PRIVATE = 'private'
    CELEB = 'celeb'
    MANIAC = 'maniac'
    INACTIVE = 'inactive'

    __tablename__ = 'breed'
    id = Column(Integer, nullable=False, primary_key=True)
    name = Column(String(8), nullable=False)


class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, nullable=False, primary_key=True)
    username = Column(String(30), nullable=False)
    breed = Column(Integer, ForeignKey('breed.id'), default=1)
    followees = Column(Integer, nullable=True)
    follows = relationship(
        'User', secondary='following',
        primaryjoin=(Following.follower_id == id),
        secondaryjoin=(Following.followee_id == id)
    )
    followers = Column(Integer, nullable=True)
    followed_by = relationship(
        'User', secondary='following',
        primaryjoin=(Following.followee_id == id),
        secondaryjoin=(Following.follower_id == id)
    )


class Effort(Base):
    __tablename__ = 'effort'
    id = Column(Integer, primary_key=True)
    fin = Column(DateTime, default=func.now())


class Location(Base):
    __tablename__ = 'location'
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String, nullable=False)
    latitude = Column(Float)
    longitude = Column(Float)


class Attendance(Base):
    __tablename__ = 'attendance'
    id = Column(Integer, primary_key=True, nullable=False)
    user = Column(Integer, ForeignKey('user.id'), nullable=False)
    location = Column(Integer, ForeignKey('location.id'), nullable=False)
    time_seen = Column(DateTime, default=func.now(), nullable=False)


class Meeting(Base):
    __tablename__ = 'meeting'
    id = Column(Integer, primary_key=True, nullable=False)
    follower_id = Column(Integer, ForeignKey('user.id'))
    followee_id = Column(Integer, ForeignKey('user.id'))
    location_id = Column(Integer, ForeignKey('location.id'), nullable=False)

    follower = relationship("User", foreign_keys=[follower_id])
    followee = relationship("User", foreign_keys=[followee_id])
    location = relationship("Location", foreign_keys=[location_id])


class Mimesis:
    def user_known(self, id):
        return self.session.query(User) \
            .filter(User.id == id) \
            .first()

    def user_id(self, username):
        return self.session.query(User) \
            .filter(User.username == username) \
            .first()

    def add_user(self, id, username):
        LOG.debug("adding user \"{}\"".format(username))
        user = User(id=id, username=username)
        self.session.add(user)
        return user

    def set_regular(self, user):
        LOG.debug("setting regular {}".format(user))
        user.breed = self.breed_map[Breed.REGULAR]

    def set_private(self, user):
        LOG.debug("setting private {}".format(user))
        user.breed = self.breed_map[Breed.PRIVATE]

    def set_celeb(self, user):
        LOG.debug('setting celeb {}'.format(user))
        user.breed = self.breed_map[Breed.CELEB]

    def set_maniac(self, user):
        LOG.debug('setting maniac {}'.format(user))
        user.breed = self.breed_map[Breed.MANIAC]

    def set_inactive(self, user):
        LOG.debug('setting inactive {}'.format(user))
        user.breed = self.breed_map[Breed.MANIAC]

    @staticmethod
    def set_followers(user, count):
        LOG.debug('setting {} followers for {}'.format(count, user))
        user.followers = count

    @staticmethod
    def set_followees(user, count):
        LOG.debug('setting {} followees for {}'.format(count, user))
        user.followees = count

    @staticmethod
    def set_follows(follower, followee):
        if followee in follower.follows:
            LOG.debug("not adding relationship \"{}\" -> \"{}\"".format(
                follower.username,
                followee.username))
            return
        LOG.debug("adding relationship \"{}\" -> \"{}\"".format(
            follower.username,
            followee.username))
        follower.follows.append(followee)

    def follows(self, follower, followee):
        return self.session.query(Following) \
            .filter(Following.follower_id == follower.id) \
            .filter(Following.followee_id == followee.id) \
            .first()

    def the_unvisited(self, n):
        stmt = self.session.query(
            Following.followee_id, func.count('*').label('followers_count')). \
            group_by(Following.followee_id). \
            subquery()
        return self.session.query(User, stmt.c.followers_count) \
            .join(Breed) \
            .filter(Breed.name == Breed.UNKNOWN) \
            .outerjoin(stmt, User.id == stmt.c.followee_id) \
            .order_by(stmt.c.followers_count.desc()) \
            .limit(n) \
            .all()

    def dig_triad(self, triad_name):
        return self.session.execute(open('sql/triads/' + triad_name + '.sql').read())

    def get_triad(self, nodes, before):
        return self.session.query(Following.follower_id, Following.followee_id, Following.first_seen).\
            filter(Following.follower_id.in_(nodes)).\
            filter(Following.followee_id.in_(nodes)).\
            filter(Following.first_seen < before).\
            all()

    def all_regular(self):
        return self.session.query(User.id).join(Breed) \
            .filter(Breed.name == Breed.REGULAR) \
            .all()

    def all_users(self):
        return self.session.query(User.id).all()

    def get_active(self, until=None):
        if not until:
            until = self.last_fin()
        followings = self.session.query(Following).filter(Following.first_seen > until)
        active = set()
        for following in followings:
            active.add(following.follower_id)
            active.add(following.followee_id)
        return active

    def add_location(self, id, name, latitude=None, longitude=None):
        loc = self.session.query(Location).filter(Location.id == id).first()
        if loc is None:
            LOG.debug("new location: {}".format(name.encode('utf8')))
            loc = Location(id=id, name=name, latitude=latitude, longitude=longitude)
            self.session.add(loc)
        return loc

    def add_attendance(self, user_id, location_id, time_seen):
        attendance = Attendance(user=user_id, location=location_id, time_seen=time_seen)
        self.session.add(attendance)
        return attendance

    def get_followings(self, until):
        return self.session.query(Following.follower_id, Following.followee_id) \
            .filter(Following.first_seen > until) \
            .all()

    def get_attendances(self, user_id, until):
        return self.session.query(Attendance.location) \
            .filter(Attendance.user == user_id) \
            .filter(Attendance.time_seen > until) \
            .all()

    def add_meeting(self, follower_id, followee_id, location_id):
        self.session.add(Meeting(follower_id=follower_id, followee_id=followee_id, location_id=location_id))

    def get_meetings_info(self):
        return self.session.execute(open("sql/tass/meetings.sql").read()).fetchall()

    def effort_fin(self):
        record = Effort()
        LOG.debug("setting effort fin: {}".format(unicode(record.fin)))
        self.session.add(record)

    def last_fin(self):
        return self.session.query(func.max(Effort.fin)).first()[0]

    def first_fin(self):
        return self.session.query(func.min(Effort.fin)).first()[0]

    def no_efforts_yet(self):
        return not self.session.query(Effort).first()

    def commit(self):
        self.session.commit()

    def close(self):
        self.session.close()

    @staticmethod
    def init_db(engine):
        LOG.info('init db')
        if not engine.has_table('user'):
            Base.metadata.create_all(engine)

    def init_breed(self):
        breed_count, = self.session.query(func.count(Breed.id)).first()
        LOG.info('breed count: {}'.format(breed_count))
        breed_map = {}
        if breed_count != 0:
            for breed in self.session.query(Breed):
                breed_map[breed.name] = breed.id
            return breed_map
        LOG.info('init breed')
        breeds = [
            Breed.UNKNOWN,
            Breed.REGULAR,
            Breed.PRIVATE,
            Breed.CELEB,
            Breed.MANIAC,
            Breed.INACTIVE
        ]
        for i in range(len(breeds)):
            self.session.add(Breed(id=i+1, name=breeds[i]))
            breed_map[breeds[i]] = i + 1
        LOG.info('breed map: {}'.format(breed_map))
        return breed_map

    def __init__(self, db_path=conf.get('db', 'path')):
        engine = create_engine('sqlite:///' + db_path)
        self.init_db(engine)
        session = sessionmaker(bind=engine)
        self.session = session()
        self.breed_map = self.init_breed()


class MySqlTriadMimesis:
    def insert_classified_triad(self, nodes, classification, first_seen):
        self.c.execute("INSERT INTO triad (a_id, b_id, c_id, triad_type_id, first_seen) "
                       "VALUES (%s, %s, %s, (SELECT id FROM triad_type WHERE name = %s), %s)",
                       (nodes[0], nodes[1], nodes[2], classification, first_seen))
        return self.c.lastrowid

    def write_triads(self, triads, triad_type, first_seen=datetime.datetime.now()):
        self.c.execute("SELECT id FROM triad_type "
                       "WHERE name = %s", (triad_type,))
        triad_type_id = self.c.fetchone()[0]

        LOG.info("appending triad type...")
        triads = [(a_id, b_id, c_id, triad_type_id, first_seen) for a_id, b_id, c_id in triads]

        LOG.info("splitting in chunks...")
        triads = MySqlTriadMimesis.chunks(triads, 10000)

        LOG.info("writing triads to db...")
        triads_added = 0
        for portion in triads:
            self.c.executemany("INSERT INTO triad (a_id, b_id, c_id, triad_type_id, first_seen) "
                               "VALUES (%s, %s, %s, %s, %s)", portion)
            triads_added += len(portion)
            LOG.info("{}...".format(triads_added))
            self.conn.commit()

        LOG.info("<done>")

    @staticmethod
    def chunks(l, n):
        for i in xrange(0, len(l), n):
            yield l[i:i+n]

    def get_new_triads(self):
        self.c.execute("SELECT triad.id, a_id, b_id, c_id, name "
                       "FROM triad "
                       "JOIN triad_type ON (triad_type_id = triad_type.id) "
                       "AND first_seen > (SELECT MAX(fin) FROM effort)")
        return self.c.fetchall()

    def get_prev_triads(self, nodes):
        a_id = nodes[0]
        b_id = nodes[1]
        c_id = nodes[2]
        self.c.execute(open("sql/mysql/change.sql").read(),
                       (a_id, b_id, c_id,
                        a_id, b_id, c_id,
                        a_id, b_id, c_id))
        prev_triads = self.c.fetchall()
        if len(prev_triads) == 0:
            return None
        else:
            return prev_triads[0]

    def write_change(self, from_triad_id, to_triad_id):
        self.c.execute("INSERT INTO triad_change (from_triad, to_triad) VALUES (%s, %s)",
                       (from_triad_id, to_triad_id))

    def effort_fin(self):
        self.c.execute("INSERT INTO effort () VALUES ()")

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def __init__(self, conn):
        self.conn = conn
        self.c = conn.cursor()


class UserStats:
    def users(self):
        return self.session.query(User).count()

    def follows(self):
        return self.session.query(Following).count()

    def privates(self):
        return self.session.query(User).join(Breed) \
            .filter(Breed.name == Breed.PRIVATE) \
            .count()

    def celebs(self):
        return self.session.query(User).join(Breed) \
            .filter(Breed.name == Breed.CELEB) \
            .count()

    def maniacs(self):
        return self.session.query(User).join(Breed) \
            .filter(Breed.name == Breed.MANIAC) \
            .count()

    def inactive(self):
        return self.session.query(User).join(Breed) \
            .filter(Breed.name == Breed.INACTIVE) \
            .count()

    def queued(self):
        return self.session.query(User).join(Breed) \
            .filter(Breed.name == Breed.UNKNOWN) \
            .count()

    def log(self):
        users = self.users()
        celebs = self.celebs()
        privates = self.privates()
        maniacs = self.maniacs()
        inactive = self.inactive()
        queued = self.queued()
        regular = users - privates - celebs - maniacs - inactive - queued
        processed = users - queued
        LOG.info("-----------------------")
        LOG.info("        stats")
        LOG.info("-----------------------")
        LOG.info("users \t = {}".format(users))
        LOG.info("processed \t = {}".format(processed))
        LOG.info("queued \t = {}".format(queued))
        LOG.info("regular \t = {}".format(regular))
        LOG.info("privates \t = {}".format(privates))
        LOG.info("celebs \t = {}".format(celebs))
        LOG.info("maniacs \t = {}".format(maniacs))
        LOG.info("inactive \t = {}".format(inactive))
        LOG.info("-----------------------")
        return self

    def close(self):
        self.session.close()

    def __init__(self, db_path):
        engine = create_engine('sqlite:///' + db_path)
        session = sessionmaker(bind=engine)
        self.session = session()

# mimesis = Mimesis(db_path=conf.get('test', 'db'))
# mimesis.session.add(Location(name='somewhere'))
