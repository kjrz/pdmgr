import ConfigParser
import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, func, create_engine, and_
from sqlalchemy.orm import relationship, sessionmaker, aliased
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


class User(Base):
    class Breed(object):
        UNKNOWN = 'unknown'
        REGULAR = 'regular'
        PRIVATE = 'private'
        CELEB = 'celeb'
        MANIAC = 'maniac'
        INACTIVE = 'inactive'

    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String(30), nullable=False)
    breed = Column(String(10), default=Breed.UNKNOWN)
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


class Change(Base):
    __tablename__ = 'change'
    from_triad_id = Column(Integer, ForeignKey('triad.id'), nullable=False, primary_key=True)
    # from_triad = relationship('triad', primaryjoin=(Triad.id == from_triad_id))
    to_triad_id = Column(Integer, ForeignKey('triad.id'), nullable=True, primary_key=True)


class Triad(Base):
    __tablename__ = 'triad'
    id = Column(Integer, primary_key=True)
    a_id = Column(Integer, ForeignKey('user.id'), nullable=False)
    b_id = Column(Integer, ForeignKey('user.id'), nullable=False)
    c_id = Column(Integer, ForeignKey('user.id'), nullable=False)
    triad_type = Column(String(4), nullable=False)
    first_seen = Column(DateTime, default=func.now())
    # changed_to = relationship(
    #     'triad', secondary='changes',
    #     primaryjoin=(Change.from_triad_id == id),
    #     secondaryjoin=(Change.to_triad_id == id)
    # )  # TODO: test
    # changed_from = relationship(
    #     'triad', secondary='changes',
    #     primaryjoin=(Change.to_triad_id == id),
    #     secondaryjoin=(Change.from_triad_id == id)
    # )  # TODO: test


class Effort(Base):
    __tablename__ = 'effort'
    id = Column(Integer, primary_key=True)
    fin = Column(DateTime, default=func.now())


class Mimesis:
    def user_known(self, id):
        return self.session.query(User) \
            .filter(User.id == id) \
            .first()

    def add_user(self, id, username):
        LOG.debug("adding user \"{}\"".format(username))
        user = User(id=id, username=username)
        self.session.add(user)
        return user

    @staticmethod
    def set_regular(user):
        LOG.debug("setting regular {}".format(user))
        user.breed = User.Breed.REGULAR

    @staticmethod
    def set_private(user):
        LOG.debug("setting private {}".format(user))
        user.breed = User.Breed.PRIVATE

    @staticmethod
    def set_celeb(user):
        LOG.debug('setting celeb {}'.format(user))
        user.breed = User.Breed.CELEB

    @staticmethod
    def set_maniac(user):
        LOG.debug('setting maniac {}'.format(user))
        user.breed = User.Breed.MANIAC

    @staticmethod
    def set_inactive(user):
        LOG.debug('setting inactive {}'.format(user))
        user.breed = User.Breed.INACTIVE

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
        LOG.debug("adding relationship \"{}\" -> \"{}\"".format(
            follower.username,
            followee.username))
        follower.follows.append(followee)

    def follows(self, follower, followee):
        query_result = self.session.query(Following) \
            .filter(Following.follower_id == follower.id) \
            .filter(Following.followee_id == followee.id) \
            .all()
        return len(query_result) == 1

    def the_unvisited(self, n):
        stmt = self.session.query(
            Following.followee_id, func.count('*').label('followers_count')). \
            group_by(Following.followee_id). \
            subquery()
        return self.session.query(User, stmt.c.followers_count) \
            .filter(User.breed == User.Breed.UNKNOWN) \
            .outerjoin(stmt, User.id == stmt.c.followee_id) \
            .order_by(stmt.c.followers_count.desc()) \
            .limit(n) \
            .all()

    def dig_triad(self, triad_name):
        return self.session.execute(open('sql/triads/' + triad_name + '.sql').read())

    def add_triad(self, a_id, b_id, c_id, triad_type):
        if self.triad_known(a_id, b_id, c_id, triad_type):
            return
        LOG.debug("adding triad {}-{}-{}: {}".format(a_id, b_id, c_id, triad_type))
        triad = Triad(a_id=a_id, b_id=b_id, c_id=c_id, triad_type=triad_type)
        self.session.add(triad)
        return triad

    def triad_known(self, a_id, b_id, c_id, triad_type):
        return self.session.query(Triad) \
            .filter(Triad.a_id == a_id) \
            .filter(Triad.b_id == b_id) \
            .filter(Triad.c_id == c_id) \
            .filter(Triad.triad_type == triad_type) \
            .first()

    def the_unstable(self):
        # TODO: only dig unstable triads
        return self.session.query(Triad.a_id, Triad.b_id, Triad.c_id) \
            .outerjoin(Change, Triad.id == Change.from_triad_id) \
            .filter(Change.from_triad_id == None) \
            .all()

    def add_change(self, from_triad, to_triad):
        self.session.add(Change(from_triad_id=from_triad, to_triad_id=to_triad))

    def the_changed(self):
        LOG.info('digging changes')
        prev_run = self.session.query(func.max(Effort.fin)).first()[0]
        LOG.info('previous run: {}'.format(prev_run))
        from_triad = aliased(Triad)
        to_triad = aliased(Triad)
        known_change = aliased(Change)
        return self.session.query(from_triad.id, to_triad.id) \
            .join(to_triad, and_(to_triad.a_id.in_((from_triad.a_id, from_triad.b_id, from_triad.c_id)),
                                 to_triad.b_id.in_((from_triad.a_id, from_triad.b_id, from_triad.c_id)),
                                 to_triad.c_id.in_((from_triad.a_id, from_triad.b_id, from_triad.c_id))
                                 )) \
            .filter(from_triad.first_seen < prev_run) \
            .filter(to_triad.first_seen > prev_run) \
            .outerjoin(known_change, known_change.from_triad_id == from_triad.id) \
            .filter(known_change.from_triad_id == None) \
            .all()

    def effort_fin(self):
        record = Effort()
        self.session.add(record)

    def commit(self):
        self.session.commit()

    def close(self):
        self.session.close()

    @staticmethod
    def init_db(engine):
        LOG.info('init db')
        if not engine.has_table('user'):
            Base.metadata.create_all(engine)

    def __init__(self, db_path=conf.get('db', 'path')):
        engine = create_engine('sqlite:///' + db_path)
        self.init_db(engine)
        session = sessionmaker(bind=engine)
        self.session = session()


class UserStats:
    def users(self):
        return self.session.query(User).count()

    def follows(self):
        return self.session.query(Following).count()

    def privates(self):
        return self.session.query(User) \
            .filter(User.breed == User.Breed.PRIVATE) \
            .count()

    def celebs(self):
        return self.session.query(User) \
            .filter(User.breed == User.Breed.CELEB) \
            .count()

    def maniacs(self):
        return self.session.query(User) \
            .filter(User.breed == User.Breed.MANIAC) \
            .count()

    def inactive(self):
        return self.session.query(User) \
            .filter(User.breed == User.Breed.INACTIVE) \
            .count()

    def queued(self):
        return self.session.query(User) \
            .filter(User.breed == User.Breed.UNKNOWN) \
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


class TriadStats:
    def triad_count(self):
        return self.session.query(Triad).count()

    def close(self):
        self.session.close()

    def __init__(self, db_path):
        engine = create_engine('sqlite:///' + db_path)
        session = sessionmaker(bind=engine)
        self.session = session()
