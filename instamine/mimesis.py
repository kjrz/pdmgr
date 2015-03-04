import ConfigParser
import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, func, create_engine
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

    def commit(self):
        self.session.commit()

    def close(self):
        self.session.close()

    @staticmethod
    def init_db(engine):
        if not engine.has_table("user"):
            Base.metadata.create_all(engine)

    def __init__(self, db_path):
        engine = create_engine('sqlite:///' + db_path)
        self.init_db(engine)
        session = sessionmaker(bind=engine)
        self.session = session()


class Stats:
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
