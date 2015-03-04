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
    seen = Column(DateTime, default=func.now())


class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String(30), nullable=False)
    follows = relationship(
        'User', secondary='following',
        primaryjoin=(Following.follower_id == id),
        secondaryjoin=(Following.followee_id == id)
    )
    followed_by = relationship(
        'User', secondary='following',
        primaryjoin=(Following.followee_id == id),
        secondaryjoin=(Following.follower_id == id)
    )


class Private(Base):
    __tablename__ = 'private'
    id = Column(Integer, ForeignKey('user.id'), primary_key=True)


class Celeb(Base):
    __tablename__ = 'celeb'
    id = Column(Integer, ForeignKey('user.id'), primary_key=True)


class Maniac(Base):
    __tablename__ = 'maniac'
    id = Column(Integer, ForeignKey('user.id'), primary_key=True)


class Inactive(Base):
    __tablename__ = 'inactive'
    id = Column(Integer, ForeignKey('user.id'), primary_key=True)


class Mimesis:
    def known(self, table_class, id):
        return self.session.query(table_class) \
            .filter(table_class.id == id) \
            .first()

    def user_known(self, id):
        return self.known(table_class=User, id=id)

    def private_known(self, id):
        return self.known(table_class=Private, id=id)

    def celeb_known(self, id):
        return self.known(table_class=Celeb, id=id)

    def add_user(self, id, username):
        LOG.debug("adding user \"{}\"".format(username))
        user = User(id=id, username=username)
        self.session.add(user)
        return user

    def add_private(self, id):
        LOG.debug("adding private {}".format(id))
        private = Private(id=id)
        self.session.add(private)
        return private

    def add_celeb(self, id):
        LOG.debug('adding celeb {}'.format(id))
        celeb = Celeb(id=id)
        self.session.add(celeb)
        return celeb

    def add_maniac(self, id):
        LOG.debug('adding maniac {}'.format(id))
        maniac = Maniac(id=id)
        self.session.add(maniac)
        return maniac

    def add_inactive(self, id):
        LOG.debug('adding inactive {}'.format(id))
        inactive = Inactive(id=id)
        self.session.add(inactive)
        return inactive

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
            .filter(User.follows == None) \
            .outerjoin((Private, Private.id == User.id)) \
            .filter(Private.id == None) \
            .outerjoin((Celeb, Celeb.id == User.id)) \
            .filter(Celeb.id == None) \
            .outerjoin((Maniac, Maniac.id == User.id)) \
            .filter(Maniac.id == None) \
            .outerjoin((Inactive, Inactive.id == User.id)) \
            .filter(Inactive.id == None) \
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
        return self.session.query(Private).count()

    def celebs(self):
        return self.session.query(Celeb).count()

    def maniacs(self):
        return self.session.query(Maniac).count()

    def inactive(self):
        return self.session.query(Inactive).count()

    def queued(self):
        stmt = self.session.query(
            Following.followee_id, func.count('*').label('followers_count')). \
            group_by(Following.followee_id). \
            subquery()
        return self.session.query(User, stmt.c.followers_count) \
            .filter(User.follows == None) \
            .outerjoin((Private, Private.id == User.id)) \
            .filter(Private.id == None) \
            .outerjoin((Celeb, Celeb.id == User.id)) \
            .filter(Celeb.id == None) \
            .outerjoin((Maniac, Maniac.id == User.id)) \
            .filter(Maniac.id == None) \
            .outerjoin((Inactive, Inactive.id == User.id)) \
            .filter(Inactive.id == None) \
            .outerjoin(stmt, User.id == stmt.c.followee_id) \
            .order_by(stmt.c.followers_count.desc()) \
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
