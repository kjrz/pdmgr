import ConfigParser
import logging

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, func, create_engine
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')

LIMIT = conf.getint('algorithm', 'limit')

LOG = logging.getLogger(conf.get('log', 'name'))


def init_db(db_path):
    engine = create_engine('sqlite:///' + db_path)
    Base.metadata.create_all(engine)


def drop_all(db_path):
    engine = create_engine('sqlite:///' + db_path)
    Base.metadata.drop_all(engine)


def get_session(db_path):
    engine = create_engine('sqlite:///' + db_path)
    Session = sessionmaker(bind=engine)
    return Session()


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


class Celeb(Base):
    __tablename__ = 'celeb'
    id = Column(Integer, ForeignKey('user.id'), primary_key=True)


class Private(Base):
    __tablename__ = 'private'
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

    def set_follows(self, follower, followee):
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
            .outerjoin((Celeb, Celeb.id == User.id)) \
            .filter(Celeb.id == None) \
            .outerjoin((Private, Private.id == User.id)) \
            .filter(Private.id == None) \
            .outerjoin(stmt, User.id == stmt.c.followee_id) \
            .order_by(stmt.c.followers_count.desc()) \
            .limit(n) \
            .all()

    def enough_is_enough(self):
        how_many = self.session.query(User).count()
        LOG.info("got {}".format(how_many))
        return how_many >= LIMIT

    def commit(self):
        self.session.commit()

    def close(self):
        self.session.close()

    def __init__(self, db_path):
        self.session = get_session(db_path)

class Stats:
    def users(self):
        return self.session.query(User).count()

    def follows(self):
        return self.session.query(Following).count()

    def celebs(self):
        return self.session.query(Celeb).count()

    def privates(self):
        return self.session.query(Private).count()

    def in_queue(self):
        stmt = self.session.query(
            Following.followee_id, func.count('*').label('followers_count')). \
            group_by(Following.followee_id). \
            subquery()
        return self.session.query(User, stmt.c.followers_count) \
            .filter(User.follows == None) \
            .outerjoin((Celeb, Celeb.id == User.id)) \
            .filter(Celeb.id == None) \
            .outerjoin((Private, Private.id == User.id)) \
            .filter(Private.id == None) \
            .outerjoin(stmt, User.id == stmt.c.followee_id) \
            .order_by(stmt.c.followers_count.desc()) \
            .count()

    def log(self):
        LOG.info("-----------------------")
        LOG.info("        stats")
        LOG.info("-----------------------")
        LOG.info("users \t = {}".format(self.users()))
        LOG.info("follows \t = {}".format(self.follows()))
        LOG.info("celebs \t = {}".format(self.celebs()))
        LOG.info("privates \t = {}".format(self.privates()))
        LOG.info("in queue \t = {}".format(self.in_queue()))
        LOG.info("-----------------------")
        return self

    def close(self):
        self.session.close()

    def __init__(self, db_path):
        self.session = get_session(db_path)
