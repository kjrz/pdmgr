import ConfigParser
import logging

import instapi
from mimesis import Mimesis, Meeting

conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')

LOG = logging.getLogger(conf.get('log', 'name'))


class LonelyHeartsMiner:
    def reload(self):
        self.api.reload()
        self.stats()

    def dig(self):
        while self.api.ammo_left() > 0 and len(self.lonely_people):
            user_id = self.lonely_people.pop()
            user = self.db.user_known(user_id)
            LOG.info("next up -> instagram.com/{}".format(user.username))
            self.attend_to(user_id)
        self.db.commit()

    def attend_to(self, id):
        attendances = self.get_attendances(id)
        if attendances is None:
            return
        for time, loc in attendances:
            LOG.info("           {}".format(loc.name.encode('utf8')))
            self.db.add_location(loc.id, loc.name, loc.point.latitude, loc.point.longitude)
            self.db.add_attendance(id, loc.id, time)
            self.attendances += 1

    def get_attendances(self, id):
        try:
            return self.api.get_attendances(id, self.until)
        except instapi.UserPrivateException:
            LOG.info("<private>")
            self.db.set_private(self.db.user_known(id))

    def got_work_to_do(self):
        return len(self.lonely_people) > 0

    def stats(self):
        LOG.info("=========trading places stats========")
        LOG.info("     lonely people = {}".format(self.people_to_start_with))
        LOG.info("  yet to attend to = {}".format(len(self.lonely_people)))
        LOG.info("       attendances = {}".format(self.attendances))
        LOG.info("==============stats end==============")

    def close(self):
        self.db.commit()
        self.db.close()

    def __init__(self, db_path, until):
        self.api = instapi.Session()
        self.db = Mimesis(db_path)
        self.lonely_people = self.db.get_active(until)
        self.people_to_start_with = len(self.lonely_people)
        self.until = until
        self.attendances = 0
        self.stats()


class HeartsClubFinder:
    def dig(self):
        followings = self.db.get_followings(self.until)
        LOG.info("recent followings: {}".format(len(followings)))
        for follower_id, followee_id in followings:
            self.dig_meeting(follower_id, followee_id)
        LOG.info("done digging meetings")

    def dig_meeting(self, follower_id, followee_id):
        follower_at = self.get_attendances(follower_id)
        followee_at = self.get_attendances(followee_id)
        meetings = follower_at.intersection(followee_at)
        for loc in meetings:
            self.count_your_meetings()
            self.db.add_meeting(follower_id, followee_id, loc)
        self.db.commit()

    def get_attendances(self, user_id):
        attendances = self.db.get_attendances(user_id, self.until)
        return {loc for loc, in attendances}

    def count_your_meetings(self):
        self.meetings_count += 1
        LOG.info("meetings: {}".format(self.meetings_count))

    def close(self):
        self.db.close()

    def __init__(self, db_path, until):
        self.db = Mimesis(db_path)
        self.until = until
        self.meetings_count = 0


class HearsClubResults:
    def get_meetings_info(self):
        return self.db.get_meetings_info()

    def close(self):
        self.db.close()

    def __init__(self, db_path):
        self.db = Mimesis(db_path)
