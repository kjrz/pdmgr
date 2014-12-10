import logging
import ConfigParser
from datetime import datetime

from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError


conf = ConfigParser.RawConfigParser()
conf.read('../instamine.conf')

ACCESS_TOKEN = conf.get('api', 'access_token')
HOUR_SEC = 3600
HOUR_MAX = int(conf.get('api', 'hour_max'))
CLIP = int(conf.get('algorithm', 'clip'))

LOG = logging.getLogger(conf.get('log', 'name'))


class OneHourApiCallsLimitReached(Exception):
    pass


class UserPrivateException(Exception):
    def __init__(self, arg):
        self.arg = arg

    def __str__(self):
        return repr(self.arg)


class Followees:
    def __init__(self, data):
        self.data = data
        self.index = 0

    def trim(self, size):
        self.data = self.data[:size]

    def size(self):
        return len(self.data)

    def __iter__(self):
        return self

    def __len__(self):
        return len(self.data)

    def next(self):
        if self.index == len(self.data):
            raise StopIteration
        user = self.data[self.index]
        self.index += 1
        return user.id, user.username


class Session:
    def search(self, username):
        LOG.debug('issue user search: {}'.format(username))
        self.shoot()
        response = self.api.user_search(username)
        return response[0] if len(response) > 0 else None

    def info(self, id):
        LOG.debug("issue user info: {}".format(id))
        response = self.risk_private(self.api.user, id)
        self.shoot()
        return vars(response)

    def popularity(self, id):
        LOG.debug("issue popularity: {}".format(id))
        return self.info(id).get('counts').get('followed_by')

    def followees(self, id):
        LOG.debug("issue followees: {}".format(id))
        response = self.get_paginated(self.api.user_follows, id)
        LOG.debug("{} follows {} users".format(id, len(response)))
        return Followees(response)

    def get_paginated(self, request, arg):
        results, next = self.risk_private(request, arg)
        LOG.debug("paginated 1st shot: {}".format(len(results)))
        self.shoot()
        while next:
            more_results, next = request(with_next_url=next)
            LOG.debug("paginated next shot: {}".format(len(more_results)))
            self.shoot()
            results.extend(more_results)
        return results

    @staticmethod
    def risk_private(request, arg):
        try:
            return request(arg)
        except InstagramAPIError as e:
            LOG.debug("error for user {}".format(arg))
            if e.status_code == 400:
                raise UserPrivateException(arg)
            else:
                raise e

    def shoot(self):
        if self.hour_shots > HOUR_MAX:
            raise OneHourApiCallsLimitReached
        self.ammo -= 1
        self.hour_shots += 1

    def ammo_left(self):
        return self.ammo

    def reload(self):
        if self.been_an_hour():
            self.new_hour()
        self.ammo = CLIP

    def been_an_hour(self):
        been_sec = (datetime.now() - self.hour_start).seconds
        LOG.info("been {} minutes this hour".format(been_sec / 60))
        LOG.info("fired {} shots so far".format(self.hour_shots))
        return been_sec > HOUR_SEC

    def new_hour(self):
        self.hour_shots = 0
        self.hour_start = datetime.now()
        LOG.info("new hour from {}".format(
            self.hour_start.time().strftime("%H:%M")))

    def __init__(self):
        self.api = InstagramAPI(access_token=ACCESS_TOKEN)
        self.new_hour()
        self.ammo = 0
        self.reload()
