# @author:  Kevin S Hahn

"""
API request handlers for Apps.

represents the /nimsapi/apps route
"""

import bson
import logging
log = logging.getLogger('nimsapi.jobs')

import base

# TODO: create schemas to verify various json payloads
APP_SCHEMA = {}


class Apps(base.RequestHandler):

    """Return information about the all the apps."""

    def get(self):
        return list(self.app.db.apps.find())

    # TODO: add post route

    def count(self):
        return self.app.db.apps.count()


class App(base.RequestHandler):

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'App',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'App ID',
                'type': 'string',
            },
        },
        'required': ['_id'],
        'additionalProperties': True,
    }

    def get(self, _id):
        _id = bson.ObjectId(_id)
