# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('nimsapi')

import copy
import hashlib
import pymongo
import jsonschema
import bson.json_util

import base


class Users(base.RequestHandler):

    """/nimsapi/users """

    def __init__(self, request=None, response=None):
        super(Users, self).__init__(request, response)
        self.dbc = self.app.db.users

    def count(self):
        """Return the number of Users."""
        self.response.write(self.dbc.count())

    def post(self):
        """Create a new User"""
        if self.public_request: # FIXME: who is allowed to create a new user?
            self.abort(403, 'must be logged in to create new user')
        try:
            json_body = self.request.json_body
            jsonschema.validate(json_body, User.json_schema)
            json_body['email_hash'] = hashlib.md5(json_body['email']).hexdigest()
            self.dbc.insert(json_body)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        except pymongo.errors.DuplicateKeyError as e:
            self.abort(400, 'User ID %s already exists' % json_body['_id'])

    def get(self):
        """Return the list of Users."""
        if self.public_request:
            self.abort(403, 'must be logged in to retrieve User list')
        users = list(self.dbc.find({}, ['firstname', 'lastname', 'email_hash', 'wheel']))
        if self.debug:
            for user in users:
                user['details'] = self.uri_for('user', _id=str(user['_id']), _full=True) + '?' + self.request.query_string
        return users


class User(base.RequestHandler):

    """/nimsapi/users/<_id> """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'User',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'User ID',
                'type': 'string',
            },
            'firstname': {
                'title': 'First Name',
                'type': 'string',
            },
            'lastname': {
                'title': 'Last Name',
                'type': 'string',
            },
            'email': {
                'title': 'Email',
                'type': 'string',
                'format': 'email',
            },
            'email_hash': {
                'type': 'string',
            },
            'root': {
                'type': 'boolean',
            },
            'wheel': {
                'type': 'boolean',
            },
            'preferences': {
                'title': 'Preferences',
                'type': 'object',
                'properties': {
                    'data_layout': {
                        'type': 'string',
                    },
                },
            },
        },
        'required': ['_id', 'email', 'firstname', 'lastname'],
        'additionalProperties': False,
    }

    def __init__(self, request=None, response=None):
        super(User, self).__init__(request, response)
        self.dbc = self.app.db.users

    def self(self):
        """Return details for the current User."""
        user = self.dbc.find_one({'_id': self.uid}, ['firstname', 'lastname', 'root', 'wheel', 'preferences', 'email_hash'])
        if not user:
            self.abort(400, 'no user is logged in')
        user.setdefault('preferences', {})
        return user

    def get(self, _id):
        """ Return User details."""
        if self.public_request:
            self.abort(403, 'must be logged in to retrieve User info')
        projection = []
        if self.request.get('remotes') in ('1', 'true'):
            projection += ['remotes']
        if self.request.get('status') in ('1', 'true'):
            projection += ['status']
        user = self.dbc.find_one({'_id': _id}, projection or None)
        if not user:
            self.abort(404, 'no such User')
        if self.debug and (self.superuser_request or _id == self.uid):
            user['groups'] = self.uri_for('groups', _id=_id, _full=True) + '?' + self.request.query_string
        return user

    def put(self, _id):
        """Update an existing User."""
        user = self.dbc.find_one({'_id': _id})
        if not user:
            self.abort(404, 'no such User')
        if not self.superuser_request and _id != self.uid:
            self.abort(403, 'must be superuser to update another User')
        schema = copy.deepcopy(self.json_schema)
        del schema['required']
        try:
            json_body = self.request.json_body
            jsonschema.validate(json_body, schema)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        if 'wheel' in json_body and _id == self.uid:
            self.abort(400, 'user cannot alter own superuser privilege')
        self.dbc.update({'_id': _id}, {'$set': base.mongo_dict(json_body)})

    def delete(self, _id):
        """Delete a User."""
        if not self.superuser_request:
            self.abort(403, 'must be superuser to delete a User')
        self.dbc.remove({'_id': _id})


class Groups(base.RequestHandler):

    """/nimsapi/groups """

    def count(self):
        """Return the number of Groups."""
        self.response.write(self.app.db.groups.count())

    def post(self):
        """Create a new Group"""
        self.response.write('groups post\n')

    def get(self, _id=None):
        """Return the list of Groups."""
        query = None
        if _id is not None:
            if _id != self.uid and not self.superuser_request:
                self.abort(403, 'User ' + self.uid + ' may not see the Groups of User ' + _id)
            query = {'roles._id': _id}
        else:
            if not self.superuser_request:
                if self.request.get('admin').lower() in ('1', 'true'):
                    query = {'roles': {'$elemMatch': {'_id': self.uid, 'access': 'admin'}}}
                else:
                    query = {'roles._id': self.uid}
        groups = list(self.app.db.groups.find(query, ['name']))
        if self.debug:
            for group in groups:
                group['details'] = self.uri_for('group', _id=str(group['_id']), _full=True) + '?' + self.request.query_string
        return groups


class Group(base.RequestHandler):

    """/nimsapi/groups/<_id>"""

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Group',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'Database ID',
                'type': 'string',
            },
            'name': {
                'title': 'Name',
                'type': 'string',
                'maxLength': 32,
            },
            'roles': {
                'title': 'Roles',
                'type': 'array',
                'default': [],
                'items': {
                    'type': 'object',
                    'properties': {
                        '_id': {
                            'type': 'string',
                        },
                        'access': {
                            'type': 'string',
                            'enum': [k for k, v in sorted(base.INTEGER_ROLES.iteritems(), key=lambda (k, v): v)],
                        },
                    },
                },
                'uniqueItems': True,
            },
        },
        'required': ['_id'],
    }

    def get(self, _id):
        """Return Group details."""
        group = self.app.db.groups.find_one({'_id': _id})
        if not group:
            self.abort(404, 'no such Group: ' + _id)
        if not self.superuser_request:
            group = self.app.db.groups.find_one({'_id': _id, 'roles': {'$elemMatch': {'_id': self.uid, 'access': 'admin'}}})
            if not group:
                self.abort(403, 'User ' + self.uid + ' is not an admin of Group ' + _id)
        return group

    def put(self, _id):
        """Update an existing Group."""
        self.response.write('group %s put, %s\n' % (_id, self.request.params))

    def delete(self, _id):
        """Delete an Group."""
