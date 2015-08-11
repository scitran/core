# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('scitran.api')

import copy
import hashlib
import pymongo
import datetime
import jsonschema

import base
import util

ROLES = [
    {
        'rid': 'ro',
        'name': 'Read-Only',
    },
    {
        'rid': 'rw',
        'name': 'Read-Write',
    },
    {
        'rid': 'admin',
        'name': 'Admin',
    },
]

INTEGER_ROLES = {r['rid']: i for i, r in enumerate(ROLES)}


class Users(base.RequestHandler):

    """/users """

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
            json_body.setdefault('email', json_body['_id'])
            json_body.setdefault('preferences', {})
            json_body.setdefault('avatar', 'https://gravatar.com/avatar/' + hashlib.md5(json_body['email']).hexdigest() + '?s=512&d=mm')
            self.dbc.insert(json_body)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        except pymongo.errors.DuplicateKeyError as e:
            self.abort(400, 'User ID %s already exists' % json_body['_id'])

    def get(self):
        """Return the list of Users."""
        if self.public_request:
            self.abort(403, 'must be logged in to retrieve User list')
        users = list(self.dbc.find({}, {'preferences': False}))
        if self.debug:
            for user in users:
                user['debug'] = {}
                user['debug']['details'] = self.uri_for('user', str(user['_id']), _full=True) + '?' + self.request.query_string
        return users


class User(base.RequestHandler):

    """/users/<_id> """

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
            'avatar': {
                'type': 'string',
                'format': 'uri',
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
        'required': ['_id', 'firstname', 'lastname'],
        'additionalProperties': False,
    }

    def __init__(self, request=None, response=None):
        super(User, self).__init__(request, response)
        self.dbc = self.app.db.users

    def self(self):
        """Return details for the current User."""
        user = self.dbc.find_one({'_id': self.uid})
        if not user:
            self.abort(400, 'no user is logged in')
        return user

    def roles(self):
        """Return the list of user roles."""
        return ROLES

    def get(self, _id):
        """ Return User details."""
        if self.public_request:
            self.abort(403, 'must be logged in to retrieve User info')
        projection = []
        if self.request.GET.get('remotes', '').lower() in ('1', 'true'):
            projection += ['remotes']
        if self.request.GET.get('status', '').lower() in ('1', 'true'):
            projection += ['status']
        user = self.dbc.find_one({'_id': _id}, projection or None)
        if not user:
            self.abort(404, 'no such User')
        if self.debug and (self.superuser_request or _id == self.uid):
            user['debug'] = {}
            user['debug']['groups'] = self.uri_for('groups', _id, _full=True) + '?' + self.request.query_string
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
        if _id == self.uid and 'wheel' in json_body and json_body['wheel'] != user['wheel']:
            self.abort(400, 'user cannot alter own superuser privilege')
        self.dbc.update({'_id': _id}, {'$set': util.mongo_dict(json_body)})

    def delete(self, _id):
        """Delete a User."""
        if not self.superuser_request:
            self.abort(403, 'must be superuser to delete a User')
        self.dbc.remove({'_id': _id})


class Groups(base.RequestHandler):

    """/groups """

    def __init__(self, request=None, response=None):
        super(Groups, self).__init__(request, response)
        self.dbc = self.app.db.groups

    def count(self):
        """Return the number of Groups."""
        self.response.write(self.app.db.groups.count())

    def post(self):
        """Create a new Group"""
        if not self.superuser_request:
            self.abort(403, 'must be logged in and superuser to create new group')
        try:
            json_body = self.request.json_body
            jsonschema.validate(json_body, Group.post_schema)
            json_body['created'] = datetime.datetime.utcnow()
            json_body['modified'] = datetime.datetime.utcnow()
            json_body.setdefault('roles', [])
            self.dbc.insert(json_body)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        except pymongo.errors.DuplicateKeyError as e:
            self.abort(400, 'Groups ID %s already exists' % json_body['_id'])

    def get(self, _id=None):
        """Return the list of Groups."""
        query = None
        projection = ['name']
        if _id is not None:
            if _id != self.uid and not self.superuser_request:
                self.abort(403, 'User ' + self.uid + ' may not see the Groups of User ' + _id)
            query = {'roles._id': _id}
            projection += ['roles.$']
        else:
            if not self.superuser_request:
                if self.request.GET.get('admin', '').lower() in ('1', 'true'):
                    query = {'roles': {'$elemMatch': {'_id': self.uid, 'access': 'admin'}}}
                else:
                    query = {'roles._id': self.uid}
                projection += ['roles.$']
        groups = list(self.app.db.groups.find(query, projection))
        #for group in groups:
        #    group['created'], _ = util.format_timestamp(group['created']) # TODO json serializer should do this
        #    group['modified'], _ = util.format_timestamp(group['modified']) # TODO json serializer should do this
        if self.debug:
            for group in groups:
                group['debug'] = {}
                group['debug']['projects'] = self.uri_for('g_projects', gid=group['_id'], _full=True) + '?' + self.request.query_string
                group['debug']['sessions'] = self.uri_for('g_sessions', gid=group['_id'], _full=True) + '?' + self.request.query_string
                group['debug']['details'] = self.uri_for('group', group['_id'], _full=True) + '?' + self.request.query_string
        return groups


class Group(base.RequestHandler):

    """/groups/<_id>"""

    def __init__(self, request=None, response=None):
        super(Group, self).__init__(request, response)
        self.dbc = self.app.db.groups

    def schema(self):
        method =self.request.GET.get('method', '').lower()
        if method == 'put':
            return self.put_schema
        return self.post_schema

    def get(self, _id):
        """Return Group details."""
        group = self.app.db.groups.find_one({'_id': _id})
        if not group:
            self.abort(404, 'no such Group: ' + _id)
        if not self.superuser_request:
            group = self.app.db.groups.find_one({'_id': _id, 'roles': {'$elemMatch': {'_id': self.uid, 'access': 'admin'}}})
            if not group:
                self.abort(403, 'User ' + self.uid + ' is not an admin of Group ' + _id)
        if 'created' in group and 'modified' in group:
            group['created'], _ = util.format_timestamp(group['created']) # TODO json serializer should do this
            group['modified'], _ = util.format_timestamp(group['modified']) # TODO json serializer should do this
        if self.debug:
            group['debug'] = {}
            group['debug']['projects'] = self.uri_for('g_projects', gid=group['_id'], _full=True) + '?' + self.request.query_string
            group['debug']['sessions'] = self.uri_for('g_sessions', gid=group['_id'], _full=True) + '?' + self.request.query_string
        return group

    def put(self, _id):
        """Update an existing Group."""
        group = self.dbc.find_one({'_id': _id})
        if not group:
            self.abort(404, 'no such Group')
        user_perm = util.user_perm(group.get('roles', []), self.uid)
        if not self.superuser_request and not user_perm.get('access') == 'admin':
            self.abort(403, 'must be superuser or group admin to update group')
        try:
            json_body = self.request.json_body
            jsonschema.validate(json_body, self.put_schema)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        self.dbc.update({'_id': _id}, {'$set': util.mongo_dict(json_body)})

    def delete(self, _id):
        """Delete an Group."""
        if not self.superuser_request:
            self.abort(403, 'must be superuser to delete a Group')
        project_ids = [p['_id'] for p in self.app.db.projects.find({'group': _id}, [])]
        if project_ids:
            self.abort(400, 'group contains projects and cannot be deleted')
        self.dbc.delete_one({'_id': _id})
