# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('nimsapi')

import bson.json_util

import base


class Users(base.RequestHandler):

    """/nimsapi/users """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'User List',
        'type': 'array',
        'items': {
            'title': 'User',
            'type': 'object',
            'properties': {
                '_id': {
                    'title': 'Database ID',
                    'type': 'string',
                },
                'firstname': {
                    'title': 'First Name',
                    'type': 'string',
                    'default': '',
                },
                'lastname': {
                    'title': 'Last Name',
                    'type': 'string',
                    'default': '',
                },
                'email': {
                    'title': 'Email',
                    'type': 'string',
                    'format': 'email',
                    'default': '',
                },
                'email_hash': {
                    'type': 'string',
                    'default': '',
                },
            }
        }
    }

    def count(self):
        """Return the number of Users."""
        if self.request.method == 'OPTIONS':
            return self.options()
        self.response.write(self.app.db.users.count())

    def post(self):
        """Create a new User"""
        self.response.write('users post\n')

    def get(self):
        """Return the list of Users."""
        if self.uid == '@public':
            self.abort(403, 'must be logged in to retrieve User list')
        users = list(self.app.db.users.find({}, ['firstname', 'lastname', 'email_hash']))
        if self.debug:
            for user in users:
                user['metadata'] = self.uri_for('user', _id=str(user['_id']), _full=True) + '?' + self.request.query_string
        return users

    def put(self):
        """Update many Users."""
        self.response.write('users put\n')


class User(base.RequestHandler):

    """/nimsapi/users/<_id> """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'User',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'Database ID',
                'type': 'string',
            },
            'firstname': {
                'title': 'First Name',
                'type': 'string',
                'default': '',
            },
            'lastname': {
                'title': 'Last Name',
                'type': 'string',
                'default': '',
            },
            'email': {
                'title': 'Email',
                'type': 'string',
                'format': 'email',
                'default': '',
            },
            'email_hash': {
                'type': 'string',
                'default': '',
            },
            'superuser': {
                'title': 'Superuser',
                'type': 'boolean',
            },
        },
        'required': ['_id'],
    }

    def get(self, _id):
        """Return User details."""
        if self.uid == '@public':
            self.abort(403, 'must be logged in to retrieve User info')
        projection = []
        if self.request.get('remotes') in ('1', 'true'):
            projection += ['remotes']
        if self.request.get('status') in ('1', 'true'):
            projection += ['status']
        user = self.app.db.users.find_one({'_id': _id}, projection or None)
        if not user:
            self.abort(404, 'no such User')
        if self.debug:
            user['groups'] = self.uri_for('groups', _id=_id, _full=True) + '?' + self.request.query_string
        return user

    def put(self, _id):
        """Update an existing User."""
        user = self.app.db.users.find_one({'_id': _id})
        if not user:
            self.abort(404)
        if _id == self.uid or self.user_is_superuser: # users can only update their own info
            updates = {'$set': {'_id': _id}, '$unset': {'__null__': ''}}
            for k, v in self.request.params.iteritems():
                if k != 'superuser' and k in []:#user_fields:
                    updates['$set'][k] = v # FIXME: do appropriate type conversion
                elif k == 'superuser' and _id == self.uid and self.user_is_superuser is not None: # toggle superuser for requesting user
                    updates['$set'][k] = v.lower() in ('1', 'true')
                elif k == 'superuser' and _id != self.uid and self.user_is_superuser:             # enable/disable superuser for other user
                    if v.lower() in ('1', 'true') and user.get('superuser') is None:
                        updates['$set'][k] = False # superuser is tri-state: False indicates granted, but disabled, superuser privileges
                    elif v.lower() not in ('1', 'true'):
                        updates['$unset'][k] = ''
            self.app.db.users.update({'_id': _id}, updates)
        else:
            self.abort(403)

    def delete(self, _id):
        """Delete an User."""
        self.response.write('user %s delete, %s\n' % (_id, self.request.params))


class Groups(base.RequestHandler):

    """/nimsapi/groups """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Group List',
        'type': 'array',
        'items': {
            'title': 'Group',
            'type': 'object',
            'properties': {
                '_id': {
                    'title': 'Database ID',
                    'type': 'string',
                },
            }
        }
    }

    def count(self):
        """Return the number of Groups."""
        if self.request.method == 'OPTIONS':
            return self.options()
        self.response.write(self.app.db.groups.count())

    def post(self):
        """Create a new Group"""
        self.response.write('groups post\n')

    def get(self, _id=None):
        """Return the list of Groups."""
        query = None
        if _id is not None:
            if _id != self.uid and not self.user_is_superuser:
                self.abort(403, 'User ' + self.uid + ' may not see the Groups of User ' + _id)
            query = {'roles.uid': _id}
        else:
            if not self.user_is_superuser:
                if self.request.get('admin').lower() in ('1', 'true'):
                    query = {'roles': {'$elemMatch': {'uid': self.uid, 'role': 'admin'}}}
                else:
                    query = {'roles.uid': self.uid}
        groups = list(self.app.db.groups.find(query, ['name']))
        if self.debug:
            for group in groups:
                group['metadata'] = self.uri_for('group', _id=str(group['_id']), _full=True) + '?' + self.request.query_string
        return groups

    def put(self):
        """Update many Groups."""
        self.response.write('groups put\n')


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
                        'uid': {
                            'type': 'string',
                        },
                        'role': {
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
        if not self.user_is_superuser:
            group = self.app.db.groups.find_one({'_id': _id, 'roles': {'$elemMatch': {'uid': self.uid, 'role': 'admin'}}})
            if not group:
                self.abort(403, 'User ' + self.uid + ' is not an admin on Group ' + _id)
        return group

    def put(self, _id):
        """Update an existing Group."""
        self.response.write('group %s put, %s\n' % (_id, self.request.params))

    def delete(self, _id):
        """Delete an Group."""
