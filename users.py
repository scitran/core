# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('nimsapi')

import bson.json_util

import nimsapiutil


class Users(nimsapiutil.NIMSRequestHandler):

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
        return list(self.app.db.users.find({}, ['firstname', 'lastname', 'email_hash']))

    def put(self):
        """Update many Users."""
        self.response.write('users put\n')


class User(nimsapiutil.NIMSRequestHandler):

    """/nimsapi/users/<uid> """

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

    def get(self, uid):
        """Return User details."""
        if self.uid == '@public':
            self.abort(403, 'must be logged in to retrieve User info')
        projection = []
        if self.request.get('remotes') in ('1', 'true'):
            projection += ['remotes']
        if self.request.get('status') in ('1', 'true'):
            projection += ['status']
        user = self.app.db.users.find_one({'_id': uid}, projection or None)
        if not user:
            self.abort(404, 'no such User')
        return user

    def put(self, uid):
        """Update an existing User."""
        user = self.app.db.users.find_one({'_id': uid})
        if not user:
            self.abort(404)
        if uid == self.uid or self.user_is_superuser: # users can only update their own info
            updates = {'$set': {}, '$unset': {}}
            for k, v in self.request.params.iteritems():
                if k != 'superuser' and k in []:#user_fields:
                    updates['$set'][k] = v # FIXME: do appropriate type conversion
                elif k == 'superuser' and uid == self.uid and self.user_is_superuser is not None: # toggle superuser for requesting user
                    updates['$set'][k] = v.lower() in ('1', 'true')
                elif k == 'superuser' and uid != self.uid and self.user_is_superuser:             # enable/disable superuser for other user
                    if v.lower() in ('1', 'true') and user.get('superuser') is None:
                        updates['$set'][k] = False # superuser is tri-state: False indicates granted, but disabled, superuser privileges
                    elif v.lower() not in ('1', 'true'):
                        updates['$unset'][k] = ''
            self.app.db.users.update({'_id': uid}, updates)
        else:
            self.abort(403)

    def delete(self, uid):
        """Delete an User."""
        self.response.write('user %s delete, %s\n' % (uid, self.request.params))


class Groups(nimsapiutil.NIMSRequestHandler):

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

    def get(self):
        """Return the list of Groups."""
        query = None
        if not self.user_is_superuser:
            if self.request.get('admin').lower() in ('1', 'true'):
                query = {'roles': {'$elemMatch': {'uid': self.uid, 'role': 'admin'}}}
            elif self.request.get('experiment_permissions').lower() in ('1', 'true'):
                experiments = [exp['_id'] for exp in self.app.db.experiments.aggregate([
                        {'$match': {'permissions.uid': self.uid}},
                        {'$group': {'_id': '$group'}},
                        ])['result']]
                query = {'_id': {'$in': experiments}}
            else:
                query = {'roles.uid': self.uid}
        return list(self.app.db.groups.find(query, ['name']))

    def put(self):
        """Update many Groups."""
        self.response.write('groups put\n')


class Group(nimsapiutil.NIMSRequestHandler):

    """/nimsapi/groups/<gid>"""

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
                            'enum': [k for k, v in sorted(nimsapiutil.INTEGER_ROLES.iteritems(), key=lambda (k, v): v)],
                        },
                    },
                },
                'uniqueItems': True,
            },
        },
        'required': ['_id'],
    }

    def get(self, gid):
        """Return Group details."""
        group = self.app.db.groups.find_one({'_id': gid})
        if not group:
            self.abort(404, 'no such Group: ' + gid)
        group = self.app.db.groups.find_one({'_id': gid, 'roles': {'$elemMatch': {'uid': self.uid, 'role': 'admin'}}})
        if not group:
            self.abort(403, 'User ' + self.uid + ' is not an admin on Group ' + gid)
        return group

    def put(self, gid):
        """Update an existing Group."""
        self.response.write('group %s put, %s\n' % (gid, self.request.params))

    def delete(self, gid):
        """Delete an Group."""
