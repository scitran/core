# @author:  Gunnar Schaefer

import bson
import datetime

import scitran.data.medimg

from . import util
from . import users
from . import containers

PROJECT_POST_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'Project',
    'type': 'object',
    'properties': {
        'name': {
            'type': 'string',
            'maxLength': 32,
        },
    },
    'required': ['name'],
}

PROJECT_PUT_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'Project',
    'type': 'object',
    'properties': {
        'name': {
            'type': 'string',
            'maxLength': 32,
        },
        'notes': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'author': {
                        'type': 'string',
                    },
                    'timestamp': {
                        'type': 'string',
                        'format': 'date-time',
                    },
                    'text': {
                        'type': 'string',
                    },
                },
                'required': ['text'],
                'additionalProperties': False,
            },
        },
        'permissions': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'access': {
                        'type': 'string',
                        'enum': [role['rid'] for role in users.ROLES],
                    },
                    '_id': {
                        'type': 'string',
                    },
                    'site': {
                        'type': 'string',
                    },
                },
                'required': ['access', '_id'],
                'additionalProperties': False,
            },
        },
        'public': {
            'type': 'boolean',
        },
    },
    'minProperties': 1,
    'additionalProperties': False,
}


class Projects(containers.ContainerList):

    """/projects """

    post_schema = PROJECT_POST_SCHEMA

    def __init__(self, request=None, response=None):
        super(Projects, self).__init__(request, response)
        self.dbc = self.app.db.projects

    def count(self):
        """Return the number of Projects."""
        self.response.write(self.dbc.count())

    def post(self, gid):
        """Create a new Project."""
        json_body = self._post()
        group = self.app.db.groups.find_one({'_id': gid}, ['roles'])
        if not group:
            self.abort(400, 'invalid group id')
        if not self.superuser_request and util.user_perm(group['roles'], self.uid).get('access') != 'admin':
            self.abort(400, 'must be group admin to create project')
        json_body['group'] = gid
        if self.request.GET.get('inherit', '').lower() in ('1', 'true'):
            json_body['permissions'] = group['roles']
        else:
            json_body['permissions'] = [{'_id': self.uid, 'access': 'admin'}]
        json_body['public'] = json_body.get('public', False)
        json_body['files'] = []
        json_body['timestamp'] = datetime.datetime.utcnow()
        return {'_id': str(self.dbc.insert_one(json_body).inserted_id)}

    def get(self, uid=None, gid=None):
        """Return the User's list of Projects."""
        if gid is not None:
            group = self.app.db.groups.find_one({'_id': gid}, [])
            if not group:
                self.abort(400, 'invalid group id')
        query = {'group': gid} if gid else {}
        projection = ['group', 'name', 'notes', 'timestamp', 'timezone']
        projects = self._get(query, projection, self.request.GET.get('admin', '').lower() in ('1', 'true'), uid)
        if self.debug:
            for proj in projects:
                pid = str(proj['_id'])
                proj['debug'] = {}
                proj['debug']['group'] = self.uri_for('group', proj['group'], _full=True) + '?' + self.request.query_string
                proj['debug']['details'] = self.uri_for('project', pid, _full=True) + '?' + self.request.query_string
                proj['debug']['sessions'] = self.uri_for('p_sessions', pid=pid, _full=True) + '?' + self.request.query_string
        return projects

    def groups(self):
        """Return the User's list of Project Groups."""
        group_ids = list(set((p['group'] for p in self.get())))
        return list(self.app.db.groups.find({'_id': {'$in': group_ids}}, ['name']))


class Project(containers.Container):

    """/projects/<pid> """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Project',
        'type': 'object',
        'properties': {
            '_id': {
            },
            'permissions': {
                'title': 'Permissions',
                'type': 'object',
                'minProperties': 1,
            },
            'files': {
                'title': 'Files',
                'type': 'array',
                'items': containers.FILE_SCHEMA,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'group', 'name'], #FIXME
    }

    put_schema = PROJECT_PUT_SCHEMA

    def __init__(self, request=None, response=None):
        super(Project, self).__init__(request, response)
        self.dbc = self.app.db.projects

    def schema(self, *args, **kwargs):
        return super(Project, self).schema(scitran.data.medimg.medimg.MedImgReader.project_properties)

    def get(self, pid):
        """Return one Project, conditionally with details."""
        _id = bson.ObjectId(pid)
        proj, _ = self._get(_id)
        if self.debug:
            proj['debug'] = {}
            proj['debug']['group'] = self.uri_for('group', proj['group'], _full=True) + '?' + self.request.query_string
            proj['debug']['sessions'] = self.uri_for('p_sessions', pid=pid, _full=True) + '?' + self.request.query_string
        return proj

    def put(self, pid):
        """Update an existing Project."""
        _id = bson.ObjectId(pid)
        json_body = super(Project, self)._put(_id)
        if 'permissions' in json_body or 'public' in json_body:
            updates = {}
            if 'permissions' in json_body:
                updates['permissions'] = json_body['permissions']
            if 'public' in json_body:
                updates['public'] = json_body['public']
            session_ids = [s['_id'] for s in self.app.db.sessions.find({'project': _id}, [])]
            self.app.db.sessions.update_many({'project': _id}, {'$set': updates})
            self.app.db.acquisitions.update_many({'session': {'$in': session_ids}}, {'$set': updates})

    def delete(self, pid):
        """Delete a Project."""
        _id = bson.ObjectId(pid)
        self._get(_id, 'admin', perm_only=True)
        session_ids = [s['_id'] for s in self.app.db.sessions.find({'project': _id}, [])]
        if session_ids:
            self.abort(400, 'project contains sessions and cannot be deleted')
        self._delete(_id)
