# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('nimsapi')

import bson.json_util

import nimsdata
import nimsdata.medimg

import base


class Projects(base.RequestHandler):

    """/projects """

    def __init__(self, request=None, response=None):
        super(Projects, self).__init__(request, response)
        self.dbc = self.app.db.projects

    def count(self):
        """Return the number of Projects."""
        if self.request.method == 'OPTIONS':
            return self.options()
        self.response.write(self.dbc.count())

    def post(self):
        """Create a new Project."""
        self.response.write('projects post\n')

    def _get(self, projection):
        query = None
        if self.public_request:
            query = {'public': True}
        elif not self.superuser:
            projection['permissions'] = {'$elemMatch': {'uid': self.uid, 'site': self.source_site}}
            if self.request.get('admin').lower() in ('1', 'true'):
                query = {'permissions': {'$elemMatch': {'uid': self.uid, 'share': True, 'site': self.source_site}}}
            else:
                query = {'permissions': {'$elemMatch': {'uid': self.uid, 'site': self.source_site}}}
        return list(self.dbc.find(query, projection))

    def get(self):
        """Return the list of Projects."""
        projection = {'group': 1, 'group_name': 1, 'name': 1, 'notes': 1}
        projects = self._get(projection)
        for proj in projects:
            proj['site'] = self.app.config['site_id']
            proj['site_name'] = self.app.config['site_name']
        if self.debug:
            for proj in projects:
                pid = str(proj['_id'])
                proj['details'] = self.uri_for('project', pid=pid, _full=True) + '?' + self.request.query_string
                proj['sessions'] = self.uri_for('sessions', pid=pid, _full=True) + '?' + self.request.query_string
        return projects

    def put(self):
        """Update many Projects."""
        self.response.write('projects put\n')


class Project(base.Container):

    """/projects/<pid> """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Project',
        'type': 'object',
        'properties': {
            '_id': {
            },
            'site': {
                'type': 'string',
            },
            'site_name': {
                'title': 'Site',
                'type': 'string',
            },
            'permissions': {
                'title': 'Permissions',
                'type': 'object',
                'minProperties': 1,
            },
            'files': {
                'title': 'Files',
                'type': 'array',
                'items': base.Container.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'group', 'name'], #FIXME
    }

    def __init__(self, request=None, response=None):
        super(Project, self).__init__(request, response)
        self.dbc = self.app.db.projects

    def schema(self, *args, **kwargs):
        return super(Project, self).schema(nimsdata.medimg.medimg.MedImgReader.project_properties)

    def get(self, pid):
        """Return one Project, conditionally with details."""
        _id = bson.ObjectId(pid)
        proj = self._get(_id)
        proj['site'] = self.app.config['site_id']
        proj['site_name'] = self.app.config['site_name']
        if self.debug:
            proj['sessions'] = self.uri_for('sessions', pid=pid, _full=True) + '?' + self.request.query_string
        return proj

    def put(self, pid):
        """Update an existing Project."""
        _id = bson.ObjectId(pid)
        self._get(_id, 'read-write')
        updates = {'$set': {'_id': _id}, '$unset': {'__null__': ''}}
        for k, v in self.request.params.iteritems():
            if k in ['notes']:
                if v is not None and v != '':
                    updates['$set'][k] = v # FIXME: do appropriate type conversion
                else:
                    updates['$unset'][k] = None
        self.dbc.update({'_id': _id}, updates)

    def delete(self, pid):
        """Delete an Project."""
        self.abort(501)
