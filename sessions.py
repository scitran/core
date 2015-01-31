# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('scitran.api')

import bson.json_util

import scitran.data.medimg

import containers

SESSION_PUT_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'Session',
    'type': 'object',
    'properties': {
        'name': {
            'title': 'Name',
            'type': 'string',
            'maxLength': 32,
        },
        'notes': {
            'title': 'Notes',
            'type': 'string',
        },
        'files': {
            'title': 'Files',
            'type': 'array',
            'items': containers.FILE_SCHEMA,
            'uniqueItems': True,
        },
    },
    'additionalProperties': False,
}


class Sessions(containers.ContainerList):

    """/sessions """

    def __init__(self, request=None, response=None):
        super(Sessions, self).__init__(request, response)
        self.dbc = self.app.db.sessions

    def count(self):
        """Return the number of Sessions."""
        self.response.write(self.dbc.count())

    def post(self):
        """Create a new Session"""
        self.response.write('sessions post\n')

    def get(self, pid):
        """Return the list of Project Sessions."""
        _id = bson.ObjectId(pid)
        if not self.app.db.projects.find_one({'_id': _id}):
            self.abort(404, 'no such Project')
        query = {'project': _id}
        projection = {'label': 1, 'subject.code': 1, 'notes': 1}
        sessions = self._get(query, projection, self.request.get('admin').lower() in ('1', 'true'))
        if self.debug:
            for sess in sessions:
                sid = str(sess['_id'])
                sess['details'] = self.uri_for('session', sid, _full=True) + '?' + self.request.query_string
                sess['acquisitions'] = self.uri_for('acquisitions', sid, _full=True) + '?' + self.request.query_string
        return sessions

    def put(self):
        """Update many Sessions."""
        self.response.write('sessions put\n')


class Session(containers.Container):

    """/sessions/<sid> """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Session',
        'type': 'object',
        'properties': {
            '_id': {
            },
            'project': {
            },
            'files': {
                'title': 'Files',
                'type': 'array',
                'items': containers.FILE_SCHEMA,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'project', 'uid', 'patient_id', 'subject'], #FIXME
    }

    put_schema = SESSION_PUT_SCHEMA

    def __init__(self, request=None, response=None):
        super(Session, self).__init__(request, response)
        self.dbc = self.app.db.sessions

    def schema(self, *args, **kwargs):
        return super(Session, self).schema(scitran.data.medimg.medimg.MedImgReader.session_properties)

    def get(self, sid):
        """Return one Session, conditionally with details."""
        _id = bson.ObjectId(sid)
        sess = self._get(_id)
        if self.debug:
            sess['acquisitions'] = self.uri_for('acquisitions', sid, _full=True) + '?' + self.request.query_string
        return sess

    def put(self, sid):
        """Update an existing Session."""
        _id = bson.ObjectId(sid)
        self._put(_id)

    def delete(self, sid):
        """Delete a Session."""
        self.abort(501)
