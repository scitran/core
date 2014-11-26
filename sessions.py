# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('nimsapi')

import bson.json_util

import nimsdata
import nimsdata.medimg

import base


class Sessions(base.RequestHandler):

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
        if self.public_request:
            query['public'] = True
        elif not self.superuser_request:
            query['permissions'] = {'$elemMatch': {'uid': self.uid, 'site': self.source_site}}
            projection['permissions'] = {'$elemMatch': {'uid': self.uid, 'site': self.source_site}}
        sessions =  list(self.dbc.find(query, projection))
        if self.debug:
            for sess in sessions:
                sid = str(sess['_id'])
                sess['details'] = self.uri_for('session', sid=sid, _full=True) + '?' + self.request.query_string
                sess['acquisitions'] = self.uri_for('acquisitions', sid=sid, _full=True) + '?' + self.request.query_string
        return sessions

    def put(self):
        """Update many Sessions."""
        self.response.write('sessions put\n')


class Session(base.Container):

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
                'items': base.Container.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'project', 'uid', 'patient_id', 'subject'], #FIXME
    }

    def __init__(self, request=None, response=None):
        super(Session, self).__init__(request, response)
        self.dbc = self.app.db.sessions

    def schema(self, *args, **kwargs):
        return super(Session, self).schema(nimsdata.medimg.medimg.MedImgReader.session_properties)

    def get(self, sid):
        """Return one Session, conditionally with details."""
        _id = bson.ObjectId(sid)
        sess = self._get(_id)
        if self.debug:
            sess['acquisitions'] = self.uri_for('acquisitions', sid=sid, _full=True) + '?' + self.request.query_string
        return sess

    def put(self, sid):
        """Update an existing Session."""
        _id = bson.ObjectId(sid)
        self._get(_id, 'modify')
        updates = {'$set': {'_id': _id}, '$unset': {'__null__': ''}}
        for k, v in self.request.params.iteritems():
                if v is not None and v != '':
                    updates['$set'][k] = v # FIXME: do appropriate type conversion
                else:
                    updates['$unset'][k] = None
        self.dbc.update({'_id': _id}, updates)

    def delete(self, sid):
        """Delete a Session."""
        self.abort(501)
