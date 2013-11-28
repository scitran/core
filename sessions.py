# @author:  Gunnar Schaefer

import json
import webapp2
import bson.json_util

import nimsapiutil


class Sessions(nimsapiutil.NIMSRequestHandler):

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Session List',
        'type': 'array',
        'items': {
            'title': 'Session',
            'type': 'object',
            'properties': {
                '_id': {
                    'title': 'Database ID',
                },
                'timestamp': {
                    'title': 'Timestamp',
                },
                'subject': {
                    'title': 'Subject Code',
                    'type': 'string',
                },
            }
        }
    }

    def count(self, iid):
        """Return the number of Sessions."""
        self.response.write(json.dumps(self.app.db.sessions.count()))

    def post(self, iid):
        """Create a new Session"""
        self.response.write('sessions post\n')

    def get(self, iid, xid):
        """Return the list of Experiment Sessions."""
        experiment = self.app.db.experiments.find_one({'_id': bson.objectid.ObjectId(xid)})
        if not experiment:
            self.abort(404)
        if not self.user_is_superuser and self.userid not in experiment['permissions']:
            self.abort(403)
        query = {'experiment': bson.objectid.ObjectId(xid)}
        projection = ['timestamp', 'subject']
        sessions = list(self.app.db.sessions.find(query, projection))
        self.response.write(json.dumps(sessions, default=bson.json_util.default))

    def put(self, iid):
        """Update many Sessions."""
        self.response.write('sessions put\n')


class Session(nimsapiutil.NIMSRequestHandler):

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Session',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'Database ID',
            },
            'experiment': {
                'title': 'Experiment ID',
            },
            'timestamp': {
                'title': 'Timestamp',
            },
            'uid': {
                'title': 'DICOM UID',
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
            'patient_id': {
                'title': 'Patient ID',
                'type': 'string',
            },
            'subject': {
                'title': 'Subject Code',
                'type': 'string',
            },
            'exam': {
                'title': 'Exam Number',
                'type': 'integer',
            },
            'files': {
                'title': 'Files',
                'type': 'array',
                'items': nimsapiutil.NIMSRequestHandler.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'experiment', 'uid', 'patient_id', 'subject'],
    }

    def get(self, iid, sid):
        """Return one Session, conditionally with details."""
        session = self.app.db.sessions.find_one({'_id': bson.objectid.ObjectId(sid)})
        if not session:
            self.abort(404)
        experiment = self.app.db.experiments.find_one({'_id': bson.objectid.ObjectId(session['experiment'])})
        if not experiment:
            self.abort(500)
        if not self.user_is_superuser and self.userid not in experiment['permissions']:
            self.abort(403)
        self.response.write(json.dumps(session, default=bson.json_util.default))

    def put(self, iid, sid):
        """Update an existing Session."""
        self.response.write('session %s put, %s\n' % (sid, self.request.params))

    def delete(self, iid, sid):
        """Delete an Session."""
        self.response.write('session %s delete, %s\n' % (sid, self.request.params))

    def move(self, iid, sid):
        """
        Move a Session to another Experiment.

        Usage:
            /nimsapi/sessions/123/move?dest=456
        """
        self.response.write('session %s move, %s\n' % (sid, self.request.params))
