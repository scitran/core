# @author:  Gunnar Schaefer

import json
import webapp2
import bson.json_util

import nimsdata
import nimsapiutil


class Epochs(nimsapiutil.NIMSRequestHandler):

    """/nimsapi/epochs """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Epoch List',
        'type': 'array',
        'items': {
            'title': 'Epoch',
            'type': 'object',
            'properties': {
                '_id': {
                    'title': 'Database ID',
                },
                'timestamp': {
                    'title': 'Timestamp',
                },
                'datatype': {
                    'title': 'Datatype',
                    'type': 'string',
                },
                'series': {
                    'title': 'Series',
                    'type': 'integer',
                },
                'acquisition': {
                    'title': 'Acquisition',
                    'type': 'integer',
                },
                'description': {
                    'title': 'Description',
                    'type': 'string',
                },
            }
        }
    }

    def count(self):
        """Return the number of Epochs."""
        self.response.write(json.dumps(self.app.db.epochs.count()))

    def post(self):
        """Create a new Epoch."""
        self.response.write('epochs post\n')

    def get(self, sid):
        """Return the list of Session Epochs."""
        session = self.app.db.sessions.find_one({'_id': bson.objectid.ObjectId(sid)})
        if not session:
            self.abort(404)
        experiment = self.app.db.experiments.find_one({'_id': bson.objectid.ObjectId(session['experiment'])})
        if not experiment:
            self.abort(500)
        if not self.user_is_superuser and self.userid not in experiment['permissions']:
            self.abort(403)
        query = {'session': bson.objectid.ObjectId(sid)}
        projection = ['timestamp', 'series', 'acquisition', 'description', 'datatype']
        epochs = list(self.app.db.epochs.find(query, projection))
        self.response.write(json.dumps(epochs, default=bson.json_util.default))

    def put(self):
        """Update many Epochs."""
        self.response.write('epochs put\n')


class Epoch(nimsapiutil.NIMSRequestHandler):

    """/nimsapi/epochs/<eid> """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Epoch',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'Database ID',
            },
            'uid': {
                'title': 'UID',
                'type': 'string',
            },
            'session': {
                'title': 'Session ID',
            },
            'files': {
                'title': 'Files',
                'type': 'array',
                'items': nimsapiutil.NIMSRequestHandler.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id'],
    }

    def schema(self, *args, **kwargs):
        import copy
        json_schema = copy.deepcopy(self.json_schema)
        json_schema['properties'].update(nimsdata.nimsdicom.NIMSDicom.epoch_properties)
        self.response.write(json.dumps(json_schema, default=bson.json_util.default))

    def get(self, eid):
        """Return one Epoch, conditionally with details."""
        epoch = self.app.db.epochs.find_one({'_id': bson.objectid.ObjectId(eid)})
        if not epoch:
            self.abort(404)
        session = self.app.db.sessions.find_one({'_id': epoch['session']})
        if not session:
            self.abort(500)
        experiment = self.app.db.experiments.find_one({'_id': bson.objectid.ObjectId(session['experiment'])})
        if not experiment:
            self.abort(500)
        if not self.user_is_superuser and self.userid not in experiment['permissions']:
            self.abort(403)
        self.response.write(json.dumps(epoch, default=bson.json_util.default))

    def put(self, eid):
        """Update an existing Epoch."""
        self.response.write('epoch %s put, %s\n' % (epoch_id, self.request.params))

    def delete(self, eid):
        """Delete an Epoch."""
        self.response.write('epoch %s delete, %s\n' % (epoch_id, self.request.params))
