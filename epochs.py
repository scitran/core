# @author:  Gunnar Schaefer

import json
import webapp2
import bson.json_util

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
            'session': {
                'title': 'Session ID',
            },
            'timestamp': {
                'title': 'Timestamp',
            },
            'session_uid': {
                'title': 'DICOM UID',
                'type': 'string',
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
                'maxLength': 64,
            },
            'protocol': {
                'title': 'Protocol',
                'type': 'string',
                'maxLength': 64,
            },
            'rx_coil': {
                'title': 'Coil',
                'type': 'string',
                'maxLength': 64,
            },
            'device': {
                'title': 'Device',
                'type': 'string',
                'maxLength': 64,
            },
            'size': {
                'title': 'Size',
                'type': 'array',
                'items': {
                    'type': 'integer',
                }
            },
            'acquisition_matrix': {
                'title': 'Acquisition Matrix',
                'type': 'array',
                'items': {
                    'type': 'number',
                }
            },
            'fov': {
                'title': 'Field of View',
                'type': 'array',
                'items': {
                    'type': 'number',
                }
            },
            'mm_per_voxel': {
                'title': 'mm per Voxel',
                'type': 'array',
                'items': {
                    'type': 'number',
                }
            },
            'flip_angle': {
                'title': 'Flip Angle',
                'type': 'integer',
            },
            'num_averages': {
                'title': 'Averages',
                'type': 'integer',
            },
            'num_bands': {
                'title': 'Bands',
                'type': 'integer',
            },
            'num_echos': {
                'title': 'Echos',
                'type': 'integer',
            },
            'num_slices': {
                'title': 'Slices',
                'type': 'integer',
            },
            'num_timepoints': {
                'title': 'Time Points',
                'type': 'integer',
            },
            'pixel_bandwidth': {
                'title': 'Pixel Bandwidth',
                'type': 'number',
            },
            'prescribed_duration': {
                'title': 'Prescribed Duration',
                'type': 'number',
            },
            'duration': {
                'title': 'Duration',
                'type': 'number',
            },
            'slice_encode_undersample': {
                'title': 'Slice Encode Undersample',
                'type': 'integer',
            },
            'te': {
                'title': 'Te',
                'type': 'number',
            },
            'tr': {
                'title': 'Tr',
                'type': 'number',
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
