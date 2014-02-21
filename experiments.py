# @author:  Gunnar Schaefer

import json
import webapp2
import bson.json_util

import logging
log = logging.getLogger('nimsapi')

import nimsdata
import nimsapiutil


class Experiments(nimsapiutil.NIMSRequestHandler):

    """/experiments """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Experiment List',
        'type': 'array',
        'items': {
            'title': 'Experiment',
            'type': 'object',
            'properties': {
                '_id': {
                    'title': 'Database ID',
                },
                'site': {
                    'title': 'Site',
                    'type': 'string',
                },
                'group': {
                    'title': 'Group',
                    'type': 'string',
                },
                'name': {
                    'title': 'Name',
                    'type': 'string',
                },
                'timestamp': {
                    'title': 'Timestamp',
                },
                'permissions': {
                    'title': 'Permissions',
                    'type': 'object',
                },
            }
        }
    }

    def count(self):
        """Return the number of Experiments."""
        self.response.write(json.dumps(self.app.db.experiments.count()))

    def post(self):
        """Create a new Experiment."""
        self.response.write('experiments post\n')

    def get(self):
        """Return the list of Experiments."""
        query = {'permissions.' + self.userid: {'$exists': 'true'}} if not self.user_is_superuser else None
        projection = ['group', 'name', 'timestamp', 'permissions.'+self.userid, 'notes']
        experiments = list(self.app.db.experiments.find(query, projection))
        for exp in experiments:
            exp['site'] = self.app.config['site_id']
        self.response.write(json.dumps(experiments, default=bson.json_util.default))

    def put(self):
        """Update many Experiments."""
        self.response.write('experiments put\n')


class Experiment(nimsapiutil.NIMSRequestHandler):

    """/experiments/<xid> """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Experiment',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'Database ID',
            },
            'site': {
                'title': 'Site',
                'type': 'string',
            },
            'group': {
                'title': 'Group',
                'type': 'string',
            },
            'name': {
                'title': 'Name',
                'type': 'string',
                'maxLength': 32,
            },
            'timestamp': {
                'title': 'Timestamp',
            },
            'permissions': {
                'title': 'Permissions',
                'type': 'object',
                'minProperties': 1,
            },
            'files': {
                'title': 'Files',
                'type': 'array',
                'items': nimsapiutil.NIMSRequestHandler.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'group', 'name'], #FIXME
    }

    def get(self, xid):
        """Return one Experiment, conditionally with details."""
        experiment = self.app.db.experiments.find_one({'_id': bson.ObjectId(xid)})
        if not experiment:
            self.abort(404)
        if not self.user_is_superuser:
            if self.userid not in experiment['permissions']:
                self.abort(403)
            if experiment['permissions'][self.userid] != 'admin' and experiment['permissions'][self.userid] != 'pi':
                experiment['permissions'] = {self.userid: experiment['permissions'][self.userid]}
        self.response.write(json.dumps(experiment, default=bson.json_util.default))

    def put(self, xid):
        """Update an existing Experiment."""
        self.response.write('experiment %s put, %s\n' % (exp_id, self.request.params))

    def delete(self, xid):
        """Delete an Experiment."""
        self.abort(501)


class Sessions(nimsapiutil.NIMSRequestHandler):

    """/sessions """

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
                'name': {
                    'title': 'Session',
                    'type': 'string',
                },
                'subject': {
                    'title': 'Subject',
                    'type': 'string',
                },
                'site': {
                    'title': 'Site',
                    'type': 'string',
                },
            }
        }
    }

    def count(self):
        """Return the number of Sessions."""
        self.response.write(json.dumps(self.app.db.sessions.count()))

    def post(self):
        """Create a new Session"""
        self.response.write('sessions post\n')

    def get(self, xid):
        """Return the list of Experiment Sessions."""
        experiment = self.app.db.experiments.find_one({'_id': bson.ObjectId(xid)})
        if not experiment:
            self.abort(404)
        if not self.user_is_superuser and self.userid not in experiment['permissions']:
            self.abort(403)
        query = {'experiment': bson.ObjectId(xid)}
        projection = ['name', 'subject', 'notes']
        sessions = list(self.app.db.sessions.find(query, projection))
        self.response.write(json.dumps(sessions, default=bson.json_util.default))

    def put(self):
        """Update many Sessions."""
        self.response.write('sessions put\n')


class Session(nimsapiutil.NIMSRequestHandler):

    """/sessions/<sid> """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Session',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'Database ID',
            },
            'uid': {
                'title': 'UID',
                'type': 'string',
            },
            'experiment': {
                'title': 'Experiment ID',
            },
            'site': {
                'title': 'Site',
                'type': 'string',
            },
            'files': {
                'title': 'Files',
                'type': 'array',
                'items': nimsapiutil.NIMSRequestHandler.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'experiment', 'uid', 'patient_id', 'subject'], #FIXME
    }

    def schema(self, *args, **kwargs):
        import copy
        json_schema = copy.deepcopy(self.json_schema)
        json_schema['properties'].update(nimsdata.NIMSData.session_properties)
        self.response.write(json.dumps(json_schema, default=bson.json_util.default))

    def get(self, sid):
        """Return one Session, conditionally with details."""
        session = self.app.db.sessions.find_one({'_id': bson.ObjectId(sid)})
        if not session:
            self.abort(404)
        experiment = self.app.db.experiments.find_one({'_id': session['experiment']})
        if not experiment:
            self.abort(500)
        if not self.user_is_superuser and self.userid not in experiment['permissions']:
            self.abort(403)
        self.response.write(json.dumps(session, default=bson.json_util.default))

    def put(self, sid):
        """Update an existing Session."""
        session = self.app.db.sessions.find_one({'_id': bson.ObjectId(sid)})
        if not session:
            self.abort(404)
        experiment = self.app.db.experiments.find_one({'_id': session['experiment']})
        if not experiment:
            self.abort(500)
        if not self.user_is_superuser and self.userid not in experiment['permissions']:
            self.abort(403)
        updates = {'$set': {}, '$unset': {}}
        for k, v in self.request.params.iteritems():
            if k in ['notes']:
                updates['$set'][k] = v # FIXME: do appropriate type conversion
        self.app.db.sessions.update({'_id': bson.ObjectId(sid)}, updates)

    def delete(self, sid):
        """Delete a Session."""
        self.abort(501)

    def move(self, sid):
        """
        Move a Session to another Experiment.

        Usage:
            /nimsapi/sessions/123/move?dest=456
        """
        self.response.write('session %s move, %s\n' % (sid, self.request.params))


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
                'name': {
                    'title': 'Epoch',
                    'type': 'string',
                },
                'description': {
                    'title': 'Description',
                    'type': 'string',
                },
                'datatype': {
                    'title': 'Datatype',
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
        session = self.app.db.sessions.find_one({'_id': bson.ObjectId(sid)})
        if not session:
            self.abort(404)
        experiment = self.app.db.experiments.find_one({'_id': session['experiment']})
        if not experiment:
            self.abort(500)
        if not self.user_is_superuser and self.userid not in experiment['permissions']:
            self.abort(403)
        query = {'session': bson.ObjectId(sid)}
        projection = ['name', 'description', 'datatype', 'notes']
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
        'required': ['_id'], #FIXME
    }

    def schema(self, *args, **kwargs):
        import copy
        json_schema = copy.deepcopy(self.json_schema)
        json_schema['properties'].update(nimsdata.nimsdicom.NIMSDicom.epoch_properties)
        self.response.write(json.dumps(json_schema, default=bson.json_util.default))

    def get(self, eid):
        """Return one Epoch, conditionally with details."""
        if not self.valid_parameters():
            self.abort(400, 'invalid parameters')
        epoch = self.app.db.epochs.find_one({'_id': bson.ObjectId(eid)})
        if not epoch:
            self.abort(404)
        if not self.user_access_epoch(epoch):
            self.abort(403)
        self.response.write(json.dumps(epoch, default=bson.json_util.default))

    def put(self, eid):
        """Update an existing Epoch."""
        self.response.write('epoch %s put, %s\n' % (epoch_id, self.request.params))

    def delete(self, eid):
        """Delete an Epoch."""
        self.abort(501)
