# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('nimsapi')

import bson.json_util

import nimsdata

import base


class Experiments(base.RequestHandler):

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
        if self.request.method == 'OPTIONS':
            return self.options()
        self.response.write(self.app.db.experiments.count())

    def post(self):
        """Create a new Experiment."""
        self.response.write('experiments post\n')

    def get(self):
        """Return the list of Experiments."""
        query = None
        if not self.user_is_superuser:
            if self.request.get('admin').lower() in ('1', 'true'):
                query = {'permissions': {'$elemMatch': {'uid': self.uid, 'role': 'admin', 'site': self.source_site}}}
            else:
                query = {'permissions': {'$elemMatch': {'uid': self.uid, 'site': self.source_site}}}
        projection = {
                'group': 1, 'group_name': 1, 'name': 1, 'timestamp': 1, 'notes': 1,
                'permissions': {'$elemMatch': {'uid': self.uid, 'site': self.source_site}},
                }
        experiments = list(self.app.db.experiments.find(query, projection))
        for exp in experiments:
            exp['site'] = self.app.config['site_id']
            exp['site_name'] = self.app.config['site_name']
        if self.debug:
            for exp in experiments:
                exp['metadata'] = self.uri_for('experiment', _id=str(exp['_id']), _full=True) + '?' + self.request.query_string
                exp['sessions'] = self.uri_for('sessions', _id=str(exp['_id']), _full=True) + '?' + self.request.query_string
        return experiments

    def put(self):
        """Update many Experiments."""
        self.response.write('experiments put\n')


class Experiment(base.RequestHandler):

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
                'items': base.RequestHandler.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'group', 'name'], #FIXME
    }

    def get(self, _id):
        """Return one Experiment, conditionally with details."""
        xid = bson.ObjectId(_id)
        exp = self.get_experiment(xid)
        if self.debug:
            exp['sessions'] = self.uri_for('sessions', _id=_id, _full=True) + '?' + self.request.query_string
        return exp

    def put(self, _id):
        """Update an existing Experiment."""
        xid = bson.ObjectId(_id)
        self.get_experiment(xid, 'read-write') # ensure permissions
        updates = {'$set': {}, '$unset': {}}
        for k, v in self.request.params.iteritems():
            if k in ['notes']:
                updates['$set'][k] = v # FIXME: do appropriate type conversion
        self.app.db.experiments.update({'_id': xid}, updates)

    def delete(self, xid):
        """Delete an Experiment."""
        self.abort(501)


class Sessions(base.RequestHandler):

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
        if self.request.method == 'OPTIONS':
            return self.options()
        self.response.write(self.app.db.sessions.count())

    def post(self):
        """Create a new Session"""
        self.response.write('sessions post\n')

    def get(self, _id):
        """Return the list of Experiment Sessions."""
        xid = bson.ObjectId(_id)
        self.get_experiment(xid) # ensure permissions
        query = {'experiment': xid}
        projection = ['name', 'subject', 'notes']
        sessions =  list(self.app.db.sessions.find(query, projection))
        if self.debug:
            for sess in sessions:
                sess['metadata'] = self.uri_for('session', _id=str(sess['_id']), _full=True) + '?' + self.request.query_string
                sess['epochs'] = self.uri_for('epochs', _id=str(sess['_id']), _full=True) + '?' + self.request.query_string
        return sessions

    def put(self):
        """Update many Sessions."""
        self.response.write('sessions put\n')


class Session(base.RequestHandler):

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
                'items': base.RequestHandler.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'experiment', 'uid', 'patient_id', 'subject'], #FIXME
    }

    def schema(self, *args, **kwargs):
        if self.request.method == 'OPTIONS':
            return self.options()
        import copy
        json_schema = copy.deepcopy(self.json_schema)
        json_schema['properties'].update(nimsdata.NIMSData.session_properties)
        return json_schema

    def get(self, _id):
        """Return one Session, conditionally with details."""
        sid = bson.ObjectId(_id)
        sess = self.get_session(sid)
        if self.debug:
            sess['epochs'] = self.uri_for('epochs', _id=_id, _full=True) + '?' + self.request.query_string
        return sess

    def put(self, _id):
        """Update an existing Session."""
        sid = bson.ObjectId(_id)
        self.get_session(sid, 'read-write') # ensure permissions
        updates = {'$set': {}, '$unset': {}}
        for k, v in self.request.params.iteritems():
            if k in ['notes']:
                updates['$set'][k] = v # FIXME: do appropriate type conversion
        self.app.db.sessions.update({'_id': sid}, updates)

    def delete(self, _id):
        """Delete a Session."""
        self.abort(501)


class Epochs(base.RequestHandler):

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
        if self.request.method == 'OPTIONS':
            return self.options()
        self.response.write(self.app.db.epochs.count())

    def post(self):
        """Create a new Epoch."""
        self.response.write('epochs post\n')

    def get(self, _id):
        """Return the list of Session Epochs."""
        sid = bson.ObjectId(_id)
        self.get_session(sid) # ensure permissions
        query = {'session': sid}
        projection = ['name', 'description', 'datatype', 'notes']
        epochs = list(self.app.db.epochs.find(query, projection))
        if self.debug:
            for epoch in epochs:
                epoch['metadata'] = self.uri_for('epoch', _id=str(epoch['_id']), _full=True) + '?' + self.request.query_string
        return epochs

    def put(self):
        """Update many Epochs."""
        self.response.write('epochs put\n')


class Epoch(base.RequestHandler):

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
                'items': base.RequestHandler.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id'], #FIXME
    }

    def schema(self, *args, **kwargs):
        if self.request.method == 'OPTIONS':
            return self.options()
        import copy
        json_schema = copy.deepcopy(self.json_schema)
        json_schema['properties'].update(nimsdata.nimsdicom.NIMSDicom.epoch_properties)
        return json_schema

    def get(self, _id):
        """Return one Epoch, conditionally with details."""
        eid = bson.ObjectId(_id)
        return self.get_epoch(eid)

    def put(self, _id):
        """Update an existing Epoch."""
        eid = bson.ObjectId(_id)
        self.get_epoch(eid, 'read-write') # ensure permissions
        updates = {'$set': {}, '$unset': {}}
        for k, v in self.request.params.iteritems():
            if k in ['notes']:
                updates['$set'][k] = v # FIXME: do appropriate type conversion
        self.app.db.epochs.update({'_id': eid}, updates)

    def delete(self, _id):
        """Delete an Epoch."""
        self.abort(501)
