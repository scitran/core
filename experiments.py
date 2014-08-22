# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('nimsapi')

import bson.json_util

import nimsdata
import nimsdata.medimg.medimg

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
                    'type': 'string',
                },
                'site_name': {
                    'title': 'Site',
                    'type': 'string',
                },
                'gid': {
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
                    'type': 'string',
                    'format': 'date-time',
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
                'gid': 1, 'group': 1, 'name': 1, 'notes': 1,
                'permissions': {'$elemMatch': {'uid': self.uid, 'site': self.source_site}},
                }
        experiments = list(self.app.db.experiments.find(query, projection))
        for exp in experiments:
            exp['site'] = self.app.config['site_id']
            exp['site_name'] = self.app.config['site_name']
        if self.debug:
            for exp in experiments:
                xid = str(exp['_id'])
                exp['metadata'] = self.uri_for('experiment', xid=xid, _full=True) + '?' + self.request.query_string
                exp['sessions'] = self.uri_for('sessions', xid=xid, _full=True) + '?' + self.request.query_string
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
                'items': base.RequestHandler.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'group', 'name'], #FIXME
    }

    def schema(self, *args, **kwargs):
        return super(Experiment, self).schema(nimsdata.medimg.medimg.MedImgReader.experiment_properties)

    def get(self, xid):
        """Return one Experiment, conditionally with details."""
        _id = bson.ObjectId(xid)
        exp = self.get_experiment(_id)
        exp['site'] = self.app.config['site_id']
        exp['site_name'] = self.app.config['site_name']
        if self.debug:
            exp['sessions'] = self.uri_for('sessions', xid=xid, _full=True) + '?' + self.request.query_string
        return exp

    def put(self, xid):
        """Update an existing Experiment."""
        _id = bson.ObjectId(xid)
        self.get_experiment(_id, 'read-write') # ensure permissions
        updates = {'$set': {'_id': _id}, '$unset': {'__null__': ''}}
        for k, v in self.request.params.iteritems():
            if k in ['notes']:
                if v:
                    updates['$set'][k] = v # FIXME: do appropriate type conversion
                else:
                    updates['$unset'][k] = None
        self.app.db.experiments.update({'_id': _id}, updates)

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

    def get(self, xid):
        """Return the list of Experiment Sessions."""
        _id = bson.ObjectId(xid)
        self.get_experiment(_id) # ensure permissions
        query = {'experiment': _id}
        projection = ['label', 'subject.code', 'notes']
        sessions =  list(self.app.db.sessions.find(query, projection))
        if self.debug:
            for sess in sessions:
                sid = str(sess['_id'])
                sess['metadata'] = self.uri_for('session', sid=sid, _full=True) + '?' + self.request.query_string
                sess['epochs'] = self.uri_for('epochs', sid=sid, _full=True) + '?' + self.request.query_string
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
            },
            'experiment': {
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
        return super(Session, self).schema(nimsdata.medimg.medimg.MedImgReader.session_properties)

    def get(self, sid):
        """Return one Session, conditionally with details."""
        _id = bson.ObjectId(sid)
        sess = self.get_session(_id)
        if self.debug:
            sess['epochs'] = self.uri_for('epochs', sid=sid, _full=True) + '?' + self.request.query_string
        return sess

    def put(self, sid):
        """Update an existing Session."""
        _id = bson.ObjectId(sid)
        self.get_session(_id, 'read-write') # ensure permissions
        updates = {'$set': {'_id': _id}, '$unset': {'__null__': ''}}
        for k, v in self.request.params.iteritems():
            if k in ['notes']:
                if v:
                    updates['$set'][k] = v # FIXME: do appropriate type conversion
                else:
                    updates['$unset'][k] = None
        self.app.db.sessions.update({'_id': _id}, updates)

    def delete(self, sid):
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

    def get(self, sid):
        """Return the list of Session Epochs."""
        _id = bson.ObjectId(sid)
        self.get_session(_id) # ensure permissions
        query = {'session': _id}
        projection = ['label', 'description', 'datatype', 'notes']
        epochs = list(self.app.db.epochs.find(query, projection))
        if self.debug:
            for epoch in epochs:
                eid = str(epoch['_id'])
                epoch['metadata'] = self.uri_for('epoch', eid=eid, _full=True) + '?' + self.request.query_string
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
            },
            'session': {
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
        return super(Epoch, self).schema(nimsdata.medimg.medimg.MedImgReader.epoch_properties)

    def get(self, eid):
        """Return one Epoch, conditionally with details."""
        _id = bson.ObjectId(eid)
        return self.get_epoch(_id)

    def put(self, eid):
        """Update an existing Epoch."""
        _id = bson.ObjectId(eid)
        self.get_epoch(_id, 'read-write') # ensure permissions
        updates = {'$set': {'_id': _id}, '$unset': {'__null__': ''}}
        for k, v in self.request.params.iteritems():
            if k in ['notes']:
                if v:
                    updates['$set'][k] = v # FIXME: do appropriate type conversion
                else:
                    updates['$unset'][k] = None
        self.app.db.epochs.update({'_id': _id}, updates)

    def delete(self, eid):
        """Delete an Epoch."""
        self.abort(501)
