# @author:  Gunnar Schaefer

import json
import webapp2
import bson.json_util

import nimsapiutil


class Experiments(nimsapiutil.NIMSRequestHandler):

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
                'firstname': {
                    'title': 'First Name',
                    'type': 'string',
                },
                'lastname': {
                    'title': 'Last Name',
                    'type': 'string',
                },
                'email_hash': {
                    'type': 'string',
                },
            }
        }
    }

    def count(self, iid):
        """Return the number of Experiments."""
        self.response.write(json.dumps(self.app.db.experiments.count()))

    def post(self, iid):
        """Create a new Experiment."""
        self.response.write('experiments post\n')

    def get(self, iid):
        """Return the list of Experiments."""
        query = {'permissions.' + self.userid: {'$exists': 'true'}} if not self.user_is_superuser else None
        projection = ['group', 'name', 'permissions.'+self.userid]
        experiments = list(self.app.db.experiments.find(query, projection))
        session_aggregates = self.app.db.sessions.aggregate([
                {'$match': {'experiment': {'$in': [exp['_id'] for exp in experiments]}}},
                {'$group': {'_id': '$experiment', 'timestamp': {'$max': '$timestamp'}}},
                ])['result']
        timestamps = {sa['_id']: sa['timestamp'] for sa in session_aggregates}
        for exp in experiments:
            exp['timestamp'] = timestamps[exp['_id']]
        self.response.write(json.dumps(experiments, default=bson.json_util.default))

    def put(self, iid):
        """Update many Experiments."""
        self.response.write('experiments put\n')


class Experiment(nimsapiutil.NIMSRequestHandler):

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Experiment',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'Database ID',
            },
            'timestamp': {
                'title': 'Timestamp',
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
        'required': ['_id', 'group', 'name'],
    }

    def get(self, iid, xid):
        """Return one Experiment, conditionally with details."""
        experiment = self.app.db.experiments.find_one({'_id': bson.objectid.ObjectId(xid)})
        if not experiment:
            self.abort(404)
        experiment['timestamp'] = self.app.db.sessions.aggregate([
                {'$match': {'experiment': bson.objectid.ObjectId(xid)}},
                {'$group': {'_id': '$experiment', 'timestamp': {'$max': '$timestamp'}}},
                ])['result'][0]['timestamp']
        if not self.user_is_superuser:
            if self.userid not in experiment['permissions']:
                self.abort(403)
            if experiment['permissions'][self.userid] != 'admin' and experiment['permissions'][self.userid] != 'pi':
                experiment['permissions'] = {self.userid: experiment['permissions'][self.userid]}
        self.response.write(json.dumps(experiment, default=bson.json_util.default))

    def put(self, iid, xid):
        """Update an existing Experiment."""
        self.response.write('experiment %s put, %s\n' % (exp_id, self.request.params))

    def delete(self, iid, xid):
        """Delete an Experiment."""
        self.response.write('experiment %s delete, %s\n' % (exp_id, self.request.params))
