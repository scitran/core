# @author:  Gunnar Schaefer

import json
import webapp2
import bson.json_util

import nimsapiutil


class Experiments(nimsapiutil.NIMSRequestHandler):

    def count(self):
        """Return the number of Experiments."""
        self.response.write('%d experiments\n' % self.app.db.experiments.count())

    def post(self):
        """Create a new Experiment"""
        self.response.write('experiments post\n')

    def get(self):
        """Return the list of Experiments."""
        query = {'permissions.' + self.userid: {'$exists': 'true'}} if not self.user_is_superuser else None
        projection = ['timestamp', 'group', 'name', 'permissions.'+self.userid]
        experiments = list(self.app.db.experiments.find(query, projection))
        self.response.write(json.dumps(experiments, default=bson.json_util.default))

    def put(self):
        """Update many Experiments."""
        self.response.write('experiments put\n')


class Experiment(nimsapiutil.NIMSRequestHandler):

    def get(self, exp_id):
        """Return one Experiment, conditionally with details."""
        experiment = self.app.db.experiments.find_one({'_id': bson.objectid.ObjectId(exp_id)})
        if not experiment:
            self.abort(404)
        if not self.user_is_superuser:
            if self.userid not in experiment['permissions']:
                self.abort(403)
            if experiment['permissions'][self.userid] != 'admin' and experiment['permissions'][self.userid] != 'pi':
                experiment['permissions'] = {self.userid: experiment['permissions'][self.userid]}
        self.response.write(json.dumps(experiment, default=bson.json_util.default))

    def put(self, exp_id):
        """Update an existing Experiment."""
        self.response.write('experiment %s put, %s\n' % (exp_id, self.request.params))

    def delete(self, exp_id):
        """Delete an Experiment."""
        self.response.write('experiment %s delete, %s\n' % (exp_id, self.request.params))
