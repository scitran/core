# @author:  Gunnar Schaefer

import json
import webapp2
import bson.json_util

class Experiments(webapp2.RequestHandler):

    def count(self):
        """Return the number of Experiments."""
        self.response.write('%d experiments\n' % self.app.db.experiments.count())

    def post(self):
        """Create a new Experiment"""
        self.response.write('experiments post\n')

    def get(self):
        """Return the list of Experiments."""
        self.request.remote_user = self.request.get('user', None) # FIXME: auth system should set REMOTE_USER
        user = self.request.remote_user or '@public'
        query = {'permissions.' + user: {'$exists': 'true'}}
        projection = {'owner': 1, 'name': 1, 'permissions.' + user: 1, 'timestamp': 1}
        experiments = list(self.app.db.experiments.find(query, projection))
        self.response.headers['Content-Type'] = 'application/json'
        self.response.write(json.dumps(experiments, default=bson.json_util.default))

    def put(self):
        """Update many Experiments."""
        self.response.write('experiments put\n')


class Experiment(webapp2.RequestHandler):

    def get(self, _id):
        """Return one Experiment, conditionally with details."""
        self.request.remote_user = self.request.get('user', None) # FIXME: auth system should set REMOTE_USER
        user = self.request.remote_user or '@public'
        query = {'_id': bson.objectid.ObjectId(_id), 'permissions.' + user: {'$exists': 'true'}}
        experiment = self.app.db.experiments.find_one({'_id': bson.objectid.ObjectId(_id)})
        if not experiment:
            self.abort(404)
        if user not in experiment['permissions']:
            self.abort(403)
        if experiment['permissions'][user] != 'admin' and experiment['permissions'][user] != 'pi':
            experiment['permissions'] = {user: experiment['permissions'][user]}
        self.response.headers['Content-Type'] = 'application/json'
        self.response.write(json.dumps(experiment, default=bson.json_util.default))

    def put(self, _id):
        """Update an existing Experiment."""
        self.response.write('experiment %s put, %s\n' % (_id, self.request.params))

    def delete(self, _id):
        """Delete an Experiment."""
        self.response.write('experiment %s delete, %s\n' % (_id, self.request.params))
