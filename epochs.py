# @author:  Gunnar Schaefer

import json
import webapp2
import bson.json_util


class Epochs(webapp2.RequestHandler):

    def count(self):
        """Return the number of Epochs."""
        self.response.write('epochs count\n')

    def post(self):
        """Create a new Epoch"""
        self.response.write('epochs post\n')

    def get(self, sess_id):
        """Return the list of Session Epochs."""
        self.request.remote_user = self.request.get('user', None) # FIXME: auth system should set REMOTE_USER
        user = self.request.remote_user or '@public'
        session = self.app.db.sessions.find_one({'_id': sess_id})
        if not session:
            self.abort(404)
        experiment = self.app.db.experiments.find_one({'_id': bson.objectid.ObjectId(session['experiment'])})
        if not experiment:
            self.abort(500)
        if user not in experiment['permissions']:
            self.abort(403)
        epochs = list(self.app.db.epochs.find({'session': sess_id}, {'files': 0}))
        self.response.headers['Content-Type'] = 'application/json'
        self.response.write(json.dumps(epochs, default=bson.json_util.default))

    def put(self):
        """Update many Epochs."""
        self.response.write('epochs put\n')


class Epoch(webapp2.RequestHandler):

    def get(self, epoch_id):
        """Return one Epoch, conditionally with details."""
        self.request.remote_user = self.request.get('user', None) # FIXME: auth system should set REMOTE_USER
        user = self.request.remote_user or '@public'
        epoch = self.app.db.epochs.find_one({'_id': bson.objectid.ObjectId(epoch_id)})
        if not epoch:
            self.abort(404)
        session = self.app.db.sessions.find_one({'_id': epoch['session']})
        if not session:
            self.abort(500)
        experiment = self.app.db.experiments.find_one({'_id': bson.objectid.ObjectId(session['experiment'])})
        if not experiment:
            self.abort(500)
        if user not in experiment['permissions']:
            self.abort(403)
        self.response.headers['Content-Type'] = 'application/json'
        self.response.write(json.dumps(epoch, default=bson.json_util.default))

    def put(self, _id):
        """Update an existing Epoch."""
        self.response.write('epoch %s put, %s\n' % (_id, self.request.params))

    def delete(self, _id):
        """Delete an Epoch."""
        self.response.write('epoch %s delete, %s\n' % (_id, self.request.params))
