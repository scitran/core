#!/usr/bin/env python
#
# @author:  Gunnar Schaefer

import os
import json
import uuid
import hashlib
import logging
import pymongo
import tarfile
import webapp2
import requests
import zipfile
import argparse
import bson.json_util
import webapp2_extras.routes

import nimsutil

import epochs
import sessions
import experiments
import nimsapiutil

log = logging.getLogger('nimsapi')


class NIMSAPI(nimsapiutil.NIMSRequestHandler):

    def head(self):
        """Return 200 OK."""
        self.response.set_status(200)

    def get(self):
        """Return API documentation"""
        self.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        self.response.write('nimsapi - {0}\n'.format(self.app.config['site_id']))

    def upload(self):
        # TODO add security: either authenticated user or machine-to-machine CRAM
        if 'Content-MD5' not in self.request.headers:
            self.abort(400, 'Request must contain a valid "Content-MD5" header.')
        filename = self.request.get('filename', 'anonymous')
        stage_path = self.app.config['stage_path']
        with nimsutil.TempDir(prefix='.tmp', dir=stage_path) as tempdir_path:
            hash_ = hashlib.md5()
            upload_filepath = os.path.join(tempdir_path, filename)
            log.info(os.path.basename(upload_filepath))
            with open(upload_filepath, 'wb') as upload_file:
                for chunk in iter(lambda: self.request.body_file.read(2**20), ''):
                    hash_.update(chunk)
                    upload_file.write(chunk)
            if hash_.hexdigest() != self.request.headers['Content-MD5']:
                self.abort(400, 'Content-MD5 mismatch.')
            if not tarfile.is_tarfile(upload_filepath) and not zipfile.is_zipfile(upload_filepath):
                self.abort(415)
            os.rename(upload_filepath, os.path.join(stage_path, str(uuid.uuid1()) + '_' + fid)) # add UUID to prevent clobbering files

    def download(self):
        paths = []
        symlinks = []
        for js_id in self.request.get('id', allow_multiple=True):
            type_, _id = js_id.split('_')
            _idpaths, _idsymlinks = resource_types[type_].download_info(_id)
            paths += _idpaths
            symlinks += _idsymlinks

    def dump(self):
        self.response.write(json.dumps(list(self.app.db.sessions.find()), default=bson.json_util.default))


class Users(nimsapiutil.NIMSRequestHandler):

    def count(self, iid):
        """Return the number of Users."""
        self.response.write('%d users\n' % self.app.db.users.count())

    def post(self, iid):
        """Create a new User"""
        self.response.write('users post\n')

    def get(self, iid):
        """Return the list of Users."""
        projection = ['firstname', 'lastname', 'email_hash']
        users = list(self.app.db.users.find({}, projection))
        self.response.write(json.dumps(users, default=bson.json_util.default))

    def put(self, iid):
        """Update many Users."""
        self.response.write('users put\n')


class User(nimsapiutil.NIMSRequestHandler):

    def get(self, iid, uid):
        """Return User details."""
        user = self.app.db.users.find_one({'_id': uid})
        self.response.write(json.dumps(user, default=bson.json_util.default))

    def put(self, iid, uid):
        """Update an existing User."""
        user = self.app.db.users.find_one({'_id': uid})
        if not user:
            self.abort(404)
        if uid == self.userid or self.user_is_superuser: # users can only update their own info
            updates = {'$set': {}, '$unset': {}}
            for k, v in self.request.params.iteritems():
                if k != 'superuser' and k in []:#user_fields:
                    updates['$set'][k] = v # FIXME: do appropriate type conversion
                elif k == 'superuser' and uid == self.userid and self.user_is_superuser is not None: # toggle superuser for requesting user
                    updates['$set'][k] = v.lower() in ('1', 'true')
                elif k == 'superuser' and uid != self.userid and self.user_is_superuser:             # enable/disable superuser for other user
                    if v.lower() in ('1', 'true') and user.get('superuser') is None:
                        updates['$set'][k] = False # superuser is tri-state: False indicates granted, but disabled, superuser privileges
                    elif v.lower() not in ('1', 'true'):
                        updates['$unset'][k] = ''
            user = self.app.db.users.find_and_modify({'_id': uid}, updates, new=True)
        else:
            self.abort(403)
        self.response.write(json.dumps(user, default=bson.json_util.default) + '\n')

    def delete(self, iid, uid):
        """Delete an User."""
        self.response.write('user %s delete, %s\n' % (uid, self.request.params))


class Groups(nimsapiutil.NIMSRequestHandler):

    def count(self, iid):
        """Return the number of Groups."""
        self.response.write('%d groups\n' % self.app.db.groups.count())

    def post(self, iid):
        """Create a new Group"""
        self.response.write('groups post\n')

    def get(self, iid):
        """Return the list of Groups."""
        projection = ['_id']
        groups = list(self.app.db.groups.find({}, projection))
        self.response.write(json.dumps(groups, default=bson.json_util.default))

    def put(self, iid):
        """Update many Groups."""
        self.response.write('groups put\n')


class Group(nimsapiutil.NIMSRequestHandler):

    def get(self, iid, gid):
        """Return Group details."""
        group = self.app.db.groups.find_one({'_id': gid})
        self.response.write(json.dumps(group, default=bson.json_util.default))

    def put(self, iid, gid):
        """Update an existing Group."""
        self.response.write('group %s put, %s\n' % (gid, self.request.params))

    def delete(self, iid, gid):
        """Delete an Group."""


class Remotes(nimsapiutil.NIMSRequestHandler):

    def get(self):
        """Return Remote NIMS sites"""
        logging.info(self.user)
        # TODO: remotes by default will show all registered remote site
        if self.request.get('all'):
            projection = ['_id', 'hostname', 'ip4']
            sites = list(self.app.db.remotes.find({}, projection))
            self.response.write(json.dumps(sites, default=bson.json_util.default))
        # if 'all' not specificed, then user MUST be speficied
        else:
            """Return the list of remotes where user has membership"""
            logging.info(self.user['_id'])
            # projection = ['_id', 'hostname', 'ip4']
            projection = ['_id', 'hostname', 'ip4', 'users']

            remotes = list(self.app.db.remotes.find({'users': {'$in': [self.user['_id']]}}, projection))
            self.response.write(json.dumps(remotes, default=bson.json_util.default))

            # TODO: IMPORTANT; refine how remotes are returned from pymongo queries

            # in the list of remotes; in which sites, are there experiments, which the person has access to?
            # return ONLY remote site names.

            # if not superuser, does person have permissions to what is being queried
            # query = {'permissions.' + self.userid: {'$in': 'true'}} if not self.user_is_superuser else None
            # projection = ['_id', 'users', 'hostname', 'ip4']
            # remotes = list(self.app.db.remotes.find(query, projection))

            # session_aggregates = self.app.db.sessions.aggregate([
            # #         {'$match': {'experiment': {'$in': [exp['_id'] for exp in experiments]}}},
            #         {'$group': {'_id': '$experiment', 'timestamp': {'$max': '$timestamp'}}},
            #         ])['result']
            # timestamps = {sa['_id']: sa['timestamp'] for sa in session_aggregates}
            # for exp in experiments:
            #     exp['timestamp'] = timestamps[exp['_id']]
            # self.response.write(json.dumps(experiments, default=bson.json_util.default))


class ArgumentParser(argparse.ArgumentParser):

    def __init__(self):
        super(ArgumentParser, self).__init__()
        self.add_argument('uri', help='NIMS DB URI')
        self.add_argument('stage_path', help='path to staging area')
        self.add_argument('--pubkey', default='internims/NIMSpubkey.pub', help='path to ssl pubkey')
        self.add_argument('-u', '--uid', default='local', help='site uid')
        self.add_argument('-f', '--logfile', help='path to log file')
        self.add_argument('-l', '--loglevel', default='info', help='path to log file')
        self.add_argument('-q', '--quiet', action='store_true', default=False, help='disable console logging')

routes = [
    webapp2.Route(r'/nimsapi',                                      NIMSAPI),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi', [
        webapp2.Route(r'/download',                                 NIMSAPI, handler_method='download', methods=['GET']),
        webapp2.Route(r'/dump',                                     NIMSAPI, handler_method='dump', methods=['GET']),
        webapp2.Route(r'/upload/<fid>',                             NIMSAPI, handler_method='upload', methods=['PUT']),
        webapp2.Route(r'/remotes',                                  Remotes),
        ]),
    # webapp2_extras.routes.PathPrefixRoute has bug, variable MUST have regex
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/<iid:[^/]+>', [
        webapp2.Route(r'/users',                                    Users),
        webapp2.Route(r'/users/count',                              Users, handler_method='count', methods=['GET']),
        webapp2.Route(r'/users/<uid>',                              User),
        webapp2.Route(r'/groups',                                   Groups),
        webapp2.Route(r'/groups/count',                             Groups, handler_method='count', methods=['GET']),
        webapp2.Route(r'/groups/<gid>',                             Group),
        webapp2.Route(r'/experiments',                              experiments.Experiments),
        webapp2.Route(r'/experiments/count',                        experiments.Experiments, handler_method='count', methods=['GET']),
        webapp2.Route(r'/experiments/<xid:[0-9a-f]{24}>',           experiments.Experiment),
        webapp2.Route(r'/experiments/<xid:[0-9a-f]{24}>/sessions',  sessions.Sessions),
        webapp2.Route(r'/sessions/count',                           sessions.Sessions, handler_method='count', methods=['GET']),
        webapp2.Route(r'/sessions/<sid:[0-9a-f]{24}>',              sessions.Session),
        webapp2.Route(r'/sessions/<sid:[0-9a-f]{24}>/move',         sessions.Session, handler_method='move'),
        webapp2.Route(r'/sessions/<sid:[0-9a-f]{24}>/epochs',       epochs.Epochs),
        webapp2.Route(r'/epochs/count',                             epochs.Epochs, handler_method='count', methods=['GET']),
        webapp2.Route(r'/epochs/<eid:[0-9a-f]{24}>',                epochs.Epoch),
    ]),
]


if __name__ == '__main__':
    args = ArgumentParser().parse_args()
    nimsutil.configure_log(args.logfile, not args.quiet, args.loglevel)

    from paste import httpserver
    app = webapp2.WSGIApplication(routes, debug=True, config=dict(stage_path=args.stage_path, site_id=args.uid, pubkey=args.pubkey))
    app.db = (pymongo.MongoReplicaSetClient(args.uri) if 'replicaSet' in args.uri else pymongo.MongoClient(args.uri)).get_default_database()
    httpserver.serve(app, host=httpserver.socket.gethostname(), port='8080')
