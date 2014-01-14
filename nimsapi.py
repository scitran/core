#!/usr/bin/env python
#
# @author:  Gunnar Schaefer, Kevin S. Hahn

import os
import re
import json
import uuid
import hashlib
import logging
import pymongo
import tarfile
import webapp2
import zipfile
import argparse
import markdown
import bson.json_util
import webapp2_extras.routes
import Crypto.PublicKey.RSA

import nimsutil

import experiments
import nimsapiutil
import collections_

log = logging.getLogger('nimsapi')


class NIMSAPI(nimsapiutil.NIMSRequestHandler):

    """/nimsapi """

    def head(self):
        """Return 200 OK."""
        self.response.set_status(200)

    def get(self):
        """Return API documentation"""
        resources = """
            Resource                                            | Description
            :---------------------------------------------------|:-----------------------
            nimsapi/download                                    | download
            nimsapi/upload                                      | upload
            nimsapi/remotes                                     | list of remote instances
            [(nimsapi/users)]                                   | list of users
            [(nimsapi/users/count)]                             | count of users
            [(nimsapi/users/listschema)]                        | schema for user list
            [(nimsapi/users/schema)]                            | schema for single user
            nimsapi/users/*<uid>*                               | details for user *<uid>*
            [(nimsapi/groups)]                                  | list of groups
            [(nimsapi/groups/count)]                            | count of groups
            [(nimsapi/groups/listschema)]                       | schema for group list
            [(nimsapi/groups/schema)]                           | schema for single group
            nimsapi/groups/*<gid>*                              | details for group *<gid>*
            [(nimsapi/experiments)]                             | list of experiments
            [(nimsapi/experiments/count)]                       | count of experiments
            [(nimsapi/experiments/listschema)]                  | schema for experiment list
            [(nimsapi/experiments/schema)]                      | schema for single experiment
            nimsapi/experiments/*<xid>*                         | details for experiment *<xid>*
            nimsapi/experiments/*<xid>*/sessions                | list sessions for experiment *<xid>*
            [(nimsapi/sessions/count)]                          | count of sessions
            [(nimsapi/sessions/listschema)]                     | schema for sessions list
            [(nimsapi/sessions/schema)]                         | schema for single session
            nimsapi/sessions/*<sid>*                            | details for session *<sid>*
            nimsapi/sessions/*<sid>*/move                       | move session *<sid>* to a different experiment
            nimsapi/sessions/*<sid>*/epochs                     | list epochs for session *<sid>*
            [(nimsapi/epochs/count)]                            | count of epochs
            [(nimsapi/epochs/listschema)]                       | schema for epoch list
            [(nimsapi/epochs/schema)]                           | schema for single epoch
            nimsapi/epochs/*<eid>*                              | details for epoch *<eid>*
            [(nimsapi/collections)]                             | list of collections
            [(nimsapi/collections/count)]                       | count of collections
            [(nimsapi/collections/listschema)]                  | schema for collections list
            [(nimsapi/collections/schema)]                      | schema for single collection
            nimsapi/collections/*<cid>*                         | details for collection *<cid>*
            nimsapi/collections/*<cid>*/sessions                | list sessions for collection *<cid>*
            nimsapi/collections/*<cid>*/epochs?session=*<sid>*  | list of epochs for collection *<cid>*, optionally restricted to session *<sid>*
            """
        resources = re.sub(r'\[\((.*)\)\]', r'[\1](\1)', resources).replace('<', '&lt;').replace('>', '&gt;').strip()
        self.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        self.response.write('<html>\n')
        self.response.write('<head>\n')
        self.response.write('<title>NIMSAPI</title>\n')
        self.response.write('<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">\n')
        self.response.write('<style type="text/css">\n')
        self.response.write('table {width:0%; border-width:1px; padding: 0;border-collapse: collapse;}\n')
        self.response.write('table tr {border-top: 1px solid #b8b8b8; background-color: white; margin: 0; padding: 0;}\n')
        self.response.write('table tr:nth-child(2n) {background-color: #f8f8f8;}\n')
        self.response.write('table thead tr :last-child {width:100%;}\n')
        self.response.write('table tr th {font-weight: bold; border: 1px solid #b8b8b8; background-color: #cdcdcd; margin: 0; padding: 6px 13px;}\n')
        self.response.write('table tr th {font-weight: bold; border: 1px solid #b8b8b8; background-color: #cdcdcd; margin: 0; padding: 6px 13px;}\n')
        self.response.write('table tr td {border: 1px solid #b8b8b8; margin: 0; padding: 6px 13px;}\n')
        self.response.write('table tr th :first-child, table tr td :first-child {margin-top: 0;}\n')
        self.response.write('table tr th :last-child, table tr td :last-child {margin-bottom: 0;}\n')
        self.response.write('</style>\n')
        self.response.write('</head>\n')
        self.response.write('<body style="min-width:900px">\n')
        self.response.write(markdown.markdown(resources, ['extra']))
        self.response.write('</body>\n')
        self.response.write('</html>\n')

    def upload(self):
        # TODO add security: either authenticated user or machine-to-machine CRAM
        if 'Content-MD5' not in self.request.headers:
            self.abort(400, 'Request must contain a valid "Content-MD5" header.')
        filename = self.request.get('filename', 'anonymous')
        stage_path = self.app.config['stage_path']
        with nimsutil.TempDir(prefix='.tmp', dir=stage_path) as tempdir_path:
            hash_ = hashlib.md5()
            upload_filepath = os.path.join(tempdir_path, filename)
            log.info('upload from ' + self.request.user_agent + ': ' + os.path.basename(upload_filepath))
            with open(upload_filepath, 'wb') as upload_file:
                for chunk in iter(lambda: self.request.body_file.read(2**20), ''):
                    hash_.update(chunk)
                    upload_file.write(chunk)
            if hash_.hexdigest() != self.request.headers['Content-MD5']:
                self.abort(400, 'Content-MD5 mismatch.')
            if not tarfile.is_tarfile(upload_filepath) and not zipfile.is_zipfile(upload_filepath):
                self.abort(415)
            os.rename(upload_filepath, os.path.join(stage_path, str(uuid.uuid1()) + '_' + filename)) # add UUID to prevent clobbering files

    def download(self):
        paths = []
        symlinks = []
        for js_id in self.request.get('id', allow_multiple=True):
            type_, _id = js_id.split('_')
            _idpaths, _idsymlinks = resource_types[type_].download_info(_id)
            paths += _idpaths
            symlinks += _idsymlinks


class Users(nimsapiutil.NIMSRequestHandler):

    """/nimsapi/users """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'User List',
        'type': 'array',
        'items': {
            'title': 'User',
            'type': 'object',
            'properties': {
                '_id': {
                    'title': 'Database ID',
                    'type': 'string',
                },
                'firstname': {
                    'title': 'First Name',
                    'type': 'string',
                    'default': '',
                },
                'lastname': {
                    'title': 'Last Name',
                    'type': 'string',
                    'default': '',
                },
                'email': {
                    'title': 'Email',
                    'type': 'string',
                    'format': 'email',
                    'default': '',
                },
                'email_hash': {
                    'type': 'string',
                    'default': '',
                },
            }
        }
    }

    def count(self):
        """Return the number of Users."""
        self.response.write('%d users\n' % self.app.db.users.count())

    def post(self):
        """Create a new User"""
        self.response.write('users post\n')

    def get(self):
        """Return the list of Users."""
        projection = ['firstname', 'lastname', 'email_hash']
        users = list(self.app.db.users.find({}, projection))
        self.response.write(json.dumps(users, default=bson.json_util.default))

    def put(self):
        """Update many Users."""
        self.response.write('users put\n')


class User(nimsapiutil.NIMSRequestHandler):

    """/nimsapi/users/<uid> """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'User',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'Database ID',
                'type': 'string',
            },
            'firstname': {
                'title': 'First Name',
                'type': 'string',
                'default': '',
            },
            'lastname': {
                'title': 'Last Name',
                'type': 'string',
                'default': '',
            },
            'email': {
                'title': 'Email',
                'type': 'string',
                'format': 'email',
                'default': '',
            },
            'email_hash': {
                'type': 'string',
                'default': '',
            },
            'superuser': {
                'title': 'Superuser',
                'type': 'boolean',
            },
        },
        'required': ['_id'],
    }

    def get(self, uid):
        """Return User details."""
        user = self.app.db.users.find_one({'_id': uid})
        self.response.write(json.dumps(user, default=bson.json_util.default))

    def put(self, uid):
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
            self.app.db.users.update({'_id': uid}, updates)
        else:
            self.abort(403)

    def delete(self, uid):
        """Delete an User."""
        self.response.write('user %s delete, %s\n' % (uid, self.request.params))


class Groups(nimsapiutil.NIMSRequestHandler):

    """/nimsapi/groups """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Group List',
        'type': 'array',
        'items': {
            'title': 'Group',
            'type': 'object',
            'properties': {
                '_id': {
                    'title': 'Database ID',
                    'type': 'string',
                },
            }
        }
    }

    def count(self):
        """Return the number of Groups."""
        self.response.write('%d groups\n' % self.app.db.groups.count())

    def post(self):
        """Create a new Group"""
        self.response.write('groups post\n')

    def get(self):
        """Return the list of Groups."""
        projection = ['_id']
        groups = list(self.app.db.groups.find({}, projection))
        self.response.write(json.dumps(groups, default=bson.json_util.default))

    def put(self):
        """Update many Groups."""
        self.response.write('groups put\n')


class Group(nimsapiutil.NIMSRequestHandler):

    """/nimsapi/groups/<gid>"""

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Group',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'Database ID',
                'type': 'string',
            },
            'name': {
                'title': 'Name',
                'type': 'string',
                'maxLength': 32,
            },
            'pis': {
                'title': 'PIs',
                'type': 'array',
                'default': [],
                'items': {
                    'type': 'string',
                },
                'uniqueItems': True,
            },
            'admins': {
                'title': 'Admins',
                'type': 'array',
                'default': [],
                'items': {
                    'type': 'string',
                },
                'uniqueItems': True,
            },
            'memebers': {
                'title': 'Members',
                'type': 'array',
                'default': [],
                'items': {
                    'type': 'string',
                },
                'uniqueItems': True,
            },
        },
        'required': ['_id'],
    }

    def get(self, gid):
        """Return Group details."""
        group = self.app.db.groups.find_one({'_id': gid})
        self.response.write(json.dumps(group, default=bson.json_util.default))

    def put(self, gid):
        """Update an existing Group."""
        self.response.write('group %s put, %s\n' % (gid, self.request.params))

    def delete(self, gid):
        """Delete an Group."""


class Remotes(nimsapiutil.NIMSRequestHandler):

    """/nimsapi/remotes """

    def get(self):
        """Return the list of remotes where user has membership"""
        # TODO: implement special 'all' case - report ALL available instances, regardless of user permissions
        # applies to adding new remote users, need to be able to select from ALL available remote sites
        # query, user in userlist, _id does not match this site _id
        query = {'users': {'$in': [self.user['_id']]}, '_id': {'$ne': self.app.config['site_id']}}
        projection = ['_id']
        # if app has no site-id or pubkey, cannot fetch peer registry, and db.remotes will be empty
        remotes = list(self.app.db.remotes.find(query, projection))
        data_remotes = []                                   # for list buildup
        for remote in remotes:
            # use own API to dispatch requests (hacky)
            response = self.app.get_response('/nimsapi/experiments?user=' + self.user['_id'] + '&iid=' + remote['_id'], headers=[('User-Agent', 'remotes_requestor')])
            xpcount = len(json.loads(response.body))
            if xpcount > 0:
                log.debug('%s has access to %s expirements on %s' % (self.user['_id'], xpcount, remote['_id']))
                data_remotes.append(remote['_id'])

        # return json encoded list of remote site '_id's
        self.response.write(json.dumps(data_remotes, indent=4, separators=(',', ': ')))


class ArgumentParser(argparse.ArgumentParser):

    def __init__(self):
        super(ArgumentParser, self).__init__()
        self.add_argument('uri', help='NIMS DB URI')
        self.add_argument('stage_path', help='path to staging area')
        self.add_argument('-k', '--privkey', help='path to private SSL key file')
        self.add_argument('-u', '--uid', help='site UID')
        self.add_argument('-f', '--logfile', help='path to log file')
        self.add_argument('-l', '--loglevel', default='info', help='path to log file')
        self.add_argument('-q', '--quiet', action='store_true', default=False, help='disable console logging')

routes = [
    webapp2.Route(r'/nimsapi',                                      NIMSAPI),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi', [
        webapp2.Route(r'/download',                                 NIMSAPI, handler_method='download', methods=['GET']),
        webapp2.Route(r'/upload',                                   NIMSAPI, handler_method='upload', methods=['PUT']),
        webapp2.Route(r'/remotes',                                  Remotes),
        webapp2.Route(r'/users',                                    Users),
        webapp2.Route(r'/users/count',                              Users, handler_method='count', methods=['GET']),
        webapp2.Route(r'/users/listschema',                         Users, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/users/schema',                             User, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/users/<uid>',                              User),
        webapp2.Route(r'/groups',                                   Groups),
        webapp2.Route(r'/groups/count',                             Groups, handler_method='count', methods=['GET']),
        webapp2.Route(r'/groups/listschema',                        Groups, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/groups/schema',                            Group, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/groups/<gid>',                             Group),
        webapp2.Route(r'/experiments',                              experiments.Experiments),
        webapp2.Route(r'/experiments/count',                        experiments.Experiments, handler_method='count', methods=['GET']),
        webapp2.Route(r'/experiments/listschema',                   experiments.Experiments, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/experiments/schema',                       experiments.Experiment, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/experiments/<xid:[0-9a-f]{24}>',           experiments.Experiment),
        webapp2.Route(r'/experiments/<xid:[0-9a-f]{24}>/sessions',  experiments.Sessions),
        webapp2.Route(r'/sessions/count',                           experiments.Sessions, handler_method='count', methods=['GET']),
        webapp2.Route(r'/sessions/listschema',                      experiments.Sessions, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/sessions/schema',                          experiments.Session, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/sessions/<sid:[0-9a-f]{24}>',              experiments.Session),
        webapp2.Route(r'/sessions/<sid:[0-9a-f]{24}>/move',         experiments.Session, handler_method='move'),
        webapp2.Route(r'/sessions/<sid:[0-9a-f]{24}>/epochs',       experiments.Epochs),
        webapp2.Route(r'/epochs/count',                             experiments.Epochs, handler_method='count', methods=['GET']),
        webapp2.Route(r'/epochs/listschema',                        experiments.Epochs, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/epochs/schema',                            experiments.Epoch, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/epochs/<eid:[0-9a-f]{24}>',                experiments.Epoch),
        webapp2.Route(r'/collections',                              collections_.Collections),
        webapp2.Route(r'/collections/count',                        collections_.Collections, handler_method='count', methods=['GET']),
        webapp2.Route(r'/collections/listschema',                   collections_.Collections, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/collections/schema',                       collections_.Collection, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/collections/<cid:[0-9a-f]{24}>',           collections_.Collection),
        webapp2.Route(r'/collections/<cid:[0-9a-f]{24}>/sessions',  collections_.Sessions),
        webapp2.Route(r'/collections/<cid:[0-9a-f]{24}>/epochs',    collections_.Epochs),
    ]),
]

app = webapp2.WSGIApplication(routes, debug=True)


if __name__ == '__main__':
    args = ArgumentParser().parse_args()
    nimsutil.configure_log(args.logfile, not args.quiet, args.loglevel)

    if args.privkey:
        privkey = Crypto.PublicKey.RSA.importKey(open(args.privkey).read())
        log.info('private SSL key loaded successfully')
    else:
        privkey = None
        log.warning('private SSL key not specified: internims functionality disabled')

    import paste.httpserver
    app.config = dict(stage_path=args.stage_path, site_id=args.uid, privkey=privkey)
    app.db = (pymongo.MongoReplicaSetClient(args.uri) if 'replicaSet' in args.uri else pymongo.MongoClient(args.uri)).get_default_database()
    paste.httpserver.serve(app, port='8080')

# import nimsapi, webapp2, pymongo, bson.json_util
# nimsapi.app.db = pymongo.MongoClient('mongodb://nims:cnimr750@slice.stanford.edu/nims').get_default_database()
# nimsapi.app.config['site_id'] = 'local'
# headers = [('User-Agent', 'nimsfs')]
# response = webapp2.Request.blank('/nimsapi/experiments?user=gsfr', headers=headers).get_response(nimsapi.app)
# response.status
# response.body
