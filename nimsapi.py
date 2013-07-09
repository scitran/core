#!/usr/bin/env python
#
# @author:  Gunnar Schaefer

import os
import bson
import pprint
import hashlib
import pymongo
import tarfile
import webapp2
import zipfile
import argparse

import nimsutil

#import experiments
#import subjects
#import sessions
#import epochs
#import users
#import groups

db = None
stage_path = None

#resource_types = {
#        'exp':      experiments,
#        'subj':     subjects,
#        'sess':     sessions,
#        'epoch':    epochs,
#        'uid':      users,
#        'gid':      groups,
#        }


class NIMSAPI(webapp2.RequestHandler):

    def get(self):
        #a = range(10000000)
        #b = [i*i for i in a]
        #return webapp2.redirect('/nimsapi/dump')
        self.response.write('nimsapi\n')

    def upload(self, filename):
        #pprint.pprint(vars(self.request))
        hash_ = hashlib.md5()
        with nimsutil.TempDir(prefix='.tmp', dir=stage_path) as tempdir_path:
            upload_filepath = os.path.join(tempdir_path, filename)
            log.info(os.path.basename(upload_filepath))
            with open(upload_filepath, 'wb') as upload_file:
                for chunk in iter(lambda: self.request.body_file.read(2**20), ''):
                    hash_.update(chunk)
                    upload_file.write(chunk)
            if hash_.hexdigest() != self.request.get('md5'):
                self.abort(406)
            if not tarfile.is_tarfile(upload_filepath) and not zipfile.is_zipfile(upload_filepath):
                self.abort(415)
            os.rename(upload_filepath, os.path.join(stage_path, filename))

    def download(self):
        paths = []
        symlinks = []
        for js_id in self.request.get('id', allow_multiple=True):
            type_, _id = js_id.split('_')
            _idpaths, _idsymlinks = resource_types[type_].download_info(_id)
            paths += _idpaths
            symlinks += _idsymlinks

    def dump(self):
        self.response.write('<pre>\n')
        self.response.write(pprint.pformat(list(db.sessions.find())))
        self.response.write('</pre>\n')


class Experiments(webapp2.RequestHandler):

    def count(self):
        """Return the number of Experiments."""
        self.response.write('%d experiments\n' % db.experiments.count())

    def post(self):
        """Create a new Experiment"""
        self.response.write('experiments post\n')

    def get(self):
        """Return the list of Experiments."""
        self.response.write('<pre>\n')
        self.response.write(pprint.pformat(list(db.experiments.find())))
        self.response.write('</pre>\n')

    def put(self):
        """Update many Experiments."""
        self.response.write('experiments put\n')


class Experiment(webapp2.RequestHandler):

    def get(self, _id):
        """Return Experiment details."""
        self.response.write('<pre>\n')
        self.response.write('experiment %s get, %s\n' % (_id, self.request.params))
        self.response.write(pprint.pformat(list(db.experiments.find({'_id': bson.objectid.ObjectId(_id)}))))
        self.response.write('</pre>\n')

    def put(self, _id):
        """Update an existing Experiment."""
        self.response.write('experiment %s put, %s\n' % (_id, self.request.params))

    def delete(self, _id):
        """Delete an Experiment."""
        self.response.write('experiment %s delete, %s\n' % (_id, self.request.params))


class ExperimentSubjects(webapp2.RequestHandler):

    def get(self, _id):
        """Return the list of Experiment Subjects."""
        self.response.write('experiment %s get subjects, %s\n' % (_id, self.request.params))


class ExperimentSessions(webapp2.RequestHandler):

    def get(self, _id):
        """Return the list of Experiment Sessions."""
        self.response.write('<pre>\n')
        self.response.write('experiment %s get sessions, %s\n' % (_id, self.request.params))
        self.response.write(pprint.pformat(list(db.sessions.find({'experiment': bson.objectid.ObjectId(_id)}, ['timestamp']))))
        self.response.write('</pre>\n')


class ExperimentDatasets(webapp2.RequestHandler):

    def get(self, _id):
        """Return the list of Experiment Datasets."""
        self.response.write('experiment %s get datasets, %s\n' % (_id, self.request.params))


class Sessions(webapp2.RequestHandler):

    def count(self):
        """Return the number of Sessions."""
        self.response.write('sessions count\n')

    def post(self):
        """Create a new Session"""
        self.response.write('sessions post\n')

    def get(self):
        """Return the list of Sessions."""
        self.response.write('list sessions\n')

    def put(self):
        """Update many Sessions."""
        self.response.write('sessions put\n')


class Session(webapp2.RequestHandler):

    def get(self, _id):
        """Return Session details."""
        self.response.write('<pre>\n')
        self.response.write('session %s get, %s\n' % (_id, self.request.params))
        self.response.write(pprint.pformat(list(db.sessions.find({'_id': _id}))))
        self.response.write('</pre>\n')

    def put(self, _id):
        """Update an existing Session."""
        self.response.write('session %s put, %s\n' % (_id, self.request.params))

    def delete(self, _id):
        """Delete an Session."""
        self.response.write('session %s delete, %s\n' % (_id, self.request.params))

    def move(self, _id):
        """
        Move a Session to another Subject or Experiment.

        Usage:
            /nimsapi/sessions/123/move?dest=subj_456
            /nimsapi/sessions/123/move?dest=exp_789
        """
        self.response.write('session %s move, %s\n' % (_id, self.request.params))


class SessionEpochs(webapp2.RequestHandler):

    def get(self, _id):
        """Return the list of Session Epochs."""
        self.response.write('session %s get epochs, %s\n' % (_id, self.request.params))


class SessionDatasets(webapp2.RequestHandler):

    def get(self, _id):
        """Return the list of Session Datasets."""
        self.response.write('session %s get datasets, %s\n' % (_id, self.request.params))


class Users(webapp2.RequestHandler):

    def count(self):
        """Return the number of Users."""
        self.response.write('%d users\n' % db.users.count())

    def post(self):
        """Create a new User"""
        self.response.write('users post\n')

    def get(self):
        """Return the list of Users."""
        self.response.write('<pre>\n')
        self.response.write(pprint.pformat(list(db.users.find({}, ['firstname', 'lastname']))))
        self.response.write('</pre>\n')

    def put(self):
        """Update many Users."""
        self.response.write('users put\n')


class User(webapp2.RequestHandler):

    def get(self, _id):
        """Return User details."""
        self.response.write('<pre>\n')
        self.response.write('user %s get, %s\n' % (_id, self.request.params))
        self.response.write(pprint.pformat(list(db.users.find({'_id': _id}))))
        self.response.write('</pre>\n')

    def put(self, _id):
        """Update an existing User."""
        self.response.write('user %s put, %s\n' % (_id, self.request.params))

    def delete(self, _id):
        """Delete an User."""
        self.response.write('user %s delete, %s\n' % (_id, self.request.params))


class Groups(webapp2.RequestHandler):

    def count(self):
        """Return the number of Groups."""
        self.response.write('%d groups\n' % db.groups.count())

    def post(self):
        """Create a new Group"""
        self.response.write('groups post\n')

    def get(self):
        """Return the list of Groups."""
        self.response.write('<pre>\n')
        self.response.write(pprint.pformat(list(db.groups.find({}, []))))
        self.response.write('</pre>\n')

    def put(self):
        """Update many Groups."""
        self.response.write('groups put\n')


class Group(webapp2.RequestHandler):

    def get(self, _id):
        """Return Group details."""
        self.response.write('<pre>\n')
        self.response.write('group %s get, %s\n' % (_id, self.request.params))
        self.response.write(pprint.pformat(list(db.groups.find({'_id': _id}))))
        self.response.write('</pre>\n')

    def put(self, _id):
        """Update an existing Group."""
        self.response.write('group %s put, %s\n' % (_id, self.request.params))

    def delete(self, _id):
        """Delete an Group."""


class ArgumentParser(argparse.ArgumentParser):

    def __init__(self):
        super(ArgumentParser, self).__init__()
        self.add_argument('uri', help='NIMS DB URI')
        self.add_argument('db', help='NIMS DB name')
        self.add_argument('stage_path', help='path to staging area')
        self.add_argument('-n', '--logname', default=os.path.splitext(os.path.basename(__file__))[0], help='process name for log')
        self.add_argument('-f', '--logfile', help='path to log file')
        self.add_argument('-l', '--loglevel', default='info', help='path to log file')
        self.add_argument('-q', '--quiet', action='store_true', default=False, help='disable console logging')


routes = [
        webapp2.Route(r'/nimsapi',                                          NIMSAPI),
        webapp2.Route(r'/nimsapi/upload/<filename:.+>',                     NIMSAPI, handler_method='upload', methods=['PUT']),
        webapp2.Route(r'/nimsapi/download',                                 NIMSAPI, handler_method='download', methods=['GET']),
        webapp2.Route(r'/nimsapi/dump',                                     NIMSAPI, handler_method='dump', methods=['GET']),
        webapp2.Route(r'/nimsapi/experiments',                              Experiments),
        webapp2.Route(r'/nimsapi/experiments/count',                        Experiments, handler_method='count', methods=['GET']),
        webapp2.Route(r'/nimsapi/experiments/<_id:[0-9a-f]+>',              Experiment),
        webapp2.Route(r'/nimsapi/experiments/<_id:[0-9a-f]+>/subjects',     ExperimentSubjects),
        webapp2.Route(r'/nimsapi/experiments/<_id:[0-9a-f]+>/sessions',     ExperimentSessions),
        webapp2.Route(r'/nimsapi/experiments/<_id:[0-9a-f]+>/datasets',     ExperimentDatasets),
        webapp2.Route(r'/nimsapi/sessions',                                 Sessions),
        webapp2.Route(r'/nimsapi/sessions/count',                           Sessions, handler_method='count', methods=['GET']),
        webapp2.Route(r'/nimsapi/sessions/<_id:[0-9.]+>',                   Session),
        webapp2.Route(r'/nimsapi/sessions/<_id:[0-9.]+>/move',              Session, handler_method='move'),
        webapp2.Route(r'/nimsapi/users',                                    Users),
        webapp2.Route(r'/nimsapi/users/count',                              Users, handler_method='count', methods=['GET']),
        webapp2.Route(r'/nimsapi/users/<_id:.+>',                           User),
        webapp2.Route(r'/nimsapi/groups',                                   Groups),
        webapp2.Route(r'/nimsapi/groups/count',                             Groups, handler_method='count', methods=['GET']),
        webapp2.Route(r'/nimsapi/groups/<_id:.+>',                          Group),
        ]

if __name__ == '__main__':
    args = ArgumentParser().parse_args()
    log = nimsutil.get_logger(args.logname, args.logfile, not args.quiet, args.loglevel)
    stage_path = args.stage_path
    db = pymongo.MongoClient(*pymongo.uri_parser.parse_host(args.uri))[args.db]

    from paste import httpserver
    nimsapi = webapp2.WSGIApplication(routes, debug=True)
    httpserver.serve(nimsapi, host=httpserver.socket.gethostname(), port='8080')
else:
    nimsapi = webapp2.WSGIApplication(routes, debug=True)


#API = NIMSAPI
#APIResource = experiments.Experiments
#routes = [
#        webapp2.Route(r'/', API),
#        webapp2.Route(r'/resource', APIResource),
#        ]
#
#from webapp2_extras.routes import PathPrefixRoute
#
#if __name__ == '__main__':
#    from paste import httpserver
#    nimsapi = webapp2.WSGIApplication([PathPrefixRoute('/', routes)], debug=True)
#    httpserver.serve(nimsapi, host='127.0.0.1', port='8080')
#else:
#    from webapp2_extras.routes import PathPrefixRoute
#    nimsapi = webapp2.WSGIApplication([PathPrefixRoute('/nimsapi', routes)], debug=True)


# GUI:
# /nims/status
# /nims/browse
# /nims/experiments
# /nims/groups
# /nims/preferences
# /nims/admin
