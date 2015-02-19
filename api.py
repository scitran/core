#!/usr/bin/env python
#
# @author:  Gunnar Schaefer, Kevin S. Hahn

import logging
import logging.config
log = logging.getLogger('scitran.api')
logging.getLogger('scitran.data').setLevel(logging.WARNING) # silence scitran.data logging
logging.getLogger('MARKDOWN').setLevel(logging.WARNING) # silence Markdown library logging

import os
import json
import webapp2
import bson.json_util
import webapp2_extras.routes

import core
import users
import projects
import sessions
import acquisitions
import collections_


routes = [
    webapp2.Route(r'/api',                                          core.Core),
    webapp2_extras.routes.PathPrefixRoute(r'/api', [
        webapp2.Route(r'/download',                                 core.Core, handler_method='download', methods=['GET', 'POST'], name='download'),
        webapp2.Route(r'/sites',                                    core.Core, handler_method='sites', methods=['GET']),
        webapp2.Route(r'/log',                                      core.Core, handler_method='log', methods=['GET']),
        webapp2.Route(r'/search',                                   core.Core, handler_method='search', methods=['GET', 'POST']),
    ]),
    webapp2.Route(r'/api/users',                                    users.Users),
    webapp2_extras.routes.PathPrefixRoute(r'/api/users', [
        webapp2.Route(r'/count',                                    users.Users, handler_method='count', methods=['GET']),
        webapp2.Route(r'/self',                                     users.User, handler_method='self', methods=['GET']),
        webapp2.Route(r'/roles',                                    users.User, handler_method='roles', methods=['GET']),
        webapp2.Route(r'/schema',                                   users.User, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/<_id>',                                    users.User, name='user'),
        webapp2.Route(r'/<_id>/groups',                             users.Groups, name='groups'),
    ]),
    webapp2.Route(r'/api/groups',                                   users.Groups),
    webapp2_extras.routes.PathPrefixRoute(r'/api/groups', [
        webapp2.Route(r'/count',                                    users.Groups, handler_method='count', methods=['GET']),
        webapp2.Route(r'/schema',                                   users.Group, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/<_id>',                                    users.Group, name='group'),
    ]),
    webapp2.Route(r'/api/projects',                                 projects.Projects),
    webapp2_extras.routes.PathPrefixRoute(r'/api/projects', [
        webapp2.Route(r'/count',                                    projects.Projects, handler_method='count', methods=['GET']),
        webapp2.Route(r'/groups',                                   projects.Projects, handler_method='groups', methods=['GET']),
        webapp2.Route(r'/schema',                                   projects.Project, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/<:[0-9a-f]{24}>',                          projects.Project, name='project'),
        webapp2.Route(r'/<:[0-9a-f]{24}>/file',                     projects.Project, handler_method='get_file', methods=['GET', 'POST']),
        webapp2.Route(r'/<:[0-9a-f]{24}>/file',                     projects.Project, handler_method='put_file', methods=['PUT']),
        webapp2.Route(r'/<:[0-9a-f]{24}>/sessions',                 sessions.Sessions, name='sessions'),
    ]),
    webapp2.Route(r'/api/collections',                              collections_.Collections),
    webapp2_extras.routes.PathPrefixRoute(r'/api/collections', [
        webapp2.Route(r'/count',                                    collections_.Collections, handler_method='count', methods=['GET']),
        webapp2.Route(r'/curators',                                 collections_.Collections, handler_method='curators', methods=['GET']),
        webapp2.Route(r'/schema',                                   collections_.Collection, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/<:[0-9a-f]{24}>',                          collections_.Collection, name='collection'),
        webapp2.Route(r'/<:[0-9a-f]{24}>/file',                     collections_.Collection, handler_method='get_file', methods=['GET', 'POST']),
        webapp2.Route(r'/<:[0-9a-f]{24}>/file',                     collections_.Collection, handler_method='put_file', methods=['PUT']),
        webapp2.Route(r'/<:[0-9a-f]{24}>/sessions',                 collections_.CollectionSessions, name='coll_sessions'),
        webapp2.Route(r'/<:[0-9a-f]{24}>/acquisitions',             collections_.CollectionAcquisitions, name='coll_acquisitions'),
    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/api/sessions', [
        webapp2.Route(r'/count',                                    sessions.Sessions, handler_method='count', methods=['GET']),
        webapp2.Route(r'/schema',                                   sessions.Session, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/<:[0-9a-f]{24}>',                          sessions.Session, name='session'),
        webapp2.Route(r'/<:[0-9a-f]{24}>/file',                     sessions.Session, handler_method='get_file', methods=['GET', 'POST']),
        webapp2.Route(r'/<:[0-9a-f]{24}>/file',                     sessions.Session, handler_method='put_file', methods=['PUT']),
        webapp2.Route(r'/<:[0-9a-f]{24}>/acquisitions',             acquisitions.Acquisitions, name='acquisitions'),
    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/api/acquisitions', [
        webapp2.Route(r'/count',                                    acquisitions.Acquisitions, handler_method='count', methods=['GET']),
        webapp2.Route(r'/schema',                                   acquisitions.Acquisition, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/<:[0-9a-f]{24}>',                          acquisitions.Acquisition, name='acquisition'),
        webapp2.Route(r'/<:[0-9a-f]{24}>/file',                     acquisitions.Acquisition, handler_method='get_file', methods=['GET', 'POST']),
        webapp2.Route(r'/<:[0-9a-f]{24}>/file',                     acquisitions.Acquisition, handler_method='put_file', methods=['PUT']),
    ]),
]

def dispatcher(router, request, response):
    rv = router.default_dispatcher(request, response)
    if rv is not None:
        response.write(json.dumps(rv, default=bson.json_util.default))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'

app = webapp2.WSGIApplication(routes)
app.router.set_dispatcher(dispatcher)
app.config = {
        'data_path':        'nims',
        'quarantine_path':  'quarantine',
        'site_id':          'local',
        'site_name':        'Local',
        'ssl_cert':         None,
        'insecure':         False,
        'log_path':         None,
        'demo':             False,
        }


if __name__ == '__main__':
    import pymongo
    import argparse
    import ConfigParser
    import paste.httpserver

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('config_file', help='path to config file')
    arg_parser.add_argument('--host', default='127.0.0.1', help='IP address to bind to')
    arg_parser.add_argument('--port', default='8080', help='TCP port to listen on')
    arg_parser.add_argument('--db_uri', help='NIMS DB URI')
    arg_parser.add_argument('--data_path', help='path to storage area')
    arg_parser.add_argument('--log_path', help='path to API log file')
    arg_parser.add_argument('--ssl_cert', help='path to SSL certificate file, containing private key and certificate chain')
    arg_parser.add_argument('--site_id', help='InterNIMS site ID')
    arg_parser.add_argument('--site_name', help='InterNIMS site name')
    arg_parser.add_argument('--oauth2_id_endpoint', help='OAuth2 provider ID endpoint')
    arg_parser.add_argument('--demo', help='demo mode, enables auto user creation')
    args = arg_parser.parse_args()

    app.config['here'] = os.path.dirname(os.path.abspath(args.config_file))
    config = ConfigParser.ConfigParser(app.config)
    config.read(args.config_file)
    logging.config.fileConfig(args.config_file, disable_existing_loggers=False)
    logging.getLogger('paste.httpserver').setLevel(logging.INFO) # silence paste logging

    app.config['site_id'] = args.site_id or app.config['site_id']
    app.config['site_name'] = args.site_name or app.config['site_name']
    app.config['data_path'] = os.path.join(args.data_path or config.get('nims', 'data_path'), 'nims')
    app.config['quarantine_path'] = os.path.join(args.data_path or config.get('nims', 'data_path'), 'quarantine')
    app.config['log_path'] = args.log_path or app.config['log_path']
    app.config['oauth2_id_endpoint'] = args.oauth2_id_endpoint or config.get('oauth2', 'id_endpoint')
    app.config['insecure'] = config.getboolean('nims', 'insecure')
    app.config['ssl_cert'] = args.ssl_cert or config.get('nims', 'ssl_cert')     # to give to requests
    app.config['demo'] = arg.demo or config.getboolean('nims', 'demo')

    if not app.config['ssl_cert']:
        log.warning('SSL certificate not specified, interNIMS functionality disabled')

    if not os.path.exists(app.config['data_path']):
        os.makedirs(app.config['data_path'])
    if not os.path.exists(app.config['quarantine_path']):
        os.makedirs(app.config['quarantine_path'])

    kwargs = dict(tz_aware=True)
    db_uri = args.db_uri or config.get('nims', 'db_uri')
    db_client = pymongo.MongoReplicaSetClient(db_uri, **kwargs) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri, **kwargs)
    app.db = db_client.get_default_database()

    app.debug = True # send stack trace for uncaught exceptions to client
    paste.httpserver.serve(app, host=args.host, port=args.port, ssl_pem=args.ssl_cert)
