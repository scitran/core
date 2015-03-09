#!/usr/bin/env python
#
# @author:  Gunnar Schaefer, Kevin S. Hahn

import logging
logging.basicConfig(
        format='%(asctime)s %(name)16.16s:%(levelname)4.4s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        )
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


if __name__ == '__main__':
    import pymongo
    import argparse
    import paste.httpserver

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--host', default='127.0.0.1', help='IP address to bind to [127.0.0.1]')
    arg_parser.add_argument('--port', default='8080', help='TCP port to listen on [8080]')
    arg_parser.add_argument('--db_uri', help='SciTran DB URI', default='mongodb://localhost/scitran')
    arg_parser.add_argument('--data_path', help='path to storage area', required=True)
    arg_parser.add_argument('--log_level', help='log level [info]', default='info')
    arg_parser.add_argument('--ssl_cert', help='path to SSL certificate file, containing private key and certificate chain', required=True)
    arg_parser.add_argument('--site_id', help='site ID for Scitran Central [local]', default='local')
    arg_parser.add_argument('--oauth2_id_endpoint', help='OAuth2 provider ID endpoint', default='https://www.googleapis.com/plus/v1/people/me/openIdConnect')
    arg_parser.add_argument('--demo', help='enable automatic user creation', action='store_true', default=False)
    arg_parser.add_argument('--insecure', help='allow user info as urlencoded param', action='store_true', default=False)
    args = arg_parser.parse_args()

    args.quarantine_path = os.path.join(args.data_path, 'quarantine')
    app.config = vars(args)

    logging.getLogger('paste.httpserver').setLevel(logging.WARNING) # silence paste logging
    log.setLevel(getattr(logging, args.log_level.upper()))

    if not app.config['ssl_cert']:
        log.warning('SSL certificate not specified, SciTran Central functionality disabled')
    if app.config['site_id'] == 'local':
        log.warning('site_id not configured, SciTran Central functionality disabled')

    if not os.path.exists(app.config['data_path']):
        os.makedirs(app.config['data_path'])
    if not os.path.exists(app.config['quarantine_path']):
        os.makedirs(app.config['quarantine_path'])

    kwargs = dict(tz_aware=True)
    db_client = pymongo.MongoReplicaSetClient(args.db_uri, **kwargs) if 'replicaSet' in args.db_uri else pymongo.MongoClient(args.db_uri, **kwargs)
    app.db = db_client.get_default_database()
    app.db.sites.update({'_id': args.site_id}, {'_id': args.site_id, 'name': 'Local'}, upsert=True)

    app.debug = True # send stack trace for uncaught exceptions to client
    paste.httpserver.serve(app, host=args.host, port=args.port, ssl_pem=args.ssl_cert)
