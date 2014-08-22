#!/usr/bin/env python
#
# @author:  Gunnar Schaefer, Kevin S. Hahn

import logging
import logging.config
log = logging.getLogger('nimsapi')

import json
import webapp2
import bson.json_util
import webapp2_extras.routes

import core
import users
import experiments
import collections_


routes = [
    webapp2.Route(r'/nimsapi',                                          core.Core),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi', [
        webapp2.Route(r'/download',                                     core.Core, handler_method='download', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/login',                                        core.Core, handler_method='login', methods=['OPTIONS', 'GET', 'POST']),
        webapp2.Route(r'/sites',                                        core.Core, handler_method='sites', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/roles',                                        core.Core, handler_method='roles', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/log',                                          core.Core, handler_method='log', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/search',                                       core.Core, handler_method='search', methods=['OPTIONS', 'GET', 'POST']),
    ]),
    webapp2.Route(r'/nimsapi/users',                                    users.Users),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/users', [
        webapp2.Route(r'/count',                                        users.Users, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   users.Users, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       users.User, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<_id>',                                        users.User, name='user'),
        webapp2.Route(r'/<_id>/groups',                                 users.Groups, name='groups'),
    ]),
    webapp2.Route(r'/nimsapi/groups',                                   users.Groups),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/groups', [
        webapp2.Route(r'/count',                                        users.Groups, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   users.Groups, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       users.Group, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<_id>',                                        users.Group, name='group'),
    ]),
    webapp2.Route(r'/nimsapi/experiments',                              experiments.Experiments),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/experiments', [
        webapp2.Route(r'/count',                                        experiments.Experiments, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   experiments.Experiments, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       experiments.Experiment, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<xid:[0-9a-f]{24}>',                           experiments.Experiment, name='experiment'),
        webapp2.Route(r'/<xid:[0-9a-f]{24}>/sessions',                  experiments.Sessions, name='sessions'),
    ]),
    webapp2.Route(r'/nimsapi/collections',                              collections_.Collections),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/collections', [
        webapp2.Route(r'/count',                                        collections_.Collections, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   collections_.Collections, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       collections_.Collection, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<cid:[0-9a-f]{24}>',                           collections_.Collection, name='collection'),
        webapp2.Route(r'/<cid:[0-9a-f]{24}>/sessions',                  collections_.Sessions, name='vp_sessions'),
        webapp2.Route(r'/<cid:[0-9a-f]{24}>/epochs',                    collections_.Epochs, name='vp_epochs'),
    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/sessions', [
        webapp2.Route(r'/count',                                        experiments.Sessions, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   experiments.Sessions, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       experiments.Session, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<sid:[0-9a-f]{24}>',                           experiments.Session, name='session'),
        webapp2.Route(r'/<sid:[0-9a-f]{24}>/epochs',                    experiments.Epochs, name='epochs'),
    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/epochs', [
        webapp2.Route(r'/count',                                        experiments.Epochs, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   experiments.Epochs, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       experiments.Epoch, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<eid:[0-9a-f]{24}>',                           experiments.Epoch, name='epoch'),
    ]),
]

def dispatcher(router, request, response):
    rv = router.default_dispatcher(request, response)
    if rv is not None:
        return response.write(json.dumps(rv, default=bson.json_util.default))

app = webapp2.WSGIApplication(routes)
app.router.set_dispatcher(dispatcher)
app.config = {
        'store_path':   '.',
        'site_id':      'local',
        'site_name':    'Local',
        'ssl_cert':     None,
        'insecure':     False,
        'log_path':     None,
        }


if __name__ == '__main__':
    import os
    import pymongo
    import argparse
    import ConfigParser
    import paste.httpserver

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('config_file', help='path to config file')
    arg_parser.add_argument('--db_uri', help='NIMS DB URI')
    arg_parser.add_argument('--store_path', help='path to staging area')
    arg_parser.add_argument('--log_path', help='path to API log file')
    arg_parser.add_argument('--ssl_cert', help='path to SSL certificate file, containing private key and certificate chain')
    arg_parser.add_argument('--site_id', help='InterNIMS site ID')
    arg_parser.add_argument('--site_name', help='InterNIMS site name')
    arg_parser.add_argument('--oauth2_id_endpoint', help='OAuth2 provider ID endpoint')
    args = arg_parser.parse_args()

    app.config['here'] = os.path.dirname(os.path.abspath(args.config_file))
    config = ConfigParser.ConfigParser(app.config)
    config.read(args.config_file)
    logging.config.fileConfig(args.config_file, disable_existing_loggers=False)
    logging.getLogger('paste.httpserver').setLevel(logging.INFO) # silence paste logging

    app.config['site_id'] = args.site_id or app.config['site_id']
    app.config['site_name'] = args.site_name or app.config['site_name']
    app.config['store_path'] = args.store_path or config.get('nims', 'store_path')
    app.config['log_path'] = args.log_path or app.config['log_path']
    app.config['oauth2_id_endpoint'] = args.oauth2_id_endpoint or config.get('oauth2', 'id_endpoint')
    app.config['insecure'] = config.getboolean('nims', 'insecure')
    app.config['ssl_cert'] = args.ssl_cert or config.get('nims', 'ssl_cert')     # to give to requests

    if not app.config['ssl_cert']:
        log.warning('SSL certificate not specified, interNIMS functionality disabled')

    kwargs = dict(tz_aware=True)
    db_uri = args.db_uri or config.get('nims', 'db_uri')
    db_client = pymongo.MongoReplicaSetClient(db_uri, **kwargs) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri, **kwargs)
    app.db = db_client.get_default_database()

    app.debug = True
    paste.httpserver.serve(app, port='8080')
