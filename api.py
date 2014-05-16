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
    ]),
    webapp2.Route(r'/nimsapi/users',                                    users.Users),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/users', [
        webapp2.Route(r'/count',                                        users.Users, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   users.Users, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       users.User, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<uid>',                                        users.User),
    ]),
    webapp2.Route(r'/nimsapi/groups',                                   users.Groups),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/groups', [
        webapp2.Route(r'/count',                                        users.Groups, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   users.Groups, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       users.Group, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<gid>',                                        users.Group),
    ]),
    webapp2.Route(r'/nimsapi/experiments',                              experiments.Experiments),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/experiments', [
        webapp2.Route(r'/count',                                        experiments.Experiments, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   experiments.Experiments, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       experiments.Experiment, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<xid:[0-9a-f]{24}>',                           experiments.Experiment),
        webapp2.Route(r'/<xid:[0-9a-f]{24}>/sessions',                  experiments.Sessions),
    ]),
    webapp2.Route(r'/nimsapi/collections',                              collections_.Collections),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/collections', [
        webapp2.Route(r'/count',                                        collections_.Collections, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   collections_.Collections, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       collections_.Collection, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<cid:[0-9a-f]{24}>',                           collections_.Collection),
        webapp2.Route(r'/<cid:[0-9a-f]{24}>/sessions',                  collections_.Sessions),
        webapp2.Route(r'/<cid:[0-9a-f]{24}>/epochs',                    collections_.Epochs),
    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/sessions', [
        webapp2.Route(r'/count',                                        experiments.Sessions, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   experiments.Sessions, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       experiments.Session, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<sid:[0-9a-f]{24}>',                           experiments.Session),
        webapp2.Route(r'/<sid:[0-9a-f]{24}>/epochs',                    experiments.Epochs),
    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/nimsapi/epochs', [
        webapp2.Route(r'/count',                                        experiments.Epochs, handler_method='count', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/listschema',                                   experiments.Epochs, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/schema',                                       experiments.Epoch, handler_method='schema', methods=['OPTIONS', 'GET']),
        webapp2.Route(r'/<eid:[0-9a-f]{24}>',                           experiments.Epoch),
    ]),
]

def dispatcher(router, request, response):
    rv = router.default_dispatcher(request, response)
    if rv is not None:
        return response.write(json.dumps(rv, default=bson.json_util.default))

app = webapp2.WSGIApplication(routes)
app.router.set_dispatcher(dispatcher)
app.config = dict(store_path='', site_id='local', site_name='Local', ssl_key=None, insecure=False, log_path='')


if __name__ == '__main__':
    import os
    import sys
    import pymongo
    import argparse
    import ConfigParser
    import paste.httpserver
    import Crypto.PublicKey.RSA

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('config_file', help='path to config file')
    arg_parser.add_argument('--db_uri', help='NIMS DB URI')
    arg_parser.add_argument('--store_path', help='path to staging area')
    arg_parser.add_argument('--log_path', help='path to API log file')
    arg_parser.add_argument('--ssl_key', help='path to private SSL key file')
    arg_parser.add_argument('--site_id', help='InterNIMS site ID')
    arg_parser.add_argument('--site_name', help='InterNIMS site name')
    arg_parser.add_argument('--oauth2_id_endpoint', help='OAuth2 provider ID endpoint')
    args = arg_parser.parse_args()

    config = ConfigParser.ConfigParser({'here': os.path.dirname(os.path.abspath(args.config_file))})
    config.read(args.config_file)
    logging.config.fileConfig(args.config_file, disable_existing_loggers=False)
    logging.getLogger('paste.httpserver').setLevel(logging.INFO) # silence paste logging

    if args.ssl_key:
        try:
            ssl_key = Crypto.PublicKey.RSA.importKey(open(args.ssl_key).read())
        except:
            log.error(args.ssl_key + ' is not a valid private SSL key file, bailing out')
            sys.exit(1)
        else:
            log.debug('successfully loaded private SSL key from ' + args.ssl_key)
            app.config['ssl_key'] = ssl_key
    else:
        log.warning('private SSL key not specified, InterNIMS functionality disabled')

    app.config['site_id'] = args.site_id or app.config['site_id']
    app.config['site_name'] = args.site_name or app.config['site_name']
    app.config['store_path'] = args.store_path or config.get('nims', 'store_path')
    app.config['log_path'] = args.log_path or app.config['log_path']
    app.config['oauth2_id_endpoint'] = args.oauth2_id_endpoint or config.get('oauth2', 'id_endpoint')
    app.config['insecure'] = config.getboolean('nims', 'insecure')

    kwargs = dict(tz_aware=True)
    db_uri = args.db_uri or config.get('nims', 'db_uri')
    db_client = pymongo.MongoReplicaSetClient(db_uri, **kwargs) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri, **kwargs)
    app.db = db_client.get_default_database()

    app.debug = True
    paste.httpserver.serve(app, port='8080')
