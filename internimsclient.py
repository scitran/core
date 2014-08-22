#!/usr/bin/env python
# @author:  Gunnar Schaefer, Kevin S. Hahn
"""
Client registers this instance with a central instance registery.

Client sends information about non-local users who are permitted
to access data in the local instance.  The local instance will
recieve information about other registered instances, and which of it's
local users are permitted to access data in other instances.
"""

import logging
import logging.config
log = logging.getLogger('internims')
logging.getLogger('requests').setLevel(logging.WARNING)  # silence Requests library logging

import re
import json
import requests


def update(db, api_uri, site_name, site_id, ssl_cert, internims_url):
    """Send is-alive signal to central peer registry."""
    exp_userlist = [e['permissions'] for e in db.experiments.find(None, {'_id': False, 'permissions.uid': True, 'permissions.site': True})]
    col_userlist = [c['permissions'] for c in db.collections.find(None, {'_id': False, 'permissions.uid': True, 'permissions.site': True})]
    grp_userlist = [g['roles'] for g in db.groups.find(None, {'_id': False, 'roles.uid': True, 'roles.site': True})]
    # cannot hash on dictionary; temporarily use tuple
    remote_users = set([(user['uid'], user['site']) for container in exp_userlist+col_userlist+grp_userlist for user in container if user.get('site') is not None])
    remote_users = [{'user': user[0], 'site': user[1]} for user in remote_users]

    payload = json.dumps({'_id': site_id, 'api_uri': api_uri, 'users': remote_users, 'name': site_name})
    try:
        r = requests.post(internims_url, data=payload, cert=ssl_cert)
    except requests.exceptions.ConnectionError:
        log.debug('SDMC is not reachable')
    else:
        if r.status_code == 200:
            # expecting
            # {'sites': [{'_id': 'foo.example.org', 'name': 'Example', 'api_uri': 'foo.example.org/api'},],
            #  'users': {'username1': [{'_id': 'site.hostname.edu', 'name': 'FooFooLand'}],
            #            }
            # }
            response = (json.loads(r.content))
            sites = response.get('sites')
            users = response.get('users')
            log.debug('recieved sites: %s ' % ', '.join(s['_id'] for s in sites))
            log.debug('recieved users: %s' % ', '.join([key for key in users]))
            if response.get('users'):
                for uid, remotes in response['users'].iteritems():
                    db.users.update({'_id': uid}, {'$set': {'remotes': remotes}})
            if sites:
                db.remotes.remove({'_id': {'$nin': [site['_id'] for site in response['sites']]}})
                [db.remotes.update({'_id': site['_id']}, site, upsert=True) for site in sites]
                db.users.update(   # clean users who no longer have remotes
                        {'remotes': {'$exists': True}, '_id': {'$nin': users.keys()}},
                        {'$unset': {'remotes': ''}},
                        multi=True,
                        )
            log.info('%3d users with remote data, %3d remotes' % (
                    len([u['_id'] for u in db.users.find({'remotes': {'$exists': True}}, {'_id': True})]),
                    len([s['_id'] for s in db.remotes.find({}, {'_id': True})])
                    ))
            return True
        else:
            # r.reason contains generic description for the specific error code
            # need the part of the error response body that contains the detailed explanation
            reason = re.search('<br /><br />\n(.*)\n\n\n </body>\n</html>', r.content)
            if reason:
                msg = reason.group(1)
            else:
                msg = r.reason
            log.warning('%s - %s' % (r.status_code, msg))
            return False


def clean_remotes(db):
    """Remove db.remotes, and removes remotes field from all db.users."""
    log.debug('removing remotes from users, and remotes collection')
    db.remotes.remove({})
    db.users.update({'remotes': {'$exists': True}}, {'$unset': {'remotes': ''}}, multi=True)


if __name__ == '__main__':
    import os
    import time
    import pymongo
    import argparse
    import ConfigParser

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('configfile',  help='path to configuration file')
    arg_parser.add_argument('--internims_url', help='https://internims.appspot.com')
    arg_parser.add_argument('--db_uri', help='DB URI')
    arg_parser.add_argument('--api_uri', help='API URL, without http:// or https://')
    arg_parser.add_argument('--site_id', help='instance hostname (used as unique ID)')
    arg_parser.add_argument('--site_name', help='instance name')
    arg_parser.add_argument('--sleeptime', default=60, type=int, help='time to sleep between is alive signals')
    arg_parser.add_argument('--ssl_cert', help='path to server ssl certificate file')
    arg_parser.add_argument('--debug', help='enable default mode', action='store_true', default=False)
    args = arg_parser.parse_args()

    config = ConfigParser.ConfigParser({'here': os.path.dirname(os.path.abspath(args.configfile))})
    config.read(args.configfile)
    logging.config.fileConfig(args.configfile, disable_existing_loggers=False)

    ssl_cert = args.ssl_cert or config.get('nims', 'ssl_cert')

    db_uri = args.db_uri or config.get('nims', 'db_uri')
    db = (pymongo.MongoReplicaSetClient(db_uri) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri)).get_default_database()

    site_name = args.site_name or config.get('nims', 'site_name')
    site_id = args.site_id or config.get('nims', 'site_id')
    api_uri = args.api_uri or config.get('nims', 'api_uri')
    internims_url = args.internims_url or config.get('nims', 'internims_url')
    debug = args.debug or config.get('nims', 'insecure')
    fail_count = 0

    while True:
        if not update(db, api_uri, site_name, site_id, ssl_cert, internims_url):
            fail_count += 1
        else:
            fail_count = 0
        if fail_count == 3:
            log.debug('InterNIMS unreachable, purging all remotes info')
            clean_remotes(db)
        time.sleep(args.sleeptime)
