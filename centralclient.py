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
logging.basicConfig()
log = logging.getLogger('centralclient')
logging.getLogger('requests').setLevel(logging.WARNING)  # silence Requests library logging

import re
import json
import requests


def update(db, api_uri, site_name, site_id, ssl_cert, central_url):
    """Send is-alive signal to central peer registry."""
    proj_userlist = [p['permissions'] for p in db.projects.find(None, {'_id': False, 'permissions._id': True, 'permissions.site': True})]
    col_userlist = [c['permissions'] for c in db.collections.find(None, {'_id': False, 'permissions._id': True, 'permissions.site': True})]
    grp_userlist = [g['roles'] for g in db.groups.find(None, {'_id': False, 'roles._id': True, 'roles.site': True})]
    # cannot hash on dictionary; temporarily use tuple
    remote_users = set([(user['_id'], user['site']) for container in proj_userlist+col_userlist+grp_userlist for user in container if user.get('site') is not None])
    remote_users = [{'user': user[0], 'site': user[1]} for user in remote_users]

    payload = json.dumps({'api_uri': api_uri, 'users': remote_users, 'name': site_name})
    route = '%s/%s/%s' % (central_url, 'instances', site_id)
    try:
        r = requests.put(route, data=payload, cert=ssl_cert)
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
                for _id, remotes in response['users'].iteritems():
                    db.users.update({'_id': _id}, {'$set': {'remotes': remotes}})
            if sites:
                db.sites.remove({'_id': {'$nin': [site['_id'] for site in response['sites']]}})
                [db.sites.update({'_id': site['_id']}, site, upsert=True) for site in sites]
                db.users.update(   # clean users who no longer have remotes
                        {'remotes': {'$exists': True}, '_id': {'$nin': users.keys()}},
                        {'$unset': {'remotes': ''}},
                        multi=True,
                        )
            log.info('%3d users with remote data, %3d remotes' % (
                    len([u['_id'] for u in db.users.find({'remotes': {'$exists': True}}, {'_id': True})]),
                    len([s['_id'] for s in db.sites.find({}, {'_id': True})])
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
    """Remove db.sites, and removes remotes field from all db.users."""
    log.debug('removing remotes from users, and remotes collection')
    db.sites.remove({})
    db.users.update({'remotes': {'$exists': True}}, {'$unset': {'remotes': ''}}, multi=True)


if __name__ == '__main__':
    import time
    import pymongo
    import argparse

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--central_url', help='Scitran Central API URL', default='https://sdmc.scitran.io')
    arg_parser.add_argument('--db_uri', help='DB URI', required=True)
    arg_parser.add_argument('--api_uri', help='API URL, with https:// prefix', required=True)
    arg_parser.add_argument('--site_id', help='instance hostname (used as unique ID)', required=True)
    arg_parser.add_argument('--site_name', help='instance name', nargs='+', required=True)
    arg_parser.add_argument('--ssl_cert', help='path to server ssl certificate file', required=True)
    arg_parser.add_argument('--sleeptime', default=60, type=int, help='time to sleep between is alive signals')
    arg_parser.add_argument('--debug', help='enable default mode', action='store_true', default=False)
    arg_parser.add_argument('--log_level', help='log level [info]', default='info')
    args = arg_parser.parse_args()
    args.site_name = ' '.join(args.site_name) if args.site_name else None  # site_name as string

    logging.basicConfig()
    log.setLevel(getattr(logging, args.log_level.upper()))

    db = (pymongo.MongoReplicaSetClient(args.db_uri) if 'replicaSet' in args.db_uri else pymongo.MongoClient(args.db_uri)).get_default_database()

    fail_count = 0
    while True:
        if not update(db, args.api_uri, args.site_name, args.site_id, args.ssl_cert, args.central_url):
            fail_count += 1
        else:
            fail_count = 0
        if fail_count == 3:
            log.debug('scitran central unreachable, purging all remotes info')
            clean_remotes(db)
        time.sleep(args.sleeptime)
