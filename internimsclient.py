#!/usr/bin/env python
#
# @author:  Gunnar Schaefer, Kevin S. Hahn

import json
import base64
import datetime
import requests
import Crypto.Hash.SHA
import Crypto.PublicKey.RSA
import Crypto.Signature.PKCS1_v1_5

import logging
import logging.config
log = logging.getLogger('internims')
logging.getLogger('requests').setLevel(logging.WARNING)


def update(db, api_uri, site_id, privkey, internims_url):
    """sends is-alive signal to internims central."""
    db.remotes.ensure_index('timestamp', expireAfterSeconds=120)

    exp_userlist = [e['permissions'] for e in db.experiments.find(None, {'_id': False, 'permissions.uid': True})]
    col_userlist = [c['permissions'] for c in db.collections.find(None, {'_id': False, 'permissions.uid': True})]
    remote_users = list(set([user['uid'] for container in exp_userlist+col_userlist for user in container if '#' in user['uid']]))

    payload = json.dumps({'iid': site_id, 'api_uri': api_uri, 'users': remote_users})
    h = Crypto.Hash.SHA.new(payload)
    signature = Crypto.Signature.PKCS1_v1_5.new(privkey).sign(h)
    headers = {'Authorization': base64.b64encode(signature)}

    r = requests.post(url=internims_url, data=payload, headers=headers, verify=True)
    if r.status_code == 200:
        response = (json.loads(r.content))
        # update remotes entries
        for site in response['sites']:
            #FIXME
            site['timestamp'] = datetime.datetime.strptime(site['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
            db.remotes.update({'_id': site['_id']}, site, upsert=True)
        log.debug('updating remotes: ' + ', '.join((r['_id'] for r in response['sites'])))

        # delete remotes from users, who no longer have remotes
        db.users.update({'remotes': {'$exists':True}, 'uid': {'$nin': response['users'].keys()}}, {'$unset': {'remotes': ''}}, multi=True)

        # add remotes to users
        log.debug('users w/ remotes: ' + ', '.join(response['users']))
        for uid, remotes in response['users'].iteritems():
            db.users.update({'uid': uid}, {'$set': {'remotes': remotes}})
    else:
        log.warning((r.status_code, r.reason))


if __name__ == '__main__':
    import os
    import sys
    import time
    import pymongo
    import argparse
    import ConfigParser

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('configfile',  help='path to configuration file')
    arg_parser.add_argument('--internims_url', help='https://internims.appspot.com')
    arg_parser.add_argument('--db_uri', help='DB URI')
    arg_parser.add_argument('--api_uri', help='API URL, without http:// or https://')
    arg_parser.add_argument('--site_id', help='instance ID')
    arg_parser.add_argument('--sleeptime', default=60, type=int, help='time to sleep between is alive signals')
    arg_parser.add_argument('--ssl_key', help='path to privkey file')
    args = arg_parser.parse_args()

    config = ConfigParser.ConfigParser({'here': os.path.dirname(os.path.abspath(args.configfile))})
    config.read(args.configfile)
    logging.config.fileConfig(args.configfile, disable_existing_loggers=False)

    privkey_file = args.ssl_key or config.get('nims', 'ssl_key')
    if privkey_file:
        try:
            privkey = Crypto.PublicKey.RSA.importKey(open(privkey_file).read())
        except:
            log.warn(privkey_file + ' is not a valid private SSL key file, bailing out.')
            sys.exit(1)
        else:
            log.info('successfully loaded private SSL key from ' + privkey_file)
    else:
        log.warn('private SSL key not specified, bailing out.')
        sys.exit(1)

    db_uri = args.db_uri or config.get('nims', 'db_uri')
    db = (pymongo.MongoReplicaSetClient(db_uri) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri)).get_default_database()

    site_id = args.site_id or config.get('nims', 'site_id')
    api_uri = args.api_uri or config.get('nims', 'api_uri')
    internims_url = args.internims_url or config.get('nims', 'internims_url')

    while True:
        update(db, api_uri, site_id, privkey, internims_url)
        time.sleep(args.sleeptime)
