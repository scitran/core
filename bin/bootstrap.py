#!/usr/bin/env python

"""This script helps bootstrap users and data"""

import os
import sys
import json
import logging
import argparse
import datetime
import requests

logging.basicConfig(
    format='%(asctime)s %(levelname)8.8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG,
)
log = logging.getLogger('scitran.bootstrap')

logging.getLogger('requests').setLevel(logging.WARNING) # silence Requests library


def users(filepath, api_url, http_headers, insecure):
    now = datetime.datetime.utcnow()
    with open(filepath) as fd:
        input_data = json.load(fd)
    with requests.Session() as rs:
        log.info('bootstrapping users...')
        rs.verify = not insecure
        rs.headers = http_headers
        for u in input_data.get('users', []):
            r = rs.post(api_url + '/users', json=u)
            if r.ok:
                log.info('S     ' + u['_id'])
            else:
                log.error('F     ' + u['_id'])

        log.info('bootstrapping groups...')
        site_id = rs.get(api_url + '/config').json()['site']['id']
        for g in input_data.get('groups', []):
            roles = g.pop('roles')
            r = rs.post(api_url + '/groups' , json=g)
            if r.ok:
                log.info('S     ' + g['_id'])
            else:
                log.error('F     ' + g['_id'])
            for role in roles:
                role.setdefault('site', site_id)
                r = rs.post(api_url + '/groups/' + g['_id'] + '/roles' , json=role)
                if not r.ok:
                    log.error('F     ' + str(role) + ' -> ' + g['_id'])
    log.info('bootstrapping complete')


ap = argparse.ArgumentParser()
ap.description = 'Bootstrap SciTran users and groups'
ap.add_argument('url', help='API URL')
ap.add_argument('json', help='JSON file containing users and groups')
ap.add_argument('--insecure', action='store_true', help='do not verify SSL connections')
ap.add_argument('--secret', help='shared API secret')
args = ap.parse_args()

if args.insecure:
    requests.packages.urllib3.disable_warnings()

http_headers = {
    'X-SciTran-Method': 'bootstrapper',
    'X-SciTran-Name': 'Bootstrapper',
}
if args.secret:
    http_headers['X-SciTran-Auth'] = args.secret
# TODO: extend this to support oauth tokens

users(args.json, args.url, http_headers, args.insecure)
