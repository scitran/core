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


def _upsert_user(request_session, api_url, user_doc):
    """
    Insert or update user and return requests.Response object.

    user_doc: valid user doc defined in user input schema.
    """
    get_user_resp = request_session.get(api_url + '/users/' + user_doc['_id'])
    if get_user_resp.ok:
        return request_session.put(api_url + '/users/' + user_doc['_id'], json=user_doc)
    else:
        return request_session.post(api_url + '/users', json=user_doc)

def _upsert_role(request_session, api_url, role_doc, group_id):
    """
    Insert or update group role and return requests.Response object.

    role_doc: valid permission doc defined in permission input schema.
    """
    full_role_url = "{0}/groups/{1}/roles/{2}/{3}".format(api_url, group_id, role_doc['site'], role_doc['_id'])
    get_role_resp = request_session.get(full_role_url)
    if get_role_resp.ok:
        return request_session.put(full_role_url, json=role_doc)
    else:
        return request_session.post("{0}/groups/{1}/roles".format(api_url, group_id) , json=role_doc)

def users(filepath, api_url, http_headers, insecure):
    """
    Upserts the users/groups/roles defined in filepath parameter.

    Throws requests.HTTPError if upserts fail.
    """
    now = datetime.datetime.utcnow()
    with open(filepath) as fd:
        input_data = json.load(fd)
    with requests.Session() as rs:
        log.info('bootstrapping users...')
        rs.verify = not insecure
        rs.headers = http_headers
        for u in input_data.get('users', []):
            log.info('    {0}'.format(u['_id']))
            r = _upsert_user(request_session=rs, api_url=api_url, user_doc=u)
            r.raise_for_status()

        log.info('bootstrapping groups...')
        r = rs.get(api_url + '/config')
        r.raise_for_status()
        site_id = r.json()['site']['id']
        for g in input_data.get('groups', []):
            roles = g.pop('roles')
            log.info('    {0}'.format(g['_id']))
            r = rs.post(api_url + '/groups' , json=g)
            r.raise_for_status()
            for role in roles:
                role.setdefault('site', site_id)

                r = _upsert_role(request_session=rs, api_url=api_url, role_doc=role, group_id=g['_id'])
                r.raise_for_status()
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

try:
    users(args.json, args.url, http_headers, args.insecure)
except requests.HTTPError as http_ex:
    log.error(http_ex)
    log.error("request_body={0}".format(http_ex.response.request.body))
    sys.exit(1)
except Exception as ex:
    log.error('Unexpected error:')
    log.error(ex)
    sys.exit(1)
