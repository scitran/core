#!/usr/bin/env python

"""This script helps bootstrap users and data"""

import os
import json
import hashlib
import logging
import pymongo
import argparse
import datetime
import requests

from api import util  # from scitran.api import util

log = logging.getLogger('scitran.api.bootstrap')


def clean(args):
    db = pymongo.MongoClient(args.db_uri).get_default_database()
    db.client.drop_database(db)

clean_desc = """
example:
./bin/bootstrap.py clean mongodb://localhost/scitran
"""


def users(args):
    db = pymongo.MongoClient(args.db_uri).get_default_database()
    now = datetime.datetime.utcnow()
    with open(args.json) as json_dump:
        input_data = json.load(json_dump)
    log.info('bootstrapping users...')
    for u in input_data.get('users', []):
        log.info('    ' + u['_id'])
        u['created'] = now
        u['modified'] = now
        u.setdefault('email', u['_id'])
        u.setdefault('preferences', {})
        gravatar = 'https://gravatar.com/avatar/' + hashlib.md5(u['email']).hexdigest() + '?s=512'
        if requests.head(gravatar, params={'d': '404'}):
            u.setdefault('avatar', gravatar)
        u.setdefault('avatars', {})
        u['avatars'].setdefault('gravatar', gravatar)
        db.users.update_one({'_id': u['_id']}, {'$setOnInsert': u}, upsert=True)
    log.info('bootstrapping groups...')
    for g in input_data.get('groups', []):
        log.info('    ' + g['_id'])
        g['created'] = now
        g['modified'] = now
        for r in g['roles']:
            r.setdefault('site', args.site_id)
        db.groups.update_one({'_id': g['_id']}, {'$setOnInsert': g}, upsert=True)
    log.info('bootstrapping drones...')
    for d in input_data.get('drones', []):
        log.info('    ' + d['_id'])
        d['created'] = now
        d['modified'] = now
        db.drones.update_one({'_id': d['_id']}, {'$setOnInsert': d}, upsert=True)
    log.info('bootstrapping complete')

users_desc = """
example:
./bin/bootstrap.py users mongodb://localhost/scitran users_and_groups.json
"""


def data(args):
    quarantine_path = os.path.join(args.storage_path, 'quarantine')
    if not os.path.exists(args.storage_path):
        os.makedirs(args.storage_path)
    if not os.path.exists(quarantine_path):
        os.makedirs(quarantine_path)
    log.info('initializing DB')
    db = pymongo.MongoClient(args.db_uri).get_default_database()
    log.info('inspecting %s' % args.path)
    files = []
    for dirpath, dirnames, filenames in os.walk(args.path):
        for filepath in [os.path.join(dirpath, fn) for fn in filenames if not fn.startswith('.')]:
            if not os.path.islink(filepath):
                files.append(filepath)
        dirnames[:] = [dn for dn in dirnames if not dn.startswith('.')] # need to use slice assignment to influence walk behavior
    file_cnt = len(files)
    log.info('found %d files to sort (ignoring symlinks and dotfiles)' % file_cnt)
    for i, filepath in enumerate(files):
        log.info('sorting     %s [%s] (%d/%d)' % (os.path.basename(filepath), util.hrsize(os.path.getsize(filepath)), i+1, file_cnt))
        hash_ = hashlib.sha384()
        if not args.quick:
            with open(filepath, 'rb') as fd:
                for chunk in iter(lambda: fd.read(2**20), ''):
                    hash_.update(chunk)
        datainfo = util.parse_file(filepath, hash_.hexdigest())
        if datainfo is None:
            util.quarantine_file(filepath, quarantine_path)
            log.info('quarantining %s (unparsable)' % os.path.basename(filepath))
        else:
            util.commit_file(db.acquisitions, None, datainfo, filepath, args.storage_path)

data_desc = """
example:
./bin/bootstrap.py data mongodb://localhost/scitran /tmp/data /tmp/sorted
"""


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(help='operation to perform')

parser.add_argument('--log_level', help='log level [info]', default='info')

clean_parser = subparsers.add_parser(
        name='clean',
        help='reset database to clean state',
        description=clean_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
clean_parser.add_argument('db_uri', help='DB URI')
clean_parser.set_defaults(func=clean)

users_parser = subparsers.add_parser(
        name='users',
        help='bootstrap users and groups',
        description=users_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
users_parser.add_argument('db_uri', help='DB URI')
users_parser.add_argument('json', help='JSON file containing users and groups')
users_parser.add_argument('site_id', help='Site ID')
users_parser.set_defaults(func=users)

data_parser = subparsers.add_parser(
        name='data',
        help='bootstrap files in a dicrectory tree',
        description=data_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
data_parser.add_argument('-q', '--quick', action='store_true', help='omit computing of file checksums')
data_parser.add_argument('db_uri', help='database URI')
data_parser.add_argument('path', help='filesystem path to data')
data_parser.add_argument('storage_path', help='filesystem path to sorted data')
data_parser.set_defaults(func=data)

args = parser.parse_args()
logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))
args.func(args)
