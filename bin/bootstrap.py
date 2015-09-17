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


def users(args):
    db = pymongo.MongoClient(args.db_uri).get_default_database()
    now = datetime.datetime.utcnow()
    if args.force:
        db.client.drop_database(db)
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
        db.users.insert_one(u)
    log.info('bootstrapping groups...')
    for g in input_data.get('groups', []):
        log.info('    ' + g['_id'])
        g['created'] = now
        g['modified'] = now
        db.groups.insert_one(g)
    log.info('bootstrapping drones...')
    for d in input_data.get('drones', []):
        log.info('    ' + d['_id'])
        d['created'] = now
        d['modified'] = now
        db.drones.insert_one(d)
    log.info('bootstrapping complete')


users_desc = """
example:
./bin/bootstrap.py users mongodb://localhost/scitran users_and_groups.json
"""


def sort(args):
    quarantine_path = os.path.join(args.sort_path, 'quarantine')
    if not os.path.exists(args.sort_path):
        os.makedirs(args.sort_path)
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
        hash_ = hashlib.sha1()
        if not args.quick:
            with open(filepath, 'rb') as fd:
                for chunk in iter(lambda: fd.read(2**20), ''):
                    hash_.update(chunk)
        datainfo = util.parse_file(filepath, hash_.hexdigest())
        if datainfo is None:
            util.quarantine_file(filepath, quarantine_path)
            log.info('quarantining %s (unparsable)' % os.path.basename(filepath))
        else:
            util.commit_file(db.acquisitions, None, datainfo, filepath, args.sort_path)

sort_desc = """
example:
./bin/bootstrap.py sort mongodb://localhost/scitran /tmp/data /tmp/sorted
"""


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(help='operation to perform')

parser.add_argument('--log_level', help='log level [info]', default='info')

users_parser = subparsers.add_parser(
        name='users',
        help='bootstrap users and groups',
        description=users_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
users_parser.add_argument('-f', '--force', action='store_true', help='wipe out any existing data')
users_parser.add_argument('db_uri', help='DB URI')
users_parser.add_argument('json', help='JSON file containing users and groups')
users_parser.set_defaults(func=users)

sort_parser = subparsers.add_parser(
        name='sort',
        help='sort files in a dicrectory tree',
        description=sort_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
sort_parser.add_argument('-q', '--quick', action='store_true', help='omit computing of file checksums')
sort_parser.add_argument('db_uri', help='database URI')
sort_parser.add_argument('path', help='filesystem path to data')
sort_parser.add_argument('sort_path', help='filesystem path to sorted data')
sort_parser.set_defaults(func=sort)

args = parser.parse_args()
logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))
args.func(args)
