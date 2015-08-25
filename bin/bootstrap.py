#!/usr/bin/env python
#
# @author:  Gunnar Schaefer
"""This script helps bootstrap data"""

import os
import json
import hashlib
import logging
import pymongo
import argparse
import datetime

from api import util  # from scitran.api import util

log = logging.getLogger('scitran.api.bootstrap')


def dbinit(args):
    db = pymongo.MongoClient(args.db_uri).get_default_database()
    now = datetime.datetime.utcnow()

    if args.force:
        db.client.drop_database(db)

    db.projects.create_index([('gid', 1), ('name', 1)])
    db.sessions.create_index('project')
    db.sessions.create_index('uid')
    db.acquisitions.create_index('session')
    db.acquisitions.create_index('uid')
    db.acquisitions.create_index('collections')
    db.authtokens.create_index('timestamp', expireAfterSeconds=600)
    db.uploads.create_index('timestamp', expireAfterSeconds=60)
    db.downloads.create_index('timestamp', expireAfterSeconds=60)
    # TODO jobs indexes
    # TODO review all indexes

    if args.json:
        with open(args.json) as json_dump:
            input_data = json.load(json_dump)
        for u in input_data.get('users', []):
            u['created'] = now
            u['modified'] = now
            u.setdefault('preferences', {})
            u.setdefault('avatar', 'https://gravatar.com/avatar/' + hashlib.md5(u['email']).hexdigest() + '?s=512&d=mm')
            db.users.insert(u)
        for g in input_data.get('groups', []):
            g['created'] = now
            g['modified'] = now
            db.groups.insert(g)
        for d in input_data.get('drones', []):
            d['created'] = now
            d['modified'] = now
            db.drones.insert(d)

    db.groups.update({'_id': 'unknown'}, {'$setOnInsert': {
            'created': now,
            'modified': now,
            'name': 'Unknown',
            'roles': [],
            }}, upsert=True)

dbinit_desc = """
example:
./bin/bootstrap.py dbinit mongodb://cnifs.stanford.edu/nims?replicaSet=cni -j nims_users_and_groups.json
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
./bin/bootstrap.py sort mongodb://localhost/nims /tmp/data /tmp/sorted
"""


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(help='operation to perform')

dbinit_parser = subparsers.add_parser(
        name='dbinit',
        help='initialize database',
        description=dbinit_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
dbinit_parser.add_argument('-f', '--force', action='store_true', help='wipe out any existing data')
dbinit_parser.add_argument('-j', '--json', help='JSON file containing users and groups')
dbinit_parser.add_argument('db_uri', help='DB URI')
dbinit_parser.set_defaults(func=dbinit)

sort_parser = subparsers.add_parser(
        name='sort',
        help='sort all files in a dicrectory tree',
        description=sort_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
sort_parser.add_argument('-q', '--quick', action='store_true', help='omit computing of file checksums')
sort_parser.add_argument('db_uri', help='database URI')
sort_parser.add_argument('path', help='filesystem path to data')
sort_parser.add_argument('sort_path', help='filesystem path to sorted data')
sort_parser.set_defaults(func=sort)

args = parser.parse_args()
args.func(args)
