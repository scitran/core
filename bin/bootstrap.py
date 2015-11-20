#!/usr/bin/env python

"""This script helps bootstrap users and data"""

import os
import json
import shutil
import hashlib
import logging
import pymongo
import zipfile
import argparse
import datetime
import requests


from api.dao import reaperutil
from api import util  # from scitran.api import util
from api import mongo
log = logging.getLogger('scitran.api.bootstrap')


def clean(db, args):
    db.client.drop_database(db)

clean_desc = """
example:
./bin/bootstrap.py clean mongodb://localhost/scitran
"""


def dbinit(db, args):
    # TODO jobs indexes
    # TODO review all indexes
    db.projects.create_index([('gid', 1), ('name', 1)])
    db.sessions.create_index('project')
    db.sessions.create_index('uid')
    db.acquisitions.create_index('session')
    db.acquisitions.create_index('uid')
    db.acquisitions.create_index('collections')
    db.authtokens.create_index('timestamp', expireAfterSeconds=600)
    db.uploads.create_index('timestamp', expireAfterSeconds=60)
    db.downloads.create_index('timestamp', expireAfterSeconds=60)

    now = datetime.datetime.utcnow()
    db.groups.update_one({'_id': 'unknown'}, {'$setOnInsert': { 'created': now, 'modified': now, 'name': 'Unknown', 'roles': []}}, upsert=True)

    db.config.update_one({'latest': True}, {'$set': {
        'site_id': args.site_id,
        'site_name': args.site_name,
        'site_url': args.site_url,
        }}, upsert=True)

    db.sites.replace_one({'_id': args.site_id}, {'name': args.site_name, 'site_url': args.site_url}, upsert=True)

dbinit_desc = """
example:
./bin/bootstrap.py dbinit mongodb://localhost/scitran local Local https://localhost/api
"""


def users(db, args):
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
    config = db.config.find_one({'latest': True})
    site_id = config.get('site_id')
    for g in input_data.get('groups', []):
        log.info('    ' + g['_id'])
        g['created'] = now
        g['modified'] = now
        for r in g['roles']:
            r.setdefault('site', site_id)
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


def data(_, args):
    if not os.path.exists(args.storage_path):
        os.makedirs(args.storage_path)
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
        log.info('loading    %s [%s] (%d/%d)' % (os.path.basename(filepath), util.hrsize(os.path.getsize(filepath)), i+1, file_cnt))
        hash_ = hashlib.sha384()
        size = os.path.getsize(filepath)
        try:
            metadata = json.loads(zipfile.ZipFile(filepath).comment)
        except ValueError as e:
            log.warning(str(e))
            continue
        container = reaperutil.create_container_hierarchy(metadata)
        with open(filepath, 'rb') as fd:
            for chunk in iter(lambda: fd.read(2**20), ''):
                hash_.update(chunk)
        destpath = os.path.join(args.storage_path, container.path)
        filename = os.path.basename(filepath)
        if not os.path.exists(destpath):
            os.makedirs(destpath)
        if args.copy:
            destpath = os.path.join(destpath, filename)
            shutil.copyfile(filepath, destpath)
        else:
            shutil.move(filepath, destpath)
        created = modified = datetime.datetime.utcnow()
        fileinfo = {
            'name': filename,
            'size': size,
            'hash': hash_.hexdigest(),
            'unprocessed': True,
            'created': created,
            'modified': modified
        }
        container.add_file(fileinfo)


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

dbinit_parser = subparsers.add_parser(
        name='dbinit',
        help='initialize database and indexes',
        description=dbinit_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
dbinit_parser.add_argument('db_uri', help='DB URI')
dbinit_parser.add_argument('site_id', help='Site ID')
dbinit_parser.add_argument('site_name', help='Site Name')
dbinit_parser.add_argument('site_url', help='Site URL')
dbinit_parser.set_defaults(func=dbinit)

users_parser = subparsers.add_parser(
        name='users',
        help='bootstrap users and groups',
        description=users_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
users_parser.add_argument('db_uri', help='DB URI')
users_parser.add_argument('json', help='JSON file containing users and groups')
users_parser.set_defaults(func=users)

data_parser = subparsers.add_parser(
        name='data',
        help='bootstrap files in a dicrectory tree',
        description=data_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
data_parser.add_argument('-q', '--quick', action='store_true', help='omit computing of file checksums')
data_parser.add_argument('-c', '--copy', action='store_true', help='copy data instead of moving it')
data_parser.add_argument('db_uri', help='database URI')
data_parser.add_argument('path', help='filesystem path to data')
data_parser.add_argument('storage_path', help='filesystem path to sorted data')
data_parser.set_defaults(func=data)

args = parser.parse_args()
mongo.configure_db(args.db_uri)
logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))
args.func(mongo.db, args)
