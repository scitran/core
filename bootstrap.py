#!/usr/bin/env python
#
# @author:  Gunnar Schaefer

import os
import bson
import json
import time
import pymongo
import hashlib
import logging
import argparse
import datetime


def connect_db(db_uri, **kwargs):
    for x in range(0, 30):
        try:
            db_client = pymongo.MongoReplicaSetClient(db_uri, **kwargs) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri, **kwargs)
        except:
            time.sleep(1)
            pass
        else:
            break
    else:
        raise Exception("Could not connect to MongoDB")
    return db_client


def rsinit(args):
    db_client = pymongo.MongoClient(args.db_uri)
    repl_conf = eval(args.config)
    db_client.admin.command('replSetInitiate', repl_conf)

rsinit_desc = """
example:
./scripts/bootstrap.py rsinit mongodb://cnifs.stanford.edu \\
"dict(_id='cni', members=[ \\
    dict(_id=0, host='cnifs.stanford.edu'), \\
    dict(_id=1, host='cnibk.stanford.edu', priority=0.5), \\
    dict(_id=2, host='cni.stanford.edu', arbiterOnly=True), \\
])"
"""


def authinit(args):
    db_client = pymongo.MongoClient(args.db_uri)
    db_client['admin'].add_user(name='admin', password=args.password, roles=['userAdminAnyDatabase'])
    db_client['nims'].add_user(name=args.username, password=args.password, roles=['readWrite', 'dbAdmin'])
    uri_parts = args.db_uri.partition('://')
    print 'You must now restart mongod with the "--auth" parameter and modify your URI as follows:'
    print '    %s%s:%s@%s' % (uri_parts[0] + uri_parts[1], args.username, args.password, uri_parts[2])

    print 'openssl rand -base64 756 > cni.key'

authinit_desc = """
example:
./scripts/bootstrap.py authinit nims secret_pw mongodb://cnifs.stanford.edu/nims?replicaSet=cni
"""


def dbinit(args):
    db_client = connect_db(args.db_uri)
    db = db_client.get_default_database()

    if args.force:
        db_client.drop_database(db)

    db.projects.create_index([('gid', 1), ('name', 1)])
    db.sessions.create_index('project')
    db.sessions.create_index('uid')
    db.acquisitions.create_index('session')
    db.acquisitions.create_index('uid')
    db.acquisitions.create_index('collections')
    db.tokens.create_index('timestamp', expireAfterSeconds=600)
    db.downloads.create_index('timestamp', expireAfterSeconds=60)
    # TODO: apps and jobs indexes (indicies?)

    if args.json:
        with open(args.json) as json_dump:
            input_data = json.load(json_dump)
        if 'users' in input_data:
            db.users.insert(input_data['users'])
        if 'groups' in input_data:
            db.groups.insert(input_data['groups'])
        if 'engines' in input_data:
            db.engines.insert(input_data['engines'])
        for u in db.users.find():
            db.users.update({'_id': u['_id']}, {'$set': {'email_hash': hashlib.md5(u['email']).hexdigest()}})

    db.groups.update({'_id': 'unknown'}, {'$set': {'_id': 'unknown'}}, upsert=True)

dbinit_desc = """
example:
./scripts/bootstrap.py dbinit mongodb://cnifs.stanford.edu/nims?replicaSet=cni -j nims_users_and_groups.json
"""


def jobsinit(args):
    """Create a job entry for every acquisition's orig dataset."""
    db_client = connect_db(args.db_uri)
    db = db_client.get_default_database()
    counter = db.jobs.count() + 1   # where to start creating jobs

    for a in db.acquisitions.find({'files.state': ['orig']}, {'files.$': 1, 'session': 1, 'series': 1, 'acquisition': 1}):
        aid = a.get('_id')
        session = db.sessions.find_one({'_id': bson.ObjectId(a.get('session'))})
        project = db.projects.find_one({'_id': bson.ObjectId(session.get('project'))})
        db.jobs.insert({
            '_id': counter,
            'group': project.get('group_id'),
            'project': project.get('_id'),
            'exam': session.get('exam'),
            'app_id': 'scitran/dcm2nii:latest',
            'inputs': [
                {
                    'url': '%s/%s/%s' % ('acquisitions', aid, 'file'),
                    'payload': {
                        'type': a['files'][0]['type'],
                        'state': a['files'][0]['state'],
                        'kinds': a['files'][0]['kinds'],
                    },
                }
            ],
            'outputs': [
                {
                    'url': '%s/%s/%s' % ('acquisitions', aid, 'file'),
                    'payload': {
                        'type': 'nifti',
                        'state': ['derived', ],
                        'kinds': a['files'][0]['kinds'],
                    },
                },
            ],
            'status': 'pending',     # queued
            'activity': None,
            'added': datetime.datetime.now(),
            'timestamp': datetime.datetime.now(),
        })
        print 'created job %d, group: %s, project %s, exam %s, %s.%s' % (counter, project.get('group_id'), project.get('_id'), session.get('exam'), a.get('series'), a.get('acquisition'))
        counter += 1

jobinit_desc = """
example:
    ./scripts/bootstrap.py jobsinit mongodb://cnifs.stanford.edu/nims?replicaSet=cni
"""


def sort(args):
    logging.basicConfig(level=logging.WARNING)
    import util
    quarantine_path = os.path.join(args.sort_path, 'quarantine')
    if not os.path.exists(args.sort_path):
        os.makedirs(args.sort_path)
    if not os.path.exists(quarantine_path):
        os.makedirs(quarantine_path)
    print 'initializing DB'
    kwargs = dict(tz_aware=True)
    db_client = connect_db(args.db_uri, **kwargs)
    db = db_client.get_default_database()
    print 'inspecting %s' % args.path
    files = []
    for dirpath, dirnames, filenames in os.walk(args.path):
        for filepath in [os.path.join(dirpath, fn) for fn in filenames if not fn.startswith('.')]:
            if not os.path.islink(filepath):
                files.append(filepath)
        dirnames[:] = [dn for dn in dirnames if not dn.startswith('.')] # need to use slice assignment to influence walk behavior
    file_cnt = len(files)
    print 'found %d files to sort (ignoring symlinks and dotfiles)' % file_cnt
    for i, filepath in enumerate(files):
        print 'sorting     %s [%s] (%d/%d)' % (os.path.basename(filepath), util.hrsize(os.path.getsize(filepath)), i+1, file_cnt)
        hash_ = hashlib.sha1()
        if not args.quick:
            with open(filepath, 'rb') as fd:
                for chunk in iter(lambda: fd.read(1048577 * hash_.block_size), ''):
                    hash_.update(chunk)
        status, detail = util.insert_file(db.acquisitions, None, None, filepath, hash_.hexdigest(), args.sort_path, quarantine_path)
        if status != 200:
            print detail

sort_desc = """
example:
./scripts/bootstrap.py sort mongodb://localhost/nims /tmp/data /tmp/sorted
"""

def dbinitsort(args):
    logging.basicConfig(level=logging.WARNING)
    dbinit(args)
    upload(args)

dbinitsort_desc = """
example:
./scripts/bootstrap.py dbinitsort mongodb://localhost/nims -j bootstrap.json /tmp/data https://example.com/api/upload
"""

def upload(args):
    import util
    import datetime
    import requests
    print 'inspecting %s' % args.path
    files = []
    for dirpath, dirnames, filenames in os.walk(args.path):
        for filepath in [os.path.join(dirpath, fn) for fn in filenames if not fn.startswith('.')]:
            if not os.path.islink(filepath):
                files.append(filepath)
        dirnames[:] = [dn for dn in dirnames if not dn.startswith('.')] # need to use slice assignment to influence walk behavior
    print 'found %d files to upload (ignoring symlinks and dotfiles)' % len(files)
    for filepath in files:
        filename = os.path.basename(filepath)
        print 'hashing     %s' % filename
        hash_ = hashlib.sha1()
        with open(filepath, 'rb') as fd:
            for chunk in iter(lambda: fd.read(1048577 * hash_.block_size), ''):
                hash_.update(chunk)
        print 'uploading   %s [%s]' % (filename, util.hrsize(os.path.getsize(filepath)))
        with open(filepath, 'rb') as fd:
            headers = {'User-Agent': 'bootstrapper', 'Content-MD5': hash_.hexdigest()}
            try:
                start = datetime.datetime.now()
                r = requests.put(args.url + '?filename=%s' % filename, data=fd, headers=headers, verify=not args.no_verify)
                upload_duration = (datetime.datetime.now() - start).total_seconds()
            except requests.exceptions.ConnectionError as e:
                print 'error       %s: %s' % (filename, e)
            else:
                if r.status_code == 200:
                    print 'success     %s [%s/s]' % (filename, util.hrsize(os.path.getsize(filepath)/upload_duration))
                else:
                    print 'failure     %s: %s %s, %s' % (filename, r.status_code, r.reason, r.text)

upload_desc = """
example:
./scripts/bootstrap.py upload /tmp/data https://example.com/upload
"""


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(help='operation to perform')

rsinit_parser = subparsers.add_parser(
        name='rsinit',
        help='initialize replication set',
        description=rsinit_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
rsinit_parser.add_argument('db_uri', help='DB URI')
rsinit_parser.add_argument('config', help='replication set config')
rsinit_parser.set_defaults(func=rsinit)

authinit_parser = subparsers.add_parser(
        name='authinit',
        help='initialize authentication',
        description=authinit_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
authinit_parser.add_argument('username', help='DB username')
authinit_parser.add_argument('password', help='DB password')
authinit_parser.add_argument('db_uri', help='DB URI')
authinit_parser.set_defaults(func=authinit)

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

jobsinit_parser = subparsers.add_parser(
        name='jobsinit',
        help='initalize jobs collection from existing acquisitions',
        description=dbinit_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
jobsinit_parser.add_argument('db_uri', help='DB URI')
jobsinit_parser.set_defaults(func=jobsinit)

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

dbinitsort_parser = subparsers.add_parser(
    name='dbinitsort',
    help='initialize database, then sort all files in a directory tree',
    description=dbinitsort_desc,
    formatter_class=argparse.RawDescriptionHelpFormatter,
    )
dbinitsort_parser.add_argument('db_uri', help='database URI')
dbinitsort_parser.add_argument('path', help='filesystem path to data')
dbinitsort_parser.add_argument('url', help='upload URL')
dbinitsort_parser.add_argument('-j', '--json', help='JSON file container users and groups')
dbinitsort_parser.add_argument('-f', '--force', action='store_true', help='wipe out any existing db data')
dbinitsort_parser.add_argument('-n', '--no_verify', help='disable SSL verification', action='store_true')
dbinitsort_parser.set_defaults(func=dbinitsort)

upload_parser = subparsers.add_parser(
        name='upload',
        help='upload all files in a directory tree',
        description=upload_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
upload_parser.add_argument('path', help='filesystem path to data')
upload_parser.add_argument('url', help='upload URL')
upload_parser.add_argument('-n', '--no_verify', help='disable SSL verification', action='store_true')
upload_parser.set_defaults(func=upload)

args = parser.parse_args()
args.func(args)
