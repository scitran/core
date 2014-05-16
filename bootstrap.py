#!/usr/bin/env python
#
# @author:  Gunnar Schaefer

import os
import json
import pymongo
import hashlib
import argparse


def rsinit(args):
    db_client = pymongo.MongoClient(args.uri)
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
    db_client = pymongo.MongoClient(args.uri)
    db_client['admin'].add_user(name='admin', password=args.password, roles=['userAdminAnyDatabase'])
    db_client['nims'].add_user(name=args.username, password=args.password, roles=['readWrite', 'dbAdmin'])
    uri_parts = args.uri.partition('://')
    print 'You must now restart mongod with the "--auth" parameter and modify your URI as follows:'
    print '    %s%s:%s@%s' % (uri_parts[0] + uri_parts[1], args.username, args.password, uri_parts[2])

    print 'openssl rand -base64 756 > cni.key'

authinit_desc = """
example:
./scripts/bootstrap.py authinit nims secret_pw mongodb://cnifs.stanford.edu/nims?replicaSet=cni
"""


def dbinit(args):
    db_client = pymongo.MongoReplicaSetClient(args.uri) if 'replicaSet' in args.uri else pymongo.MongoClient(args.uri)
    db = db_client.get_default_database()

    db.experiments.create_index([('group', 1), ('name', 1)])
    db.sessions.create_index('experiment')
    db.sessions.create_index('uid')
    db.sessions.create_index('exam')
    db.epochs.create_index('session')
    db.epochs.create_index('uid')

    if args.json:
        with open(args.json) as json_dump:
            input_data = json.load(json_dump)
        if 'users' in input_data:
            db.users.insert(input_data['users'])
        if 'groups' in input_data:
            db.groups.insert(input_data['groups'])
        for u in db.users.find():
            db.users.update({'_id': u['_id']}, {'$set': {'email_hash': hashlib.md5(u['email']).hexdigest()}})

dbinit_desc = """
example:
./scripts/bootstrap.py dbinit mongodb://cnifs.stanford.edu/nims?replicaSet=cni -j nims_users_and_groups.json
"""


def hrsize(size):
    if size < 1000:
        return '%d%s' % (size, 'B')
    for suffix in 'KMGTPEZY':
        size /= 1024.
        if size < 10.:
            return '%.1f%s' % (size, suffix)
        if size < 1000.:
            return '%.0f%s' % (size, suffix)
    return '%.0f%s' % (size, 'Y')


def sort(args):
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
        print 'uploading   %s [%s]' % (filename, hrsize(os.path.getsize(filepath)))
        with open(filepath, 'rb') as fd:
            headers = {'User-Agent': 'bootstrapper', 'Content-MD5': hash_.hexdigest()}
            try:
                start = datetime.datetime.now()
                r = requests.put(args.url + '?filename=%s' % filename, data=fd, headers=headers)
                upload_duration = (datetime.datetime.now() - start).total_seconds()
            except requests.exceptions.ConnectionError as e:
                print 'error       %s: %s' % (filename, e)
            else:
                if r.status_code == 200:
                    print 'success     %s [%s/s]' % (filename, hrsize(os.path.getsize(filepath)/upload_duration))
                else:
                    print 'failure     %s: %s %s, %s' % (log_info, filename, r.status_code, r.reason, r.text)

sort_desc = """
example:
./scripts/bootstrap.py sort /tmp/data
"""


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(help='operation to perform')

rsinit_parser = subparsers.add_parser(
        name='rsinit',
        help='initialize replication set',
        description=rsinit_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
rsinit_parser.add_argument('uri', help='DB URI')
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
authinit_parser.add_argument('uri', help='DB URI')
authinit_parser.set_defaults(func=authinit)

dbinit_parser = subparsers.add_parser(
        name='dbinit',
        help='initialize database',
        description=dbinit_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
dbinit_parser.add_argument('-j', '--json', help='JSON file containing users and groups')
dbinit_parser.add_argument('uri', help='DB URI')
dbinit_parser.set_defaults(func=dbinit)

sort_parser = subparsers.add_parser(
        name='sort',
        help='initialize database',
        description=sort_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
sort_parser.add_argument('path', help='filesystem path to data')
sort_parser.add_argument('url', help='upload URL')
sort_parser.set_defaults(func=sort)

args = parser.parse_args()
args.func(args)
