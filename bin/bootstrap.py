#!/usr/bin/env python

"""This script helps bootstrap users and data"""

import os
import sys
import json
import shutil
import hashlib
import logging
import zipfile
import argparse
import datetime
import requests
import requests_toolbelt

from api import validators
from api import tempdir as tempfile

logging.basicConfig(
    format='%(asctime)s %(name)16.16s %(filename)24.24s %(lineno)5d:%(levelname)4.4s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG,
)
log = logging.getLogger('scitran.bootstrap')

logging.getLogger('requests').setLevel(logging.WARNING) # silence Requests library


if 'SCITRAN_CORE_DRONE_SECRET' not in os.environ:
    log.error('SCITRAN_CORE_DRONE_SECRET not configured')
    sys.exit(1)

if 'SCITRAN_RUNTIME_HOST' not in os.environ or 'SCITRAN_RUNTIME_PORT' not in os.environ:
    log.error('SCITRAN_RUNTIME_HOST or SCITRAN_RUNTIME_PORT not configured')
    sys.exit(1)
else:
    API_URL = 'https://%s:%s/api' % (os.environ['SCITRAN_RUNTIME_HOST'], os.environ['SCITRAN_RUNTIME_PORT'])

if 'SCITRAN_PERSISTENT_PATH' in os.environ and 'SCITRAN_PERSISTENT_DATA_PATH' not in os.environ:
    os.environ['SCITRAN_PERSISTENT_DATA_PATH'] = os.path.join(os.environ['SCITRAN_PERSISTENT_PATH'], 'data')

HTTP_HEADERS = {'X-SciTran-Auth': os.environ['SCITRAN_CORE_DRONE_SECRET'], 'User-Agent': 'SciTran Drone Bootstrapper'}


def metadata_encoder(o):
    if isinstance(o, datetime.datetime):
        if o.tzinfo is None:
            o = pytz.timezone('UTC').localize(o)
        return o.isoformat()
    elif isinstance(o, datetime.tzinfo):
        return o.zone
    raise TypeError(repr(o) + ' is not JSON serializable')


def create_archive(content, arcname, metadata, outdir=None, filenames=None):
    path = (os.path.join(outdir, arcname) if outdir else content) + '.zip'
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        zf.comment = json.dumps(metadata, default=metadata_encoder)
        zf.write(content, arcname)
        for fn in filenames or os.listdir(content):
            zf.write(os.path.join(content, fn), os.path.join(arcname, fn))
    return path


def users(args):
    now = datetime.datetime.utcnow()
    with open(args.json) as json_dump:
        input_data = json.load(json_dump)
    log.info('bootstrapping users...')
    with requests.Session() as rs:
        rs.verify = not args.insecure
        rs.headers = HTTP_HEADERS
        for u in input_data.get('users', []):
            log.info('    ' + u['_id'])
            rs.post(API_URL + '/users', json=u)
    log.info('bootstrapping groups... foo')
    site_id = 'local' #config.get_item('site', 'id')
    for g in input_data.get('groups', []):
        log.info('    ' + g['_id'])
        roles = g.pop('roles')
        rs.post(API_URL + '/groups' , json=g)
        for r in roles:
            r.setdefault('site', site_id)
            rs.post(API_URL + '/groups/' + g['_id'] + '/roles' , json=r)
    log.info('bootstrapping complete')

users_desc = """
example:
./bin/bootstrap.py users users_and_groups.json
"""


def data(args):
    log.info('Inspecting  %s' % args.path)
    files = []
    schema_validator = validators.payload_from_schema_file('uploader.json')
    with requests.Session() as rs:
        rs.verify = not args.insecure
        rs.headers = HTTP_HEADERS
        for dirpath, dirnames, filenames in os.walk(args.path):
            dirnames[:] = [dn for dn in dirnames if not dn.startswith('.')] # use slice assignment to influence walk
            if not dirnames and filenames:
                for metadata_file in filenames:
                    if metadata_file.lower() == 'metadata.json':
                        filenames.remove(metadata_file)
                        break
                else:
                    metadata_file = None
                if not metadata_file:
                    log.warning('Skipping    %s: No metadata found' % dirpath)
                    continue
                with open(os.path.join(dirpath, metadata_file)) as fd:
                    try:
                        metadata = json.load(fd)
                    except ValueError:
                        log.warning('Skipping    %s: Unparsable metadata' % dirpath)
                        continue
                with tempfile.TemporaryDirectory() as tempdir:
                    log.info('Packaging   %s' % dirpath)
                    filepath = create_archive(dirpath, os.path.basename(dirpath), metadata, tempdir, filenames)
                    filename = os.path.basename(filepath)
                    metadata.setdefault('acquisition', {}).setdefault('files', [{}])[0]['name'] = filename
                    log.info('Validating  %s' % filename)
                    try:
                        schema_validator(metadata, 'POST')
                    except validators.InputValidationException:
                        log.warning('Skipping    %s: Invalid metadata' % dirpath)
                        continue
                    log.info('Uploading   %s' % filename)
                    with open(filepath, 'rb') as fd:
                        metadata_json = json.dumps(metadata, default=metadata_encoder)
                        mpe = requests_toolbelt.multipart.encoder.MultipartEncoder(fields={'metadata': metadata_json, 'file': (filename, fd)})
                        rs.post(API_URL + '/uploader', data=mpe, headers={'Content-Type': mpe.content_type})

data_desc = """
example:
./bin/bootstrap.py data /tmp/data
"""


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(help='operation to perform')

users_parser = subparsers.add_parser(
        name='users',
        help='bootstrap users and groups',
        description=users_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
users_parser.add_argument('json', help='JSON file containing users and groups')
users_parser.set_defaults(func=users)

data_parser = subparsers.add_parser(
        name='data',
        help='bootstrap files in a dicrectory tree',
        description=data_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
data_parser.add_argument('path', help='filesystem path to data')
data_parser.set_defaults(func=data)

parser.add_argument('-i', '--insecure', action='store_true', help='do not verify SSL connections')
args = parser.parse_args()

if args.insecure:
    requests.packages.urllib3.disable_warnings()

args.func(args)
