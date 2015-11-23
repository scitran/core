import os
import copy
import pytz
import uuid
import shutil
import difflib
import hashlib
import logging
import pymongo
import zipfile
import datetime
import mimetypes
import tempdir as tempfile

logging.basicConfig(
    format='%(asctime)s %(name)16.16s %(filename)24.24s %(lineno)5d:%(levelname)4.4s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG,
)
log = logging.getLogger('scitran.api')

MIMETYPES = [
    ('.bvec', 'text', 'bvec'),
    ('.bval', 'text', 'bval'),
    ('.m', 'text', 'matlab'),
    ('.sh', 'text', 'shell'),
    ('.r', 'text', 'r'),
]
for mt in MIMETYPES:
    mimetypes.types_map.update({mt[0]: mt[1] + '/' + mt[2]})


def hrsize(size):
    if size < 1000:
        return '%d%s' % (size, 'B')
    for suffix in 'KMGTPEZY':
        size /= 1024.
        if size < 10.:
            return '%.1f%sB' % (size, suffix)
        if size < 1000.:
            return '%.0f%sB' % (size, suffix)
    return '%.0f%sB' % (size, 'Y')


def mongo_dict(d):
    def _mongo_list(d, pk=''):
        pk = pk and pk + '.'
        return sum([_mongo_list(v, pk+k) if isinstance(v, dict) else [(pk+k, v)] for k, v in d.iteritems()], [])
    return dict(_mongo_list(d))


def user_perm(permissions, _id, site=None):
    for perm in permissions:
        if perm['_id'] == _id and perm.get('site') == site:
            return perm
    else:
        return {}


def container_fileinfo(container, filename):
    for fileinfo in container.get('files', []):
        if fileinfo['filename'] == filename:
            return fileinfo
    else:
        return None


def download_ticket(ip, type_, target, filename, size):
    return {
        '_id': str(uuid.uuid4()),
        'timestamp': datetime.datetime.utcnow(),
        'ip': ip,
        'type': type_,
        'target': target,
        'filename': filename,
        'size': size,
    }


def guess_mimetype(filepath):
    """Guess MIME type based on filename."""
    mime, _ = mimetypes.guess_type(filepath)
    return mime or 'application/octet-stream'


def guess_filetype(filepath, mimetype):
    """Guess file type based on filename and MIME type."""
    type_, subtype = mimetype.split('/')
    if filepath.endswith('.nii') or filepath.endswith('.nii.gz'):
        return 'nifti'
    elif filepath.endswith('_montage.zip'):
        return 'montage'
    elif type_ == 'text' and subtype == 'plain':
        return 'text'
    else:
        return subtype
