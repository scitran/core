# @author:  Gunnar Schaefer, Kevin S. Hahn

import os
import cgi
import bson
import json
import shutil
import zipfile
import datetime
import jsonschema

import tempdir as tempfile

from . import base
from . import util
from .util import log
from . import users


FILE_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'File',
    'type': 'object',
    'properties': {
        'name': {
            'title': 'Name',
            'type': 'string',
        },
        'ext': {
            'title': 'Extension',
            'type': 'string',
        },
        'filesize': {
            'title': 'Size',
            'type': 'integer',
        },
        'sha1': {
            'title': 'SHA-1',
            'type': 'string',
        },
        'type': {
            'title': 'Type',
            'type': 'string',
        },
        'kinds': {
            'title': 'Kinds',
            'type': 'array',
        },
        'state': {
            'title': 'State',
            'type': 'array',
        },
    },
    'required': ['name', 'ext', 'filesize', 'sha1', 'type', 'kinds', 'state'],
    'additionalProperties': False
}

FILE_UPLOAD_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'Upload',
    'type': 'object',
    'properties': {
        'files': {
            'title': 'Files',
            'type': 'array',
            'items': FILE_SCHEMA,
            'uniqueItems': True,
        },
    },
    'required': ['files'],
    'additionalProperties': False,
}

FILE_ACCESS_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'File Download',
    'anyOf': [
        {
            'type': 'object',
            'properties': {
                'type': {
                    'title': 'Type',
                    'type': 'string',
                },
                'kinds': {
                    'title': 'Kinds',
                    'type': 'array',
                },
                'state': {
                    'title': 'State',
                    'type': 'array',
                },
            },
            'required': ['type', 'kinds', 'state'],
        },
        {
            'type': 'object',
            'properties': {
                'name': {
                    'title': 'Type',
                    'type': 'string',
                },
                'ext': {
                    'title': 'Type',
                    'type': 'string',
                },
            },
            'required': ['name'],
        },
    ],
}


class ContainerList(base.RequestHandler):

    def _post(self):
        try:
            json_body = self.request.json_body
            jsonschema.validate(json_body, self.post_schema)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        return json_body

    def _get(self, query, projection, admin_only=False, uid=None):
        projection = {p: 1 for p in projection + ['files', 'notes', 'timestamp', 'timezone', 'public']}
        if self.public_request:
            query['public'] = True
        else:
            if uid is not None:
                if uid != self.uid and not self.superuser_request:
                    self.abort(403, 'User ' + self.uid + ' may not see the Projects of User ' + uid)
            if not self.superuser_request or uid:
                if admin_only:
                    query['permissions'] = {'$elemMatch': {'_id': uid or self.uid, 'site': self.source_site, 'access': 'admin'}}
                else:
                    query['permissions'] = {'$elemMatch': {'_id': uid or self.uid, 'site': self.source_site}}
            if self.request.GET.get('public', '').lower() in ('1', 'true'):
                query['$or'] = [{'public': True}, {'permissions': query.pop('permissions')}]
            projection['permissions'] = {'$elemMatch': {'_id': uid or self.uid, 'site': self.source_site}}
        containers = list(self.dbc.find(query, projection))
        for container in containers:
            container.setdefault('timestamp', datetime.datetime.utcnow())
            container['attachment_count'] = len([f for f in container.get('files', []) if f.get('flavor') == 'attachment'])
        return containers


class Container(base.RequestHandler):

    def _get(self, _id, min_role=None, filename=None, perm_only=False, dbc=None, dbc_name=None):
        dbc = dbc or self.dbc
        dbc_name = dbc_name or self.__class__.__name__
        container = dbc.find_one({'_id': _id}, ['permissions'] if perm_only else None)
        if not container:
            self.abort(404, 'no such ' + dbc_name)
        user_perm = util.user_perm(container['permissions'], self.uid, self.source_site)
        if self.public_request:
            if not container.get('public', False):
                self.abort(403, 'this ' + dbc_name + ' is not public')
            del container['permissions']
        elif not self.superuser_request:
            if not container.get('public') and not user_perm:
                self.abort(403, self.uid + ' does not have permissions on this ' + dbc_name)
            if min_role and users.INTEGER_ROLES[user_perm['access']] < users.INTEGER_ROLES[min_role]:
                self.abort(403, self.uid + ' does not have at least ' + min_role + ' permissions on this ' + dbc_name)
            if not user_perm:
                container['permissions'] = []
            elif user_perm['access'] != 'admin': # if not admin, mask permissions of other users
                container['permissions'] = [user_perm]
        if self.request.GET.get('paths', '').lower() in ('1', 'true'):
            for fileinfo in container['files']:
                fileinfo['path'] = str(_id)[-3:] + '/' + str(_id) + '/' + fileinfo['filename']
        container.setdefault('timestamp', datetime.datetime.utcnow())
        return container, user_perm

    def _put(self, _id):
        json_body = self.validate_json_body(['project'])
        self._get(_id, 'admin' if 'permissions' in json_body else 'rw', perm_only=True)
        self.update_db(_id, json_body)
        return json_body

    def _delete(self, _id):
        self.dbc.delete_one({'_id': _id})
        container_path = os.path.join(self.app.config['data_path'], str(_id)[-3:] + '/' + str(_id))
        if os.path.isdir(container_path):
            log.debug('deleting ' + container_path)
            shutil.rmtree(container_path)

    def validate_json_body(self, oid_keys=[]):
        try:
            json_body = self.request.json_body
            jsonschema.validate(json_body, self.put_schema)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        if 'permissions' in json_body and json_body['permissions'] is None:
            json_body.pop('permissions')
        for key in oid_keys:
            if key in json_body:
                json_body[key] = bson.ObjectId(json_body[key])
        return json_body

    def update_db(self, _id, json_body):
        for note in json_body.get('notes', []):
            note.setdefault('author', self.uid)
            if 'timestamp' in note:
                note['timestamp'] = util.parse_timestamp(note['timestamp'])
            else:
                note['timestamp'] = datetime.datetime.utcnow()
        self.dbc.update_one({'_id': _id}, {'$set': util.mongo_dict(json_body)})
