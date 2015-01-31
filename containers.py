# @author:  Gunnar Schaefer, Kevin S. Hahn

import logging
log = logging.getLogger('scitran.api')

import os
import hashlib
import datetime
import jsonschema
import bson.json_util

import base
import util
import users

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
        'size': {
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
    'required': ['name', 'ext', 'size', 'sha1', 'type', 'kinds', 'state'],
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

FILE_DOWNLOAD_SCHEMA = {
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
            'required': ['name', 'ext'],
        },
    ],
}


class ContainerList(base.RequestHandler):

    def _get(self, query, projection, admin_only=False):
        if self.public_request:
            query['public'] = True
        else:
            projection['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site}}
            if not self.superuser_request:
                if admin_only:
                    query['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site, 'access': 'admin'}}
                else:
                    query['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site}}
        containers = list(self.dbc.find(query, projection))
        for container in containers:
            container['_id'] = str(container['_id'])
        return containers


class Container(base.RequestHandler):

    def _get(self, _id, min_role=None, access_check_only=False):
        user_perm = None
        container = self.dbc.find_one({'_id': _id}, ['permissions'] if access_check_only else None)
        if not container:
            self.abort(404, 'no such ' + self.__class__.__name__)
        if self.public_request:
            if not container.get('public', False):
                self.abort(403, 'this ' + self.__class__.__name__ + 'is not public')
            del container['permissions']
        elif not self.superuser_request:
            user_perm = None
            for perm in container['permissions']:
                if perm['_id'] == self.uid and perm.get('site') == self.source_site:
                    user_perm = perm
                    break
            else:
                self.abort(403, self.uid + ' does not have permissions on this ' + self.__class__.__name__)
            if min_role and users.INTEGER_ROLES[user_perm['access']] < users.INTEGER_ROLES[min_role]:
                self.abort(403, self.uid + ' does not have at least ' + min_role + ' permissions on this ' + self.__class__.__name__)
            if user_perm['access'] != 'admin': # if not admin, mask permissions of other users
                container['permissions'] = [user_perm]
        if self.request.get('paths').lower() in ('1', 'true'):
            for file_info in container['files']:
                file_info['path'] = str(_id)[-3:] + '/' + str(_id) + '/' + file_info['name'] + file_info['ext']
        container['_id'] = str(container['_id'])
        return container

    def _put(self, _id):
        try:
            json_body = self.request.json_body
            jsonschema.validate(json_body, self.put_schema)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        if 'permissions' in json_body and json_body['permissions'] is None:
            json_body.pop('permissions')
        if 'permissions' in json_body:
            self._get(_id, 'admin', access_check_only=True)
        else:
            self._get(_id, 'modify', access_check_only=True)
        self.dbc.update({'_id': _id}, {'$set': util.mongo_dict(json_body)})
        return json_body

    def get_file(self, cid):
        try:
            file_spec = self.request.json_body
            jsonschema.validate(file_spec, FILE_DOWNLOAD_SCHEMA)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        _id = bson.ObjectId(cid)
        container = self._get(_id, 'download')
        for file_info in container['files']:
            if 'name' in file_spec:
                if file_info['name'] == file_spec['name'] and file_info['ext'] == file_spec['ext']:
                    break
            else:
                if file_info['type'] == file_spec['type'] and file_info['kinds'] == file_spec['kinds'] and file_info['state'] == file_spec['state']:
                    break
        else:
            self.abort(404, 'no such file')
        filename = file_info['name'] + file_info['ext']
        filepath = os.path.join(self.app.config['data_path'], str(_id)[-3:] + '/' + str(_id), filename)
        tkt_spec = {
                '_id': str(bson.ObjectId()), # FIXME: use better ticket ID
                'timestamp': datetime.datetime.utcnow(),
                'type': 'single',
                'filepath': filepath,
                'filename': filename,
                'size': file_info['size'],
                }
        tkt_id = self.app.db.downloads.insert(tkt_spec)
        if self.request.method == 'GET':
            self.redirect_to('download', _abort=True, ticket=tkt_id)
        return {'url': self.uri_for('download', _full=True, ticket=tkt_id)}

    def put_file(self, cid=None):
        """
        Receive a targeted processor or user upload.

        Accepts a multipart request that contains json in first part, and data in second part.
        This POST route is used to add a file to an existing container, not for creating new containers.
        This upload is different from the main PUT route, because this does not update the primary
        metadata, nor does it try to determine where to place the file.  It always gets placed in
        the current container.

        """
        def receive_stream_and_validate(stream, digest, filename):
            # FIXME pull this out to also be used from core.Core.put() and also replace the duplicated code below
            hash_ = hashlib.sha1()
            filepath = os.path.join(tempdir_path, filename)
            with open(filepath, 'wb') as fd:
                for chunk in iter(lambda: stream.read(2**20), ''):
                    hash_.update(chunk)
                    fd.write(chunk)
            if hash_.hexdigest() != digest:
                self.abort(400, 'Content-MD5 mismatch.')
            return filepath

        if cid is None: # sortable user upload
            pass
        else:           # targeted upload
            pass
        if self.request.content_type != 'multipart/form-data':
            self.abort(400, 'content-type must be "multipart/form-data"')
        try:
            metadata = json.loads(self.request.get('metadata'))
            jsonschema.validate(metadata, FILE_UPLOAD_SCHEMA)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        if self.public_request: # processor upload
            _id = None
            # FIXME check that processor is legit
        elif cid is not None:   # targeted user upload
            _id = bson.ObjectId(cid)
            container = self._get(_id, 'modify')
        else:                   # sortable user upload
            pass
            # FIXME: pre-parse file, reject if unparsable
        data_path = self.app.config['data_path']
        quarantine_path = self.app.config['quarantine_path']
        with tempfile.TemporaryDirectory(prefix='.tmp', dir=data_path) as tempdir_path:
            filepaths = []
            for file_info in metadata['files']:
                hash_ = hashlib.sha1()
                filename = file_info['name'] + file_info['ext']
                filepaths.append(os.path.join(tempdir_path, filename))
                field_storage_obj = self.request.POST.get(filename)
                with open(filepaths[-1], 'wb') as fd:
                    for chunk in iter(lambda: field_storage_obj.file.read(2**20), ''):
                        hash_.update(chunk)
                        fd.write(chunk)
                if hash_.hexdigest() != file_info['sha1']:
                    self.abort(400, 'Content-MD5 mismatch.')
                log.info('Received    %s [%s] from %s' % (filename, util.hrsize(file_info['size']), self.request.user_agent)) # FIXME: user_agent or uid
            for filepath in filepaths:
                status, detail = util.insert_file(self.dbc, _id, file_info, filepath, file_info['sha1'], data_path, quarantine_path)
                if status != 200:
                    self.abort(status, detail)
