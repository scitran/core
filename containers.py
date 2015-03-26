# @author:  Gunnar Schaefer, Kevin S. Hahn

import logging
log = logging.getLogger('scitran.api')

import os
import json
import hashlib
import datetime
import jsonschema
import bson.json_util

import tempdir as tempfile

import base
import util
import users

import tempdir as tempfile

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

    def _get(self, _id, min_role=None, perm_only=False, dbc=None, dbc_name=None):
        dbc = dbc or self.dbc
        dbc_name = dbc_name or self.__class__.__name__
        container = dbc.find_one({'_id': _id}, ['permissions'] if perm_only else None)
        if not container:
            self.abort(404, 'no such ' + dbc_name)
        user_perm = util.user_perm(container['permissions'], self.uid, self.source_site)
        if self.public_request:
            if not container.get('public', False):
                self.abort(403, 'this ' + dbc_name + 'is not public')
            del container['permissions']
        elif not self.superuser_request:
            if not user_perm:
                self.abort(403, self.uid + ' does not have permissions on this ' + dbc_name)
            if min_role and users.INTEGER_ROLES[user_perm['access']] < users.INTEGER_ROLES[min_role]:
                self.abort(403, self.uid + ' does not have at least ' + min_role + ' permissions on this ' + dbc_name)
            if user_perm['access'] != 'admin': # if not admin, mask permissions of other users
                container['permissions'] = [user_perm]
        if self.request.get('paths').lower() in ('1', 'true'):
            for file_info in container['files']:
                file_info['path'] = str(_id)[-3:] + '/' + str(_id) + '/' + file_info['name'] + file_info['ext']
        container['_id'] = str(container['_id'])
        return container, user_perm

    def put(self, _id):
        json_body = self.validate_json_body(_id, ['project'])
        self._get(_id, 'admin' if 'permissions' in json_body else 'rw', perm_only=True)
        self.update_db(_id, json_body)
        return json_body

    def validate_json_body(self, _id, oid_keys=[]):
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
        self.dbc.update({'_id': _id}, {'$set': util.mongo_dict(json_body)})

    def get_file(self, cid):
        try:
            file_spec = self.request.json_body
            jsonschema.validate(file_spec, FILE_DOWNLOAD_SCHEMA)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        _id = bson.ObjectId(cid)
        container, _ = self._get(_id, 'ro')
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
        if self.request.method == 'GET':
            self.response.app_iter = open(filepath, 'rb')
            self.response.headers['Content-Length'] = str(file_info['size']) # must be set after setting app_iter
            self.response.headers['Content-Type'] = 'application/octet-stream'
            self.response.headers['Content-Disposition'] = 'attachment; filename=%s' % str(filename)
        else:
            ticket = util.download_ticket('single', filepath, filename, file_info['size'])
            tkt_id = self.app.db.downloads.insert(ticket)
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
        # TODO; revise how engine's upload their data to be compatible with the put_attachment fxn
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
        if self.request.content_type != 'multipart/form-data':  # do not accept the OTHER sort of multipart
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
            container, _ = self._get(_id, 'rw')
        else:                   # sortable user upload
            pass
            # FIXME: pre-parse file, reject if unparsable
        data_path = self.app.config['data_path']
        quarantine_path = self.app.config['quarantine_path']
        with tempfile.TemporaryDirectory(prefix='.tmp', dir=data_path) as tempdir_path:
            for file_info in metadata['files']:
                hash_ = hashlib.sha1()
                filename = file_info['name'] + file_info['ext']
                filepath = os.path.join(tempdir_path, filename)
                field_storage_obj = self.request.POST.get(filename)
                with open(filepath, 'wb') as fd:
                    for chunk in iter(lambda: field_storage_obj.file.read(2**20), ''):
                        hash_.update(chunk)
                        fd.write(chunk)
                if hash_.hexdigest() != file_info['sha1']:
                    self.abort(400, 'Content-MD5 mismatch.')
                log.info('Received    %s [%s] from %s' % (filename, util.hrsize(file_info['size']), self.request.user_agent)) # FIXME: user_agent or uid
                status, detail = util.insert_file(self.dbc, _id, file_info, filepath, file_info['sha1'], data_path, quarantine_path)
                if status != 200:
                    self.abort(status, detail)

    def put_attachment(self, cid):
        """
        Recieve a targetted user upload of an attachment.

        Attachments are different from files, in that they are not 'research ready'.  Attachments
        represent other documents that are generally not useable by the engine; documents like
        consent forms, pen/paper questionnaires, study recruiting materials, etc.

        Internally, attachments are distinguished from files because of what metadata is
        required.  Attachments really only need a 'kinds' and 'type'.  We don't expect iteration over
        an attachment in a way that would require tracking 'state'.
        """
        # TODO read self.request.body, using '------WebKitFormBoundary' as divider
        # first line is 'content-disposition' line, extract filename
        # second line is content-type, determine how to write to a file, as bytes or as string
        # third linedata_path = self.app.config['data_path'], just a separator, useless
        if self.request.content_type != 'multipart/form-data':
            self.abort(400, 'content-type must be "multipart/form-data"')
        # TODO: metadata validation
        _id = bson.ObjectId(cid)
        container, _ = self._get(_id, 'rw')
        data_path = self.app.config['data_path']
        quarantine_path = self.app.config['quarantine_path']
        hashes = []
        with tempfile.TemporaryDirectory(prefix='.tmp', dir=self.app.config['data_path']) as tempdir_path:
            # get and hash the metadata
            metahash = hashlib.sha1()
            metastr = self.request.POST.get('metadata').file.read()  # returns a string?
            metadata = json.loads(metastr)
            metahash.update(metastr)
            hashes.append({'name': 'metadata', 'sha1': metahash.hexdigest()})

            sha1s = json.loads(self.request.POST.get('sha').file.read())
            for finfo in metadata:
                fname = finfo.get('name') + finfo.get('ext')  # finfo['ext'] will always be empty
                fhash = hashlib.sha1()
                fobj = self.request.POST.get(fname).file
                filepath = os.path.join(tempdir_path, fname)
                with open(filepath, 'wb') as fd:
                    for chunk in iter(lambda: fobj.read(2**20), ''):
                        fhash.update(chunk)
                        fd.write(chunk)
                for s in sha1s:
                    if fname == s.get('name'):
                        if fhash.hexdigest() != s.get('sha1'):
                            self.abort(400, 'Content-MD5 mismatch %s vs %s' % (fhash.hexdigest(), s.get('sha1')))
                        else:
                            finfo['sha1'] = s.get('sha1')
                            status, detail = util.insert_file(self.dbc, _id, finfo, filepath, s.get('sha1'), data_path, quarantine_path, flavor='attachment')
                        if status != 200:
                            self.abort(400, 'upload failed')
                        break
                else:
                    self.abort(400, '%s is not listed in the sha1s' % fname)

    def get_attachment(self, cid):
        """Download one attachment."""
        fname = self.request.get('name')
        _id = bson.ObjectId(cid)
        container, _ = self._get(_id, 'ro')
        fpath = os.path.join(self.app.config['data_path'], str(_id)[-3:] + '/' + str(_id), fname)
        for a_info in container['attachments']:
            if (a_info['name'] + a_info['ext']) == fname:
                break
        else:
            self.abort(404, 'no such file')
        if self.request.method == 'GET':
            self.response.app_iter = open(fpath, 'rb')
            self.response.headers['Content-Length'] = str(a_info['size']) # must be set after setting app_iter
            self.response.headers['Content-Type'] = 'application/octet-stream'
        else:
            ticket = util.download_ticket('single', fpath, fname, a_info['size'])
            tkt_id = self.app.db.downloads.insert(ticket)
            return {'url': self.uri_for('download', _full=True, ticket=tkt_id)}

    def delete_attachment(self, cid):
        """Delete one attachment."""
        fname = self.request.get('name')
        _id = bson.ObjectId(cid)
        container, _ = self._get(_id, 'rw')
        fpath = os.path.join(self.app.config['data_path'], str(_id)[-3:] + '/' + str(_id), fname)
        for a_info in container['attachments']:
            if (a_info['name'] + a_info['ext']) == fname:
                break
        else:
            self.abort(404, 'no such file')

        name, ext = os.path.splitext(fname)
        success = self.dbc.update({'_id': _id, 'attachments.name': fname}, {'$pull': {'attachments': {'name': fname}}})
        if not success['updatedExisting']:
            log.info('could not remove database entry.')
        if os.path.exists(fpath):
            os.remove(fpath)
            log.info('removed file %s' % fpath)
        else:
            log.info('could not remove file, file %s does not exist' % fpath)
