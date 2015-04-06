# @author:  Kevin S Hahn

"""
API request handlers for Apps.

represents the /nimsapi/apps route
"""

import os
import json
import bson
import shutil
import hashlib
import logging
import tarfile
import jsonschema

log = logging.getLogger('nimsapi.jobs')

import tempdir as tempfile

import base

# TODO: create schemas to verify various json payloads
APP_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'App',
    'type': 'object',
    'properties': {
        '_id': {
            'title': 'ID',
            'type': 'string',
        },
        'entrypoint': {  # MR SPECIFIC!!!
            'title': 'Entrypoint',
            'type': 'string',
        },
        'outputs': {
            'title': 'Outputs',
            'type': 'array',
        },
        'default': {  # MR SPECIFIC!!!
            'title': 'Default Application',
            'type': 'boolean',
        },
        'app_type': {
            'title': 'App Type',
            'type': 'string',
        },
        'inputs': {
            'title': 'Inputs',
            'type': 'array',
        },
    },
    'required': ['_id', 'entrypoint', 'outputs', 'default', 'app_type', 'inputs'],
    'additionalProperties': True
}

# TODO: apps should be stored separately from the datasets
# possible in something similar to 'quarantine', or at a whole different
# location.  this should also be configurable.
class Apps(base.RequestHandler):

    """Return information about the all the apps."""

    def get(self):
        return list(self.app.db.apps.find())

    def count(self):
        return self.app.db.apps.count()

    def post(self):
        """Create a new App."""
        # if self.public_request:  # TODO: how to handle auth during bootstrap?
        #     self.abort(403, 'must be logged in to upload apps')
        apps_path = self.app.config['apps_path']
        app_meta = None
        with tempfile.TemporaryDirectory(prefix='.tmp', dir=apps_path) as tempdir_path:
            hash_ = hashlib.sha1()
            app_temp = os.path.join(tempdir_path, 'temp')
            with open(app_temp, 'wb') as fd:
                for chunk in iter(lambda: self.request.body_file.read(2**20), ''):
                    hash_.update(chunk)
                    fd.write(chunk)
            if hash_.hexdigest() != self.request.headers['Content-MD5']:
                self.abort(400, 'Content-MD5 mismatch.')  # sha1
            if not tarfile.is_tarfile(app_temp):
                self.abort(415, 'Only tar files are accepted.')
            with tarfile.open(app_temp) as tf:
                for ti in tf:
                    if ti.name.endswith('description.json'):
                        app_meta = json.load(tf.extractfile(ti))
                        break
            if not app_meta:
                self.abort(415, 'application tar does not contain description.json')
            try:
                jsonschema.validate(app_meta, APP_SCHEMA)
            except (ValueError, jsonschema.ValidationError) as e:
                self.abort(400, str(e))
            name, version = app_meta.get('_id').split(':')
            app_dir = os.path.join(apps_path, name)
            app_tar = os.path.join(app_dir, '%s-%s.tar' % (name, version))
            if not os.path.exists(app_dir):
                os.makedirs(app_dir)
            shutil.move(app_temp, app_tar)
            app_meta.update({'asset_url': 'apps/%s/%s' % (name, version)})
            app_info = self.app.db.apps.find_and_modify(app_meta.get('_id'), app_meta, new=True, upsert=True)
            log.debug('Recieved App: %s' % app_info.get('_id'))


class App(base.RequestHandler):

    def get(self, _id):
        # TODO: auth? should viewing apps be restricted?
        return self.app.db.apps.find_one({'_id': _id})

    def get_file(self, _id):
        if self.public_request:  # this will most often be a drone request
            self.abort(403, 'must be logged in to download apps')
        name, version = _id.split(':')
        fn = '%s-%s.tar' % (name, version)
        fp = os.path.join(self.app.config['apps_path'], name, fn)
        self.response.app_iter = open(fp, 'rb')
        self.response.headers['Content-Length'] = str(os.path.getsize(fp))  # must be set after setting app_iter
        self.response.headers['Content-Type'] = 'application/octet-stream'
        self.response.headers['Content-Disposition'] = 'attachment; filename=%s' % fn
