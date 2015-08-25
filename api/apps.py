# @author:  Kevin S Hahn

"""
API request handlers for Apps.

represents the /apps route
"""

import os
# import json
# import hashlib
# import tarfile
# import jsonschema

# from . import tempdir as tempfile
from . import base
# from .util import log, insert_app

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
        apps_path = self.app.config.get('apps_path')
        if not apps_path:
            self.abort(503, 'GET api/apps/<id> unavailable. apps_path not defined')
        return list(self.app.db.apps.find())

    def count(self):
        apps_path = self.app.config.get('apps_path')
        if not apps_path:
            self.abort(503, 'GET api/apps/<id> unavailable. apps_path not defined')
        return self.app.db.apps.count()

    def post(self):
        """Create a new App."""
        # this handles receive and writing the file
        # but the the json validation and database is handled by util.
        """
        apps_path = self.app.config['apps_path']
        if not apps_path:
            self.abort(503, 'POST api/apps unavailable. apps_path not defined')
        if self.public_request:  # TODO: how to handle auth during bootstrap?
            self.abort(403, 'must be logged in to upload apps')

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
            insert_app(self.app.db, app_temp, apps_path, app_meta=app_meta)  # pass meta info, prevent re-reading
            log.debug('Recieved App: %s' % app_meta.get('_id'))
        """
        # XXX util.insert_app doesn't exist...?
        raise NotImplementedError


class App(base.RequestHandler):

    def get(self, _id):
        # TODO: auth? should viewing apps be restricted?
        apps_path = self.app.config.get('apps_path')
        if not apps_path:
            self.abort(503, 'GET api/apps/<id> unavailable. apps_path not defined')
        return self.app.db.apps.find_one({'_id': _id})

    def get_file(self, _id):
        apps_path = self.app.config.get('apps_path')
        if not apps_path:
            self.abort(503, 'GET api/apps/<id> unavailable. apps_path not defined')
        if self.public_request:  # this will most often be a drone request
            self.abort(403, 'must be logged in to download apps')
        name, version = _id.split(':')
        fn = '%s-%s.tar' % (name, version)
        fp = os.path.join(apps_path, name, fn)
        self.response.app_iter = open(fp, 'rb')
        self.response.headers['Content-Length'] = str(os.path.getsize(fp))  # must be set after setting app_iter
        self.response.headers['Content-Type'] = 'application/octet-stream'
        self.response.headers['Content-Disposition'] = 'attachment; filename=%s' % fn
