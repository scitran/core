import hashlib
import datetime
import requests

from .. import base
from .. import config
from .. import validators
from ..auth import userauth, always_ok, ROLES
from ..dao import containerstorage

log = config.log


class UserHandler(base.RequestHandler):

    def __init__(self, request=None, response=None):
        super(UserHandler, self).__init__(request, response)

    def get(self, _id):
        self._init_storage()
        user = self._get_user(_id)
        permchecker = userauth.default(self, user)
        projection = []
        if self.is_true('remotes'):
            projection += ['remotes']
        if self.is_true('status'):
            projection += ['status']
        result = permchecker(self.storage.exec_op)('GET', _id, projection=projection or None)
        if result is None:
            self.abort(404, 'User does not exist')
        return result

    def self(self):
        """Return details for the current User."""
        self._init_storage()
        if not self.uid:
            self.abort(400, 'no user is logged in')
        user = self.storage.exec_op('GET', self.uid)
        if not user:
            self.abort(403, 'user does not exist')
        return user

    def get_all(self):
        self._init_storage()
        permchecker = userauth.list_permission_checker(self)
        result = permchecker(self.storage.exec_op)('GET', projection={'preferences': False})
        if result is None:
            self.abort(404, 'Not found')
        return result

    def delete(self, _id):
        self._init_storage()
        user = self._get_user(_id)
        permchecker = userauth.default(self, user)
        result = permchecker(self.storage.exec_op)('DELETE', _id)
        if result.deleted_count == 1:
            return {'deleted': result.deleted_count}
        else:
            self.abort(404, 'User {} not removed'.format(_id))
        return result

    def put(self, _id):
        self._init_storage()
        user = self._get_user(_id)
        permchecker = userauth.default(self, user)
        payload = self.request.json_body
        mongo_validator = validators.mongo_from_schema_file(self, 'user.json')
        payload_validator = validators.payload_from_schema_file(self, 'user.json')
        payload_validator(payload, 'PUT')
        payload['modified'] = datetime.datetime.utcnow()
        result = mongo_validator(permchecker(self.storage.exec_op))('PUT', _id=_id, payload=payload)
        if result.modified_count == 1:
            return {'modified': result.modified_count}
        else:
            self.abort(404, 'User {} not updated'.format(_id))

    def post(self):
        self._init_storage()
        permchecker = userauth.default(self)
        payload = self.request.json_body
        mongo_validator = validators.mongo_from_schema_file(self, 'user.json')
        payload_validator = validators.payload_from_schema_file(self, 'user.json')
        payload_validator(payload, 'POST')
        payload['created'] = payload['modified'] = datetime.datetime.utcnow()
        payload['root'] = payload.get('root', False)
        payload.setdefault('email', payload['_id'])
        gravatar = 'https://gravatar.com/avatar/' + hashlib.md5(payload['email']).hexdigest() + '?s=512'
        if requests.head(gravatar, params={'d': '404'}):
            payload.setdefault('avatar', gravatar)
        payload.setdefault('avatars', {})
        payload['avatars'].setdefault('gravatar', gravatar)
        result = mongo_validator(permchecker(self.storage.exec_op))('POST', payload=payload)
        if result.acknowledged:
            return {'_id': result.inserted_id}
        else:
            self.abort(404, 'User {} not updated'.format(_id))

    def _init_storage(self):
        self.storage = containerstorage.ContainerStorage('users', use_object_id=False)

    def _get_user(self, _id):
        user = self.storage.get_container(_id)
        if user is not None:
            return user
        else:
            self.abort(404, 'user {} not found'.format(_id))

