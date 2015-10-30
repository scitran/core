import logging
import datetime

from .. import validators
from ..auth import userauth, always_ok, ROLES
from ..dao import containerstorage
from .. import base
from .. import util

log = logging.getLogger('scitran.api')


class UserHandler(base.RequestHandler):

    def __init__(self, request=None, response=None):
        super(UserHandler, self).__init__(request, response)

    def get(self, _id):
        self._init_storage()
        user = self._get_user(_id)
        permchecker = userauth.default(self, user)
        projection = []
        if self.request.GET.get('remotes', '').lower() in ('1', 'true'):
            projection += ['remotes']
        if self.request.GET.get('status', '').lower() in ('1', 'true'):
            projection += ['status']
        result = permchecker(self.storage.exec_op)('GET', _id, projection=projection or None)
        if result is None:
            self.abort(404, 'User does not exist')
        return result

    def self(self):
        """Return details for the current User."""
        self._init_storage()
        user = self.storage.exec_op('GET', self.uid)
        if not user:
            self.abort(400, 'no user is logged in')
        return user

    def roles(self):
        return ROLES

    def get_all(self):
        self._init_storage()
        permchecker = userauth.list_permission_checker(handler)
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
        mongo_validator = validators.mongo_from_schema_file(self, 'mongo/user.json')
        payload_validator = validators.payload_from_schema_file(self, 'input/user.json')
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
        mongo_validator = validators.mongo_from_schema_file(self, 'mongo/user.json')
        payload_validator = validators.payload_from_schema_file(self, 'input/user.json')
        payload_validator(payload, 'POST')
        payload['created'] = payload['modified'] = datetime.datetime.utcnow()
        payload['root'] = payload.get('root', False)
        result = mongo_validator(permchecker(self.storage.exec_op))('POST', payload=payload)
        if result.acknowledged:
            return {'_id': result.inserted_id}
        else:
            self.abort(404, 'User {} not updated'.format(_id))

    def _init_storage(self):
        self.storage = containerstorage.CollectionStorage('users', use_oid=False)
        self.storage.dbc = self.app.db[self.storage.coll_name]

    def _get_user(self, _id):
        user = self.storage.get_container(_id)
        if user is not None:
            return user
        else:
            self.abort(404, 'user {} not found'.format(_id))

