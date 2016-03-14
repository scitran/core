import bson.errors
import bson.objectid

from .. import util
from .. import config
from . import APIStorageException

log = config.log


class ContainerStorage(object):
    """
    This class provides access to mongodb collection elements (called containers).
    It is used by ContainerHandler istances for get, create, update and delete operations on containers.
    Examples: projects, sessions, acquisitions and collections
    """

    def __init__(self, cont_name, use_object_id = False):
        self.cont_name = cont_name
        self.use_object_id = use_object_id
        self.dbc = config.db[cont_name]

    def get_container(self, _id):
        return self._get_el(_id)

    def exec_op(self, action, _id=None, payload=None, query=None, user=None, 
                public=False, projection=None, recursive=False):
        """
        Generic method to exec an operation.
        The request is dispatched to the corresponding private methods.
        """

        if action == 'GET' and _id:
            return self._get_el(_id, projection)
        if action == 'GET':
            return self._get_all_el(query, user, public, projection)
        if action == 'DELETE':
            return self._delete_el(_id)
        if action == 'PUT':
            return self._update_el(_id, payload, recursive)
        if action == 'POST':
            return self._create_el(payload)
        raise ValueError('action should be one of GET, POST, PUT, DELETE')

    def _create_el(self, payload):
        log.debug(payload)
        return self.dbc.insert_one(payload)

    def _update_el(self, _id, payload, recursive=False):
        update = {
            '$set': util.mongo_dict(payload)
        }
        if self.use_object_id:
            try:
                _id = bson.objectid.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        if recursive:
            self._propagate_changes(_id, update)
        return self.dbc.update_one({'_id': _id}, update)

    def _propagate_changes(self, _id, update):
        """
        Propagates changes down the heirarchy tree when a PUT is marked as recursive.
        """

        if self.cont_name == "projects":
            session_ids = [s['_id'] for s in config.db.sessions.find({'project': _id}, [])]
            config.db.sessions.update_many(
                {'project': _id}, update)
            config.db.acquisitions.update_many(
                {'session': {'$in': session_ids}}, update)
        elif self.cont_name == "sessions":
            config.db.acquisitions.update_many(
                {'session': _id}, update)
        else:
            raise ValueError('changes can only be propagated from project or session level')

    def _delete_el(self, _id):
        if self.use_object_id:
            try:
                _id = bson.objectid.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        return self.dbc.delete_one({'_id':_id})

    def _get_el(self, _id, projection=None):
        if self.use_object_id:
            try:
                _id = bson.objectid.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        return self.dbc.find_one(_id, projection)

    def _get_all_el(self, query, user, public, projection):
        if user:
            if query.get('permissions'):
                query['$and'] = [{'permissions': {'$elemMatch': user}}, {'permissions': query.pop('permissions')}]
            else:
                query['permissions'] = {'$elemMatch': user}
        log.debug(query)
        log.debug(projection)
        result = self.dbc.find(query, projection)
        return list(result)


class GroupStorage(ContainerStorage):

    def _create_el(self, payload):
        log.debug(payload)
        roles = payload.pop('roles')
        return self.dbc.update_one(
            {'_id': payload['_id']},
            {
                '$set': payload,
                '$setOnInsert': {'roles': roles}
            },
            upsert=True)

