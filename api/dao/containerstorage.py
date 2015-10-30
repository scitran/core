# @author:  Renzo Frigato

import bson.objectid
import bson.errors
import datetime
import logging
import copy
from .. import util
import re
from . import APIStorageException

log = logging.getLogger('scitran.api')

class CollectionStorage(object):
    """
    This class provides access to sublists of mongodb collection elements (called containers).
    """

    def __init__(self, coll_name, use_oid = False):
        self.coll_name = coll_name
        self.use_oid = use_oid
        # the collection is not loaded when the class is instantiated
        # this allows to instantiate the class when the db is not available
        # and load the collection later when the db is available
        self.dbc = None

    def get_container(self, _id):
        if self.dbc is None:
            raise RuntimeError('collection not initialized before calling get_container')
        #projection = {'permissions': 1, 'public': 1}
        return self._get_el(_id)

    def exec_op(self, action, _id=None, payload=None, query=None, user=None, public=False, projection=None):
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
            return self._update_el(_id, payload)
        if action == 'POST':
            return self._create_el(payload)
        raise ValueError('action should be one of GET, POST, PUT, DELETE')

    def _create_el(self, payload):
        log.warn(payload)
        return self.dbc.insert_one(payload)

    def _update_el(self, _id, payload):
        update = {
            '$set': util.mongo_dict(payload)
        }
        if self.use_oid:
            try:
                _id = bson.objectid.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        return self.dbc.update_one({'_id': _id}, update)

    def _delete_el(self, _id):
        if self.use_oid:
            try:
                _id = bson.objectid.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        return self.dbc.delete_one({'_id':_id})

    def _get_el(self, _id, projection=None):
        if self.use_oid:
            try:
                _id = bson.objectid.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        return self.dbc.find_one(_id, projection)

    def _get_all_el(self, query, user, public, projection):
        if user:
            if not query.get('permissions'):
                query['permissions'] = {'$elemMatch': user}
            else:
                query['$and'] = [{'permissions': {'$elemMatch': user}}, {'permissions': query.pop('permissions')}]
        if public:
            query['public'] = True
        log.warn(query)
        log.warn(projection)
        result = self.dbc.find(query, projection)
        r = list(result)
        log.warn(r)
        return r

