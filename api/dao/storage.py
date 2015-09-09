# @author:  Renzo Frigato

import bson.objectid
import copy
import logging
import re
log = logging.getLogger('scitran.api')

class ListStorage(object):

    def __init__(self, coll_name, list_name, use_oid = False):
        self.coll_name = coll_name
        self.list_name = list_name
        self.use_oid = use_oid
        self.dbc = None


    def load_collection(self, db):
        self.dbc = db.get_collection(self.coll_name)
        return self.dbc

    def get_container(self, _id):
        if self.dbc is None:
            raise RuntimeError('collection not initialized before calling get_container')
        if self.use_oid:
            _id = bson.objectid.ObjectId(_id)
        query = {'_id': _id}
        log.error('query {}'.format(query))
        return self.dbc.find_one(query)

    def apply_change(self, action, _id, elem_match=None, payload=None):
        if self.use_oid:
            _id = bson.objectid.ObjectId(_id)
        if action == 'GET':
            return self._get_el(_id, elem_match)
        if action == 'DELETE':
            return self._delete_el(_id, elem_match)
        if action == 'PUT':
            return self._update_el(_id, elem_match, payload)
        if action == 'POST':
            return self._create_el(_id, payload)
        raise ValueError('action should be one of POST, PUT, DELETE')

    def _create_el(self, _id, payload):
        log.error('payload {}'.format(payload))
        if isinstance(payload, dict):
            if payload.get('_id') is None:
                elem_match = copy.deepcopy(payload)
                payload['_id'] = bson.objectid.ObjectId()
            else:
                elem_match = payload
            query = {'_id': _id, self.list_name: {'$not': {'$elemMatch': elem_match} } }
        else:
            query = {'_id': _id, self.list_name: {'$ne': payload } }
        update = {'$push': {self.list_name: payload} }
        log.debug('query {}'.format(query))
        log.debug('update {}'.format(update))
        return self.dbc.update_one(query, update)

    def _update_el(self, _id, elem_match, payload):
        log.error('elem_match {}'.format(payload))
        log.error('payload {}'.format(elem_match))
        if isinstance(payload, dict):
            mod_elem = {}
            for k,v in payload.items():
                mod_elem[self.list_name + '.$.' + k] = v
            query = {'_id': _id, self.list_name: {'$elemMatch': elem_match} }
        else:
            mod_elem = {
                self.list_name + '.$': payload
            }
            query = {'_id': _id, self.list_name: elem_match}
        update = {
            '$set': mod_elem
        }
        log.debug('query {}'.format(query))
        log.debug('update {}'.format(update))
        return self.dbc.update_one(query, update)

    def _delete_el(self, _id, elem_match):
        log.debug('elem_match {}'.format(elem_match))
        query = {'_id': _id}
        update = {'$pull': {self.list_name: elem_match} }
        log.debug('query {}'.format(query))
        log.debug('update {}'.format(update))
        return self.dbc.update_one(query, update)

    def _get_el(self, _id, elem_match = None):
        log.error('elem_match {}'.format(elem_match))
        if isinstance(elem_match, str):
            query_params = elem_match
        else:
            query_params = {'$elemMatch': elem_match}
        query = {'_id': _id, self.list_name: query_params}
        projection = {self.list_name + '.$': 1}
        log.error('query {}'.format(query))
        log.error('projection {}'.format(projection))
        return self.dbc.find_one(query, projection)


class StringListStorage(ListStorage):

    def __init__(self, coll_name, list_name, use_oid, key_name):
        super(StringListStorage, self).__init__(coll_name, list_name, use_oid)
        self.key_name = key_name

    def apply_change(self, action, _id, elem_match=None, payload=None):
        if elem_match is not None:
            elem_match = elem_match[self.key_name]
        if payload is not None:
            payload = payload[self.key_name]
        return super(StringListStorage, self).apply_change(action, _id, elem_match, payload)
