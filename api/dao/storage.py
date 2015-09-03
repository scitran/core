# @author:  Renzo Frigato

from abc import ABCMeta, abstractmethod
import bson.objectid
import copy
import logging
log = logging.getLogger('scitran.api')

class ListStorage:
    __metaclass__ = ABCMeta

    def __init__(self, coll_name, list_name):
        self.coll_name = coll_name
        self.list_name = list_name
        self.dbc = None


    def load_collection(self, db):
        self.dbc = db.get_collection(self.coll_name)
        return self.dbc

    def get_container(self, _id, elem_match = None):
        if self.dbc is None:
            raise RuntimeError('collection not initialized before calling get_container')
        query = {'_id': _id}
        if elem_match is not None:
            query[self.list_name] = {'$elemMatch': elem_match}
        log.error('query {}'.format(query))
        return self.dbc.find_one(query)

    @abstractmethod
    def apply_change(self, action, _id, elem_match=None, payload=None):
        pass

    def _create_el(self, _id, payload):
        log.debug('payload {}'.format(payload))
        if isinstance(payload, dict) and payload.get('_id') is None:
            elem_match = copy.deepcopy(payload)
            payload['_id'] = bson.objectid.ObjectId()
        else:
             elem_match = payload
        query = {'_id': _id, self.list_name: {'$not': {'$elemMatch': elem_match} } }
        update = {'$push': {self.list_name: payload} }
        log.debug('query {}'.format(query))
        log.debug('update {}'.format(update))
        return self.dbc.update_one(query, update)

    def _update_el(self, _id, elem_match, payload):
        log.debug('elem_match {}'.format(payload))
        log.debug('payload {}'.format(elem_match))
        if isinstance(payload, str):
            mod_elem = {
                self.list_name + '.$': v
            }
        else:
            mod_elem = {}
            for k,v in payload.items():
                mod_elem[self.list_name + '.$.' + k] = v
        query = {'_id': _id, self.list_name: {'$elemMatch': elem_match} }
        update = {
            '$set': mod_elem
        }
        log.debug('query {}'.format(query))
        log.debug('update {}'.format(update))
        return self.dbc.update_one(query, update)

    def _delete_el(self, _id, elem_match):
        log.debug('elem_match {}'.format(payload))
        query = {'_id': _id}
        update = {'$pull': {self.list_name: elem_match} }
        log.debug('query {}'.format(query))
        log.debug('update {}'.format(update))
        return self.dbc.update_one(query, update)


class ObjectListStorage(ListStorage):

    def apply_change(self, action, _id, elem_match=None, payload=None):
        if action == 'DELETE':
            return super(ObjectListStorage, self)._delete_el(_id, elem_match)
        if action == 'PUT':
            return super(ObjectListStorage, self)._update_el(_id, elem_match, payload)
        if action == 'POST':
            return super(ObjectListStorage, self)._create_el(_id, payload)
        raise ValueError('action should be one of POST, PUT, DELETE')



class StringListStorage(ListStorage):

    def __init__(self, coll_name, list_name, key_name):
        super(StringListStorage, self).__init__(coll_name, list_name)
        self.key_name = key_name

    def apply_change(self, action, _id, elem_match=None, payload=None):
        if action == 'DELETE':
            return super(StringListStorage, self)._delete_el(_id, elem_match[self.key_name])
        if action == 'PUT':
            return super(StringListStorage, self)._update_el(_id, elem_match[self.key_name], payload[self.key_name])
        if action == 'POST':
            return super(StringListStorage, self)._create_el(_id, payload[self.key_name])
        raise ValueError('action should be one of POST, PUT, DELETE')
