from abc import ABCMeta, abstractmethod

class AbstractStorage:
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_container(self, _id):
        return self.dbc.find_one(_id)

    @abstractmethod
    def store_change(self, action, list_, elem_match=None, payload=None):
        pass

    def _create_el(self, _id, list_, payload):
        if !isinstance(payload, str):
            elem_match = copy.deepcopy(payload)
            payload['_id'] = bson.objectid.ObjectId()
        else:
             elem_match = payload
        query = {'_id': _id, list_: {'$not': {'$elemMatch': elem_match} } }
        update = {'$push': {list_: payload} }
        return dbc.update_one(query, update)

    def _update_el(self, _id, list_, elem_match, payload):
        if isinstance(payload, str):
            mod_elem = {
                list_ + '.$': v
            }
        else:
            mod_elem = {}
            for k,v in payload.items():
                mod_elem[list_ + '.$.' + k] = v
        query = {'_id': _id, 'notes': {'$elemMatch': elem_match} }
        update = {
            '$set': mod_elem
        }
        return self.dbc.update_one(query, update)

    def _delete_el(self, _id, list_, elem_match):
        query = {'_id': _id}
        update = {'$pull': {list_: elem_match} }
        return self.dbc.update_one(query, update)
