import storage

class NotesStorage(object):

    def init(self, dbc):
        self.dbc = dbc

    def get_container(self, _id):
        return super(NotesStorage, storage.Storage).get_container(self, _id)

    def store_change(self, action, **kwargs):
        if action == 'DELETE':
            return self._delete_note(**kwargs)
        if action == 'PUT':
            return self._update_note(**kwargs)
        if action == 'POST':
            return self._create_note(**kwargs)
        raise ValueError('action should be one of POST, PUT, DELETE')

    def _create_note(self, _id=None, **kwargs):
        _id = _id or bson.objectid.ObjectId()
        query = {'_id': _id, 'notes': {'$not': {'$elemMatch': kwargs} } }
        update = {'$push': {'notes': payload} }
        return dbc.update_one(query, update)

    def _delete_note(self, _id, **kwargs):
        query = {'_id': _id}
        update = {'$pull': {'notes': payload} }
        return dbc.update_one(query, update)


    def _update_note(self, _id, **kwargs):
        for k,v in kwargs.items():
            mod_payload['notes.$.' + k] = v

        query = {'_id': _id, 'notes': {'$elemMatch': kwargs} }

        update = {
            '$set': mod_payload
        }

        return dbc.update_one(query, update)

storage.Storage.register(NotesStorage)