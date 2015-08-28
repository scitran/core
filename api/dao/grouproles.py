import storage
import copy

class GroupRolesStorage(object):

    def init(self, dbc):
        self.dbc = dbc

    def get_container(self, _id):
        return super(GroupRolesStorage, storage.Storage).get_container(self, _id)

    def store_change(self, action, _id, elem_match=None, payload=None):
        if action == 'DELETE':
            return self._delete_role(_id, elem_match)
        if action == 'PUT':
            return self._update_role(_id, elem_match, payload)
        if action == 'POST':
            return self._create_role(_id, payload)
        raise ValueError('action should be one of POST, PUT, DELETE')

    def _create_role(self, _id, payload):
        return super(GroupRolesStorage, self)._delete_el(_id, 'roles', payload)

    def _delete_role(self, _id, elem_match):
        return super(GroupRolesStorage, self)._delete_el(_id, 'roles', elem_match)

    def _update_role(self, _id, elem_match, payload):
        return super(GroupRolesStorage, self)._update_el(_id, 'roles', elem_match, payload)


storage.Storage.register(GroupRolesStorage)