# @author:  Renzo Frigato

import logging
import base
import json
log = logging.getLogger('scitran.api')

class ListHandler(base.RequestHandler):

    def __init__(self, request=None, response=None):
        super(ListHandler, self).__init__(request, response)

    def get(self, *args, **kwargs):
        permchecker = kwargs.pop('permchecker')
        storage = kwargs.pop('storage')
        list_name = storage.list_name
        _id = kwargs.pop('cid', None) or kwargs.pop('_id')
        storage.load_collection(self.app.db)
        container = storage.get_container(_id)
        if container is not None:
            if not self.superuser_request and not permchecker(container, 'GET', self.uid):
                self.abort(403, 'user not authorized to get container {}'.format(self.uid))
            result = storage.apply_change('GET', _id, elem_match=kwargs)
            if result is None or result.get(list_name) is None or len(result[list_name]) == 0:
                self.abort(404, 'Element not found in list {} of collection {} {}'.format(list_name, storage.coll_name, _id))
            return result[list_name][0]
        else:
            self.abort(404, 'Element not found in list {} of collection {} {}'.format(list_name, storage.coll_name, _id))

    def post(self, *args, **kwargs):
        permchecker = kwargs.pop('permchecker')
        storage = kwargs.pop('storage')
        list_name = storage.list_name
        _id = kwargs.pop('cid', None) or kwargs.pop('_id')

        storage.load_collection(self.app.db)
        container = storage.get_container(_id)
        if container is not None:
            if not self.superuser_request and not permchecker(container, 'POST', self.uid):
                self.abort(403, 'user not authorized to create element in list')
        else:
            self.abort(404, 'Element not found in list {} of collection {} {}'.format(list_name, storage.coll_name, _id))
        payload = self.request.POST.mixed()
        payload.update(kwargs)
        result = storage.apply_change('POST', _id, payload=payload)

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not added in list {} of collection {} {}'.format(list_name, storage.coll_name, _id))

    def put(self, *args, **kwargs):
        method = 'PUT'
        permchecker = kwargs.pop('permchecker')
        storage = kwargs.pop('storage')
        list_name = storage.list_name
        _id = kwargs.pop('cid', None) or kwargs.pop('_id')

        storage.load_collection(self.app.db)
        container = storage.get_container(_id)
        if container is not None:
            if not self.superuser_request and not permchecker(container, method, self.uid):
                self.abort(403, 'user not authorized to update element in list')
        else:
            self.abort(404, 'Element not found in list {} of collection {}'.format(list_name, storage.coll_name))

        result = storage.apply_change(method, _id, elem_match = kwargs, payload = self.request.POST.mixed())

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not updated in list {} of collection {} {}'.format(list_name, storage.coll_name, _id))

    def delete(self, *args, **kwargs):
        method = 'DELETE'
        permchecker = kwargs.pop('permchecker')
        storage = kwargs.pop('storage')
        list_name = storage.list_name
        _id = kwargs.pop('cid', None) or kwargs.pop('_id')

        storage.load_collection(self.app.db)
        container = storage.get_container(_id)
        if container is not None:
            if not self.superuser_request and not permchecker(container, method, self.uid):
                self.abort(403, 'user not authorized to delete element from list')
        else:
            self.abort(404, 'Element not found in list {} of collection {}'.format(list_name, storage.coll_name))

        result = storage.apply_change(method, _id, elem_match = kwargs)

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not removed from list {} in collection {} {}'.format(list_name, storage.coll_name, _id))
