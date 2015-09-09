# @author:  Renzo Frigato

import logging
import base
import json
log = logging.getLogger('scitran.api')

from . import permchecker

class ListHandler(base.RequestHandler):

    def __init__(self, request=None, response=None):
        super(ListHandler, self).__init__(request, response)

    def get(self, *args, **kwargs):
        _id, perm_checker, storage = self._initialize_request(kwargs)
        list_name = storage.list_name

        result = perm_checker(storage.apply_change)('GET', _id, elem_match=kwargs)

        if result is None or result.get(list_name) is None or len(result[list_name]) == 0:
            self.abort(404, 'Element not found in list {} of collection {} {}'.format(list_name, storage.coll_name, _id))
        return result[list_name][0]

    def post(self, *args, **kwargs):
        _id, perm_checker, storage = self._initialize_request(kwargs)

        payload = self.request.POST.mixed()
        payload.update(kwargs)
        result = perm_checker(storage.apply_change)('POST', _id, payload=payload)

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not added in list {} of collection {} {}'.format(storage.list_name, storage.coll_name, _id))

    def put(self, *args, **kwargs):
        _id, perm_checker, storage = self._initialize_request(kwargs)

        result = perm_checker(storage.apply_change)('PUT', _id, elem_match = kwargs, payload = self.request.POST.mixed())

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not updated in list {} of collection {} {}'.format(storage.list_name, storage.coll_name, _id))

    def delete(self, *args, **kwargs):
        _id, perm_checker, storage = self._initialize_request(kwargs)

        result = perm_checker(storage.apply_change)('DELETE', _id, elem_match = kwargs)

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not removed from list {} in collection {} {}'.format(storage.list_name, storage.coll_name, _id))

    def _initialize_request(self, kwargs):
        perm_checker = kwargs.pop('permchecker')
        storage = kwargs.pop('storage')
        _id = kwargs.pop('cid', None) or kwargs.pop('_id')

        storage.load_collection(self.app.db)
        container = storage.get_container(_id)
        if container is not None:
            if self.superuser_request:
                perm_checker = permchecker.always_ok
            else:
                perm_checker = perm_checker(self, container)
        else:
            self.abort(404, 'Element {} not found in collection {}'.format(_id, storage.coll_name))
        return _id, perm_checker, storage

