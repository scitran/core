# @author:  Renzo Frigato

import logging
import base
import json
log = logging.getLogger('scitran.api.listhandler')

class ListHandler(base.RequestHandler):

    def __init__(self, request=None, response=None):
        super(ListHandler, self).__init__(request, response)
        self.permissions = None

    def get(self, *args, **kwargs):
        #permchecker = kwargs.pop('permchecker')
        collection = kwargs.pop('collection')
        list_ = kwargs.pop('list')
        _id = kwargs.pop('cid', None) or kwargs.pop('_id')
        query = {'_id': _id}
        projection = {}
        query[list_] = projection[list_] = {'$elemMatch': kwargs}

        dbc = self.app.db.get_collection(collection)
        result = dbc.find_one(query)

        if result is not None:
            return result[list_][0]
        else:
            self.abort(404, 'Element not found in list {} of collection {}'.format(list_, collection))

    def post(self, *args, **kwargs):
        collection = kwargs.pop('collection')
        list_ = kwargs.pop('list')
        _id = kwargs.pop('cid', None) or kwargs.pop('_id')

        payload = self.request.POST.mixed()
        payload.update(kwargs)
        query = {'_id': _id, list_: {'$not': {'$elemMatch': kwargs} } }
        update = {'$push': {list_: payload} }

        dbc = self.app.db.get_collection(collection)
        result = dbc.update_one(query, update)

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not added in list {} of collection {}'.format(list_, collection))

    def put(self, *args, **kwargs):
        collection = kwargs.pop('collection')
        list_ = kwargs.pop('list')
        _id = kwargs.pop('cid', None) or kwargs.pop('_id')
        payload = self.request.POST.mixed()
        mod_payload = {}
        for k,v in payload.items():
            mod_payload[list_ + '.$.' + k] = v

        query = {'_id': _id, list_: {'$elemMatch': kwargs} }

        update = {
            '$set': mod_payload
        }

        dbc = self.app.db.get_collection(collection)
        result = dbc.update_one(query, update)

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not added in list {} of collection {}'.format(list_, collection))

    def delete(self, *args, **kwargs):
        collection = kwargs.pop('collection')
        list_ = kwargs.pop('list')
        _id = kwargs.pop('cid', None) or kwargs.pop('_id')
        payload = self.request.POST.mixed()
        payload.update(kwargs)
        query = {'_id': _id}
        update = {'$pull': {list_: payload} }

        dbc = self.app.db.get_collection(collection)
        result = dbc.update_one(query, update)

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not added in list {} of collection {}'.format(list_, collection))
