from containerhandler import ContainerHandler

import logging
import datetime

import bson
from .. import validators
from ..auth import containerauth, always_ok
from .. import files
from ..dao import containerstorage
from .. import base
from .. import util

log = logging.getLogger('scitran.api')


class CollectionsHandler(ContainerHandler):

    container_handler_configurations = ContainerHandler.container_handler_configurations

    container_handler_configurations['collections'] = {
        'permchecker': containerauth.collection_permissions,
        'storage': containerstorage.CollectionStorage('collections', use_oid=True),
        'mongo_schema_file': 'mongo/collection.json',
        'payload_schema_file': 'input/collection.json',
        'list_projection': ['label', 'session_count', 'curator'] + ContainerHandler.default_list_projection,
    }

    def post(self, **kwargs):
        self.config = self.container_handler_configurations['collections']
        self._init_storage()
        mongo_validator, payload_validator = self._get_validators()

        payload = self.request.json_body
        log.debug(payload)
        payload_validator(payload, 'POST')
        payload['permissions'] = [{
            '_id': self.uid,
            'site': self.source_site or self.app.config['site_id'],
            'access': 'admin'
        }]
        payload['created'] = payload['modified'] = datetime.datetime.utcnow()
        result = mongo_validator(self.storage.exec_op)('POST', payload=payload)

        if result.acknowledged:
            return {'_id': result.inserted_id}
        else:
            self.abort(404, 'Element not added in collection "collections" {}'.format(_id))

    def put(self, **kwargs):
        _id = kwargs.pop('cid')
        self.config = self.container_handler_configurations['collections']
        self._init_storage()
        container = self._get_container(_id)
        mongo_validator, payload_validator = self._get_validators()

        payload = self.request.json_body or {}
        contents = payload.pop('contents', None)
        payload_validator(payload, 'PUT')
        permchecker = self._get_permchecker(container)
        payload['modified'] = str(datetime.datetime.utcnow())
        result = mongo_validator(permchecker(self.storage.exec_op))('PUT', _id=_id, payload=payload)

        if result.modified_count == 1:
            self._add_contents(contents, _id)
            return {'modified': result.modified_count}
        else:
            self.abort(404, 'Element not updated in collection {} {}'.format(storage.coll_name, _id))

    def _add_contents(self, contents, _id):
        if not contents:
            return
        acq_ids = []
        for item in contents['nodes']:
            item_id = bson.ObjectId(item['_id'])
            if item['level'] == 'project':
                sess_ids = [s['_id'] for s in self.app.db.sessions.find({'project': item_id}, [])]
                acq_ids += [a['_id'] for a in self.app.db.acquisitions.find({'session': {'$in': sess_ids}}, [])]
            elif item['level'] == 'session':
                acq_ids += [a['_id'] for a in self.app.db.acquisitions.find({'session': item_id}, [])]
            elif item['level'] == 'acquisition':
                acq_ids += [item_id]
        operator = '$addToSet' if contents['operation'] == 'add' else '$pull'
        log.info(' '.join(['collection', _id, operator, str(acq_ids)]))
        self.app.db.acquisitions.update_many({'_id': {'$in': acq_ids}}, {operator: {'collections': bson.ObjectId(_id)}})

    def delete(self, coll_name, **kwargs):
        _id = kwargs.get('cid')
        super(CollectionsHandler, self).delete(coll_name, **kwargs)
        self.app.db.acquisitions.update_many({'collections': bson.ObjectId(_id)}, {'$pull': {'collections': bson.ObjectId(_id)}})

    def get_all(self, coll_name):
        self.config = self.container_handler_configurations[coll_name]
        self._init_storage()
        public = self.request.GET.get('public', '').lower() in ('1', 'true')
        projection = {p: 1 for p in self.config['list_projection']}
        projection['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site or self.app.config['site_id']}}
        if self.superuser_request:
            permchecker = always_ok
        elif self.public_request:
            public = True
            permchecker = always_ok
        else:
            admin_only = self.request.GET.get('admin', '').lower() in ('1', 'true')
            permchecker = containerauth.list_permission_checker(self, admin_only)
        query = {}
        results = permchecker(self.storage.exec_op)('GET', query=query, public=public, projection=projection)
        if results is None:
            self.abort(404, 'Element not found in collection {} {}'.format(storage.coll_name, _id))
        if self.request.GET.get('counts', '').lower() in ('1', 'true'):
            self._add_results_counts(results)
        if self.debug:
            for coll in results:
                coll['debug'] = {}
                cid = str(coll['_id'])
                coll['debug']['details'] =  self.uri_for('coll_details', coll_name='collections', cid=cid, _full=True) + '?user=' + self.request.GET.get('user', '')
                coll['debug']['acquisitions'] = self.uri_for('coll_acq', coll_name='collections', cid=cid, _full=True) + '?user=' + self.request.GET.get('user', '')
                coll['debug']['sessions'] =     self.uri_for('coll_ses', coll_name='collections', cid=cid, _full=True) + '?user=' + self.request.GET.get('user', '')
        return results

    def _add_results_counts(self, results):
        session_counts = self.app.db.acquisitions.aggregate([
            {'$match': {'collections': {'$in': [collection['_id'] for collection in results]}}},
            {'$unwind': "$collections"},
            {'$group': {'_id': "$collections", 'sessions': {'$addToSet': "$session"}}}
            ])
        session_counts = {coll['_id']: len(coll['sessions']) for coll in session_counts}
        for coll in results:
            coll['session_count'] = session_counts.get(coll['_id'], 0)


    def curators(self):
        curator_ids = list(set((c['curator'] for c in self.get_all())))
        return list(self.app.db.users.find({'_id': {'$in': curator_ids}}, ['firstname', 'lastname']))

    def get_sessions(self, coll_name, cid):
        """Return the list of sessions in a collection."""

        # FIXME use storage and permission checking abstractions
        self.config = self.container_handler_configurations['collections']
        self._init_storage()
        _id = bson.ObjectId(cid)
        if not self.storage.dbc.find_one({'_id': _id}):
            self.abort(404, 'no such Collection')
        agg_res = self.app.db.acquisitions.aggregate([
                {'$match': {'collections': _id}},
                {'$group': {'_id': '$session'}},
                ])
        query = {'_id': {'$in': [ar['_id'] for ar in agg_res]}}
        projection = {'label': 1, 'subject.code': 1, 'notes': 1, 'timestamp': 1, 'timezone': 1}
        projection['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site}}
        log.error(query)
        log.error(projection)
        sessions = list(self.app.db.sessions.find(query, projection))
        for sess in sessions:
            sess['subject_code'] = sess.pop('subject', {}).get('code', '') # FIXME when subject is pulled out of session
        if self.debug:
            for sess in sessions:
                sess['debug'] = {}
                sid = str(sess['_id'])
                sess['debug']['details'] = self.uri_for('cont_details', coll_name='sessions', cid=sid, _full=True) + '?user=' + self.request.GET.get('user', '')
                sess['debug']['acquisitions'] = self.uri_for('coll_acq', coll_name='collections', cid=cid, _full=True) + '?session=%s&user=%s' % (sid, self.request.GET.get('user', ''))
        return sessions

    def get_acquisitions(self, cid, **kwargs):
        """Return the list of acquisitions in a collection."""

        # FIXME use storage and permission checking abstractions
        self.config = self.container_handler_configurations['collections']
        self._init_storage()
        _id = bson.ObjectId(cid)
        if not self.storage.dbc.find_one({'_id': _id}):
            self.abort(404, 'no such Collection')
        query = {'collections': _id}
        sid = self.request.GET.get('session', '')
        if bson.ObjectId.is_valid(sid):
            query['session'] = bson.ObjectId(sid)
        elif sid != '':
            self.abort(400, sid + ' is not a valid ObjectId')
        projection = {p: 1 for p in ['label', 'description', 'modality', 'datatype', 'notes', 'timestamp', 'timezone', 'files']}
        projection['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site}}
        acquisitions = list(self.app.db.acquisitions.find(query, projection))
        for acq in acquisitions:
            acq.setdefault('timestamp', datetime.datetime.utcnow())
        if self.debug:
            for acq in acquisitions:
                acq['debug'] = {}
                aid = str(acq['_id'])
                acq['debug']['details'] = self.uri_for('cont_details', coll_name='acquisitions', cid=aid, _full=True) + '?user=' + self.request.GET.get('user', '')
        return acquisitions


