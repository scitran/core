from containerhandler import ContainerHandler



class CollectionsHandler(ContainerHandler):

    def post(self, **kwargs):
        self.config = self.container_handler_configurations['collections']
        self._init_storage()
        mongo_validator, payload_validator = self._get_validators()

        payload = self.request.json_body
        log.debug(payload)
        payload_validator(payload, 'POST')
        if payload['permissions'] is None:
            payload['permissions'] = {
                '_id': self.uid,
                '_site': self.source_site or self.app.config['site_id'],
                'access': 'admin'
            }
        result = mongo_validator(self.storage.exec_op)('POST', payload=payload)

        if result.acknowledged:
            return {'_id': result.inserted_id}
        else:
            self.abort(404, 'Element not added in collection "collections" {}'.format(_id))

    def put(self, **kwargs):
        _id = kwargs.pop('cid')
        self.config = container_handler_configurations['collections']
        self._init_storage()
        container = self._get_container(_id)
        mongo_validator, payload_validator = self._get_validators()

        payload = self.request.json_body
        payload_validator(payload, 'PUT')
        permchecker = self._get_permchecker(container)
        result = mongo_validator(permchecker(self.storage.exec_op))('PUT', _id=_id, payload=payload)

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not updated in collection {} {}'.format(storage.coll_name, _id))]


    def get_all(self):
        self.config = self.container_handler_configurations['collections']
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
        result = permchecker(self.storage.exec_op)('GET', query=query, public=public, projection=projection)
        if result is None:
            self.abort(404, 'Element not found in collection {} {}'.format(storage.coll_name, _id))
        return result

    def curators(self):
        curator_ids = list(set((c['curator'] for c in self.get_all())))
        return list(self.app.db.users.find({'_id': {'$in': curator_ids}}, ['firstname', 'lastname']))

    def get_sessions(self, cid):
        """Return the list of sessions in a collection."""

        # FIXME use storage and permission checking abstractions
        _id = bson.ObjectId(cid)
        if not self.app.db.collections.find_one({'_id': _id}):
            self.abort(404, 'no such Collection')
        agg_res = self.app.db.acquisitions.aggregate([
                {'$match': {'collections': _id}},
                {'$group': {'_id': '$session'}},
                ])
        query = {'_id': {'$in': [ar['_id'] for ar in agg_res]}}
        projection = {'label': 1, 'subject.code': 1, 'notes': 1, 'timestamp': 1, 'timezone': 1}
        projection['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site}}
        sessions = list(self.dbc.find(query, projection)) # avoid permissions checking by not using ContainerList._get()
        for sess in sessions:
            sess['subject_code'] = sess.pop('subject', {}).get('code', '') # FIXME when subject is pulled out of session
        if self.debug:
            for sess in sessions:
                sid = str(sess['_id'])
                sess['details'] = self.uri_for('session', sid, _full=True) + '?user=' + self.request.GET.get('user', '')
                sess['acquisitions'] = self.uri_for('coll_acquisitions', cid, _full=True) + '?session=%s&user=%s' % (sid, self.request.GET.get('user', ''))
        return sessions

    def get_acquisitions(self, cid, **kwargs):
        """Return the list of acquisitions in a collection."""

        # FIXME use storage and permission checking abstractions
        _id = bson.ObjectId(cid)
        if not self.app.db.collections.find_one({'_id': _id}):
            self.abort(404, 'no such Collection')
        query = {'collections': _id}
        sid = self.request.GET.get('session', '')
        if bson.ObjectId.is_valid(sid):
            query['session'] = bson.ObjectId(sid)
        elif sid != '':
            self.abort(400, sid + ' is not a valid ObjectId')
        projection = {p: 1 for p in ['label', 'description', 'modality', 'datatype', 'notes', 'timestamp', 'timezone', 'files']}
        projection['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site}}
        acquisitions = list(self.dbc.find(query, projection))
        for acq in acquisitions:
            acq.setdefault('timestamp', datetime.datetime.utcnow())
        if self.debug:
            for acq in acquisitions:
                aid = str(acq['_id'])
                acq['details'] = self.uri_for('acquisition', aid, _full=True) + '?user=' + self.request.GET.get('user', '')
        return acquisitions


