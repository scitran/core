# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('nimsapi')

import bson.json_util

import base


class Collections(base.RequestHandler):

    """/collections """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Collection List',
        'type': 'array',
        'items': {
            'title': 'Collection',
            'type': 'object',
            'properties': {
                '_id': {
                    'title': 'Database ID',
                },
                'site': {
                    'title': 'Site',
                    'type': 'string',
                },
                'group': {
                    'title': 'Group',
                    'type': 'string',
                },
                'name': {
                    'title': 'Name',
                    'type': 'string',
                },
                'permissions': {
                    'title': 'Permissions',
                    'type': 'object',
                },
            }
        }
    }

    def count(self):
        """Return the number of Collections."""
        if self.request.method == 'OPTIONS':
            return self.options()
        self.response.write(self.app.db.collections.count())

    def post(self):
        """Create a new Collection."""
        name = self.request.get('name') or 'innominate'
        epoch_ids = [bson.ObjectId(eid) for eid in self.request.get_all('epochs[]', [])]
        [self.get_epoch(e_id) for e_id in epoch_ids] # ensure permissions
        _id = self.app.db.collections.insert({'curator': self.uid, 'name': name, 'permissions': [{'uid': self.uid, 'role': 'admin'}]})
        for e_id in epoch_ids:
            self.app.db.epochs.update({'_id': e_id}, {'$push': {'collections': _id}})

    def get(self):
        """Return the list of Collections."""
        query = {'permissions': {'$elemMatch': {'uid': self.uid, 'site': self.source_site}}} if not self.user_is_superuser else None
        projection = {'curator': 1, 'name': 1, 'notes': 1, 'permissions': {'$elemMatch': {'uid': self.uid, 'site': self.source_site}}}
        collections = list(self.app.db.collections.find(query, projection))
        if self.debug:
            for coll in collections:
                cid = str(coll['_id'])
                coll['metadata'] = self.uri_for('collection', cid=cid, _full=True) + '?' + self.request.query_string
                coll['sessions'] = self.uri_for('vp_sessions', cid=cid, _full=True) + '?' + self.request.query_string
                coll['epochs'] = self.uri_for('vp_epochs', cid=cid, _full=True) + '?' + self.request.query_string
        return collections

    def put(self):
        """Update many Collections."""
        self.response.write('collections put\n')


class Collection(base.RequestHandler):

    """/collections/<cid> """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Collection',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'Database ID',
            },
            'site': {
                'title': 'Site',
                'type': 'string',
            },
            'group': {
                'title': 'Group',
                'type': 'string',
            },
            'name': {
                'title': 'Name',
                'type': 'string',
                'maxLength': 32,
            },
            'permissions': {
                'title': 'Permissions',
                'type': 'object',
                'minProperties': 1,
            },
            'files': {
                'title': 'Files',
                'type': 'array',
                'items': base.RequestHandler.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'group', 'name'], #FIXME
    }

    def get(self, cid):
        """Return one Collection, conditionally with details."""
        _id = bson.ObjectId(cid)
        coll = self.get_collection(_id)
        if self.debug:
            coll['sessions'] = self.uri_for('vp_sessions', cid=cid, _full=True) + '?' + self.request.query_string
        return coll

    def put(self, cid):
        """Update an existing Collection."""
        _id = bson.ObjectId(cid)
        self.get_collection(_id, 'admin') # ensure permissions
        add_epoch_ids = [bson.ObjectId(eid) for eid in self.request.get_all('add_epochs[]', [])]
        del_epoch_ids = [bson.ObjectId(eid) for eid in self.request.get_all('del_epochs[]', [])]
        [self.get_epoch(e_id) for e_id in add_epoch_ids] # ensure permissions
        [self.get_epoch(e_id) for e_id in del_epoch_ids] # ensure permissions
        for e_id in add_epoch_ids:
            self.app.db.epochs.update({'_id': e_id}, {'$addToSet': {'collections': _id}})
        for e_id in del_epoch_ids:
            self.app.db.epochs.update({'_id': e_id}, {'$pull': {'collections': _id}})

    def delete(self, cid):
        """Delete a Collection."""
        _id = bson.ObjectId(cid)
        self.get_collection(_id, 'admin') # ensure permissions
        self.app.db.epochs.update({'collections': _id}, {'$pull': {'collections': _id}}, multi=True)
        self.app.db.collections.remove({'_id': _id})


class Sessions(base.RequestHandler):

    """/collections/<cid>/sessions """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Session List',
        'type': 'array',
        'items': {
            'title': 'Session',
            'type': 'object',
            'properties': {
                '_id': {
                    'title': 'Database ID',
                },
                'name': {
                    'title': 'Session',
                    'type': 'string',
                },
                'subject': {
                    'title': 'Subject',
                    'type': 'string',
                },
                'site': {
                    'title': 'Site',
                    'type': 'string',
                },
            }
        }
    }

    def count(self):
        """Return the number of Sessions."""
        if self.request.method == 'OPTIONS':
            return self.options()
        self.response.write(self.app.db.sessions.count())

    def post(self):
        """Create a new Session"""
        self.response.write('sessions post\n')

    def get(self, cid):
        """Return the list of Session Epochs."""
        _id = bson.ObjectId(cid)
        self.get_collection(_id) # ensure permissions
        aggregated_epochs = self.app.db.epochs.aggregate([
                {'$match': {'collections': _id}},
                {'$group': {'_id': '$session'}},
                ])['result']
        query = {'_id': {'$in': [agg_epoch['_id'] for agg_epoch in aggregated_epochs]}}
        projection = ['label', 'subject', 'notes']
        sessions = list(self.app.db.sessions.find(query, projection))
        for sess in sessions:
            sess['site'] = self.app.config['site_id']
        if self.debug:
            for sess in sessions:
                sid = str(sess['_id'])
                sess['metadata'] = self.uri_for('session', sid=sid, _full=True) + '?user=' + self.request.get('user')
                sess['epochs'] = self.uri_for('vp_epochs', cid=cid, _full=True) + '?session=%s&user=%s' % (sid, self.request.get('user'))
        return sessions

    def put(self):
        """Update many Sessions."""
        self.response.write('sessions put\n')


class Epochs(base.RequestHandler):

    """/collections/<cid>/epochs """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Epoch List',
        'type': 'array',
        'items': {
            'title': 'Epoch',
            'type': 'object',
            'properties': {
                '_id': {
                    'title': 'Database ID',
                },
                'name': {
                    'title': 'Epoch',
                    'type': 'string',
                },
                'description': {
                    'title': 'Description',
                    'type': 'string',
                },
                'datatype': {
                    'title': 'Datatype',
                    'type': 'string',
                },
            }
        }
    }

    def count(self):
        """Return the number of Epochs."""
        if self.request.method == 'OPTIONS':
            return self.options()
        self.response.write(self.app.db.epochs.count())

    def post(self):
        """Create a new Epoch."""
        self.response.write('epochs post\n')

    def get(self, cid):
        """Return the list of Session Epochs."""
        _id = bson.ObjectId(cid)
        self.get_collection(_id) # ensure permissions
        query = {'collections': _id}
        sid = self.request.get('session')
        if bson.ObjectId.is_valid(sid):
            query['session'] = bson.ObjectId(sid)
        elif sid != '':
            self.abort(400, sid + ' is not a valid ObjectId')
        projection = ['label', 'description', 'datatype', 'notes']
        epochs = list(self.app.db.epochs.find(query, projection))
        if self.debug:
            for epoch in epochs:
                eid = str(epoch['_id'])
                epoch['metadata'] = self.uri_for('epoch', eid=eid, _full=True) + '?user=' + self.request.get('user')
        return epochs

    def put(self):
        """Update many Epochs."""
        self.response.write('epochs put\n')
