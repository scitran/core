# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('nimsapi')

import bson.json_util

import nimsapiutil


class Collections(nimsapiutil.NIMSRequestHandler):

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
        [self.get_epoch(eid) for eid in epoch_ids] # ensure permissions
        cid = self.app.db.collections.insert({'curator': self.uid, 'name': name, 'permissions': [{'uid': self.uid, 'role': 'admin'}]})
        for eid in epoch_ids:
            self.app.db.epochs.update({'_id': eid}, {'$push': {'collections': cid}})

    def get(self):
        """Return the list of Collections."""
        query = {'permissions': {'$elemMatch': {'uid': self.uid, 'site': self.source_site}}} if not self.user_is_superuser else None
        projection = {'curator': 1, 'name': 1, 'notes': 1, 'permissions': {'$elemMatch': {'uid': self.uid, 'site': self.source_site}}}
        return list(self.app.db.collections.find(query, projection))

    def put(self):
        """Update many Collections."""
        self.response.write('collections put\n')


class Collection(nimsapiutil.NIMSRequestHandler):

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
                'items': nimsapiutil.NIMSRequestHandler.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'group', 'name'], #FIXME
    }

    def get(self, cid):
        """Return one Collection, conditionally with details."""
        cid = bson.ObjectId(cid)
        return self.get_collection(cid)

    def put(self, cid):
        """Update an existing Collection."""
        cid = bson.ObjectId(cid)
        self.get_collection(cid, 'admin') # ensure permissions
        add_epoch_ids = [bson.ObjectId(eid) for eid in self.request.get_all('add_epochs[]', [])]
        del_epoch_ids = [bson.ObjectId(eid) for eid in self.request.get_all('del_epochs[]', [])]
        [self.get_epoch(eid) for eid in add_epoch_ids] # ensure permissions
        [self.get_epoch(eid) for eid in del_epoch_ids] # ensure permissions
        for eid in add_epoch_ids:
            self.app.db.epochs.update({'_id': eid}, {'$addToSet': {'collections': bson.ObjectId(cid)}})
        for eid in del_epoch_ids:
            self.app.db.epochs.update({'_id': eid}, {'$pull': {'collections': bson.ObjectId(cid)}})

    def delete(self, cid):
        """Delete a Collection."""
        cid = bson.ObjectId(cid)
        self.get_collection(cid, 'admin') # ensure permissions
        self.app.db.epochs.update({'collections': cid}, {'$pull': {'collections': cid}}, multi=True)
        self.app.db.collections.remove({'_id': cid})


class Sessions(nimsapiutil.NIMSRequestHandler):

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
        cid = bson.ObjectId(cid)
        self.get_collection(cid) # ensure permissions
        aggregated_epochs = self.app.db.epochs.aggregate([
                {'$match': {'collections': cid}},
                {'$group': {'_id': '$session'}},
                ])['result']
        query = {'_id': {'$in': [agg_epoch['_id'] for agg_epoch in aggregated_epochs]}}
        projection = ['name', 'subject', 'notes']
        sessions = list(self.app.db.sessions.find(query, projection))
        for sess in sessions:
            sess['site'] = self.app.config['site_id']
        return sessions

    def put(self):
        """Update many Sessions."""
        self.response.write('sessions put\n')


class Epochs(nimsapiutil.NIMSRequestHandler):

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
        cid = bson.ObjectId(cid)
        self.get_collection(cid) # ensure permissions
        query = {'collections': cid}
        sid = self.request.get('session')
        if bson.ObjectId.is_valid(sid):
            query['session'] = bson.ObjectId(sid)
        elif sid != '':
            self.abort(400, sid + ' is not a valid ObjectId')
        projection = ['name', 'description', 'datatype', 'notes']
        return list(self.app.db.epochs.find(query, projection))

    def put(self):
        """Update many Epochs."""
        self.response.write('epochs put\n')
