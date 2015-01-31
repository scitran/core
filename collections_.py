# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('scitran.api')

import jsonschema
import bson.json_util

import util
import users
import containers
import sessions
import acquisitions

COLLECTION_POST_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'Collection',
    'type': 'object',
    'properties': {
        'name': {
            'title': 'Name',
            'type': 'string',
            'maxLength': 32,
        },
        'notes': {
            'title': 'Notes',
            'type': 'string',
        },
        'permissions': {
            'title': 'Permissions',
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'access': {
                        'type': 'string',
                        'enum': [role['rid'] for role in users.ROLES],
                    },
                    '_id': {
                        'type': 'string',
                    },
                },
                'required': ['access', '_id'],
                'additionalProperties': False,
            },
        },
    },
    'required': ['name'],
    'additionalProperties': False,
}

COLLECTION_PUT_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'Collection',
    'type': 'object',
    'properties': {
        'name': {
            'title': 'Name',
            'type': 'string',
            'maxLength': 32,
        },
        'notes': {
            'title': 'Notes',
            'type': 'string',
        },
        'permissions': {
            'title': 'Permissions',
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'access': {
                        'type': 'string',
                        'enum': [role['rid'] for role in users.ROLES],
                    },
                    '_id': {
                        'type': 'string',
                    },
                },
                'required': ['access', '_id'],
                'additionalProperties': False,
            },
        },
        'files': {
            'title': 'Files',
            'type': 'array',
            'items': containers.FILE_SCHEMA,
            'uniqueItems': True,
        },
        'contents': {
            'type': 'object',
            'properties': {
                'operation': {
                    'type': 'string',
                    'enum': ['add', 'remove'],
                },
                'nodes': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'level': {
                                'type': 'string',
                                'enum': ['project', 'session', 'acquisition'],
                            },
                            '_id': {
                                'type': 'string',
                                'pattern': '^[0-9a-f]{24}$',
                            },
                        },
                        'required': ['level', '_id'],
                        'additionalProperties': False,
                    },
                },
            },
            'required': ['operation', 'nodes'],
            'additionalProperties': False,
        },
    },
    'additionalProperties': False,
}

COLLECTION_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'Collection',
    'type': 'object',
    'properties': {
        '_id': {
        },
        'name': {
            'title': 'Name',
            'type': 'string',
            'maxLength': 32,
        },
        'curator': {
            'title': 'Curator',
            'type': 'string',
            'maxLength': 32,
        },
        'notes': {
            'title': 'Notes',
            'type': 'string',
        },
        'site': {
            'type': 'string',
        },
        'site_name': {
            'title': 'Site',
            'type': 'string',
        },
        'permissions': {
            'title': 'Permissions',
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'access': {
                        'type': 'string',
                        'enum': [role['rid'] for role in users.ROLES],
                    },
                    '_id': {
                        'type': 'string',
                    },
                },
                'required': ['access', '_id'],
                'additionalProperties': False,
            },
        },
        'files': {
            'title': 'Files',
            'type': 'array',
            'items': containers.FILE_SCHEMA,
            'uniqueItems': True,
        },
    },
}


class Collections(containers.ContainerList):

    """/collections """

    def __init__(self, request=None, response=None):
        super(Collections, self).__init__(request, response)
        self.dbc = self.app.db.collections

    def count(self):
        """Return the number of Collections."""
        self.response.write(self.dbc.count())

    def post(self):
        """Create a new Collection."""
        try:
            json_body = self.request.json_body
            jsonschema.validate(json_body, COLLECTION_POST_SCHEMA)
            json_body['curator'] = self.app.db.users.find_one({'_id': self.uid}, ['firstname', 'lastname'])
            return {'_id': str(self.dbc.insert(json_body))}
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))

    def get(self):
        """Return the list of Collections."""
        query = {'curator._id': self.request.get('curator')} if self.request.get('curator') else {}
        projection = {'curator': 1, 'name': 1, 'notes': 1}
        collections = self._get(query, projection, self.request.get('admin').lower() in ('1', 'true'))
        for coll in collections:
            coll['_id'] = str(coll['_id'])
        if self.public_request:
            users = {u['_id']: u for u in self.app.db.users.find()}
            for coll in collections:
                coll['curator'] = users[coll['curator']].get('firstname')
        if self.debug:
            for coll in collections:
                cid = str(coll['_id'])
                coll['details'] = self.uri_for('collection', cid, _full=True) + '?' + self.request.query_string
                coll['sessions'] = self.uri_for('coll_sessions', cid, _full=True) + '?' + self.request.query_string
                coll['acquisitions'] = self.uri_for('coll_acquisitions', cid, _full=True) + '?' + self.request.query_string
        return collections

    def curators(self):
        """Return the User's list of Project Groups."""
        return {c['curator']['_id']: c['curator'] for c in self.get()}.values()


class Collection(containers.Container):

    """/collections/<cid> """

    def __init__(self, request=None, response=None):
        super(Collection, self).__init__(request, response)
        self.dbc = self.app.db.collections
        self.json_schema = COLLECTION_SCHEMA

    def schema(self):
        method =self.request.get('method').lower()
        if method == 'get':
            return COLLECTION_SCHEMA
        elif method == 'post':
            return COLLECTION_POST_SCHEMA
        elif method == 'put':
            return COLLECTION_PUT_SCHEMA
        else:
            self.abort(404, 'no schema for method ' + method)

    def get(self, cid):
        """Return one Collection, conditionally with details."""
        _id = bson.ObjectId(cid)
        coll = self._get(_id)
        coll['_id'] = str(coll['_id'])
        if self.debug:
            coll['sessions'] = self.uri_for('coll_sessions', cid, _full=True) + '?' + self.request.query_string
            coll['acquisitions'] = self.uri_for('coll_acquisitions', cid, _full=True) + '?' + self.request.query_string
        return coll

    def put(self, cid):
        """Update an existing Collection."""
        _id = bson.ObjectId(cid)
        try:
            json_body = self.request.json_body
            jsonschema.validate(json_body, COLLECTION_PUT_SCHEMA)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        if 'permissions' in json_body:
            self._get(_id, 'admin', access_check_only=True)
        else:
            self._get(_id, 'modify', access_check_only=True)
        contents = json_body.pop('contents', None)
        if json_body:
            self.dbc.update({'_id': _id}, {'$set': util.mongo_dict(json_body)})
        if contents:
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
            self.app.db.acquisitions.update({'_id': {'$in': acq_ids}}, {operator: {'collections': _id}}, multi=True)

    def delete(self, cid):
        """Delete a Collection."""
        _id = bson.ObjectId(cid)
        self._get(_id, 'admin', access_check_only=True)
        self.app.db.acquisitions.update({'collections': _id}, {'$pull': {'collections': _id}}, multi=True)
        self.dbc.remove({'_id': _id})


class CollectionSessions(sessions.Sessions):

    """/collections/<cid>/sessions """

    def post(self):
        """Create a new Session"""
        self.response.write('sessions post\n')

    def get(self, cid):
        """Return the list of Collection Sessions."""
        _id = bson.ObjectId(cid)
        if not self.app.db.collections.find_one({'_id': _id}):
            self.abort(404, 'no such Collection')
        agg_res = self.app.db.acquisitions.aggregate([
                {'$match': {'collections': _id}},
                {'$group': {'_id': '$session'}},
                ])['result']
        query = {'_id': {'$in': [ar['_id'] for ar in agg_res]}}
        projection = {'label': 1, 'subject.code': 1, 'notes': 1}
        projection['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site}}
        sessions = list(self.dbc.find(query, projection))
        for sess in sessions:
            sess['site'] = self.app.config['site_id']
            sess['_id'] = str(sess['_id'])
        if self.debug:
            for sess in sessions:
                sid = str(sess['_id'])
                sess['details'] = self.uri_for('session', sid, _full=True) + '?user=' + self.request.get('user')
                sess['acquisitions'] = self.uri_for('coll_acquisitions', cid, _full=True) + '?session=%s&user=%s' % (sid, self.request.get('user'))
        return sessions

    def put(self):
        """Update many Sessions."""
        self.response.write('sessions put\n')


class CollectionAcquisitions(acquisitions.Acquisitions):

    """/collections/<cid>/acquisitions """

    def post(self):
        """Create a new Acquisition."""
        self.response.write('acquisitions post\n')

    def get(self, cid):
        """Return the list of Session Acquisitions."""
        _id = bson.ObjectId(cid)
        if not self.app.db.collections.find_one({'_id': _id}):
            self.abort(404, 'no such Collection')
        query = {'collections': _id}
        sid = self.request.get('session')
        if bson.ObjectId.is_valid(sid):
            query['session'] = bson.ObjectId(sid)
        elif sid != '':
            self.abort(400, sid + ' is not a valid ObjectId')
        projection = {'label': 1, 'description': 1, 'types': 1, 'notes': 1}
        projection['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site}}
        acquisitions = list(self.dbc.find(query, projection))
        for acq in acquisitions:
            acq['site'] = self.app.config['site_id']
            acq['_id'] = str(acq['_id'])
        if self.debug:
            for acq in acquisitions:
                aid = str(acq['_id'])
                acq['details'] = self.uri_for('acquisition', aid, _full=True) + '?user=' + self.request.get('user')
        return acquisitions

    def put(self):
        """Update many Acquisitions."""
        self.response.write('acquisitions put\n')
