# @author:  Gunnar Schaefer

import bson
import datetime
import jsonschema

from . import users
from . import util
from . import containers
from . import sessions
from . import acquisitions

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
                    'site': {
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
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'author': {
                        'type': 'string',
                    },
                    'timestamp': {
                        'type': 'string',
                        'format': 'date-time',
                    },
                    'text': {
                        'type': 'string',
                    },
                },
                'required': ['text'],
                'additionalProperties': False,
            },
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
                    'site': {
                        'type': 'string',
                    },
                },
                'required': ['access', '_id'],
                'additionalProperties': False,
            },
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
    'minProperties': 1,
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
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))
        json_body['curator'] = self.uid
        json_body['timestamp'] = datetime.datetime.utcnow()
        json_body['permissions'] = [{'_id': self.uid, 'access': 'admin'}]
        return {'_id': self.dbc.insert_one(json_body).inserted_id}

    def get(self):
        """Return the list of Collections."""
        query = {'curator': self.request.GET.get('curator')} if self.request.GET.get('curator') else {}
        projection = ['curator', 'name']
        collections = self._get(query, projection, self.request.GET.get('admin', '').lower() in ('1', 'true'))

        session_counts = self.app.db.acquisitions.aggregate([
            {'$match': {'collections': {'$in': [collection['_id'] for collection in collections]}}},
            {'$unwind': "$collections"},
            {'$group': {'_id': "$collections", 'sessions': {'$addToSet': "$session"}}}
            ])
        # Convert to dict for easy lookup
        session_counts = {coll['_id']: len(coll['sessions']) for coll in session_counts}
        for coll in collections:
            coll['session_count'] = session_counts.get(coll['_id'], 0)
        if self.debug:
            for coll in collections:
                cid = str(coll['_id'])
                coll['details'] = self.uri_for('collection', cid, _full=True) + '?' + self.request.query_string
                coll['sessions'] = self.uri_for('coll_sessions', cid, _full=True) + '?' + self.request.query_string
                coll['acquisitions'] = self.uri_for('coll_acquisitions', cid, _full=True) + '?' + self.request.query_string
        return collections

    def curators(self):
        """Return the User's list of Collection Curators."""
        curator_ids = list(set((c['curator'] for c in self.get())))
        return list(self.app.db.users.find({'_id': {'$in': curator_ids}}, ['firstname', 'lastname']))


class Collection(containers.Container):

    """/collections/<cid> """

    put_schema = COLLECTION_PUT_SCHEMA

    def __init__(self, request=None, response=None):
        super(Collection, self).__init__(request, response)
        self.dbc = self.app.db.collections
        self.json_schema = COLLECTION_SCHEMA

    def schema(self):
        method =self.request.GET.get('method', '').upper()
        if method == 'GET':
            return COLLECTION_SCHEMA
        elif method == 'POST':
            return COLLECTION_POST_SCHEMA
        elif method == 'PUT':
            return COLLECTION_PUT_SCHEMA
        else:
            self.abort(404, 'no schema for method ' + method)

    def get(self, cid):
        """Return one Collection, conditionally with details."""
        _id = bson.ObjectId(cid)
        coll, _ = self._get(_id)
        if self.debug:
            coll['sessions'] = self.uri_for('coll_sessions', cid, _full=True) + '?' + self.request.query_string
            coll['acquisitions'] = self.uri_for('coll_acquisitions', cid, _full=True) + '?' + self.request.query_string
        return coll

    def put(self, cid):
        """Update an existing Collection."""
        _id = bson.ObjectId(cid)
        json_body = self.validate_json_body()
        self._get(_id, 'admin' if 'permissions' in json_body else 'rw', perm_only=True)
        contents = json_body.pop('contents', None)
        if json_body:
            self.update_db(_id, json_body)
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
            self.app.db.acquisitions.update_many({'_id': {'$in': acq_ids}}, {operator: {'collections': _id}})

    def delete(self, cid):
        """Delete a Collection."""
        _id = bson.ObjectId(cid)
        self._get(_id, 'admin', perm_only=True)
        self.app.db.acquisitions.update_many({'collections': _id}, {'$pull': {'collections': _id}})
        self._delete(_id)


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
                ])
        query = {'_id': {'$in': [ar['_id'] for ar in agg_res]}}
        projection = {'label': 1, 'subject.code': 1, 'notes': 1, 'timestamp': 1, 'timezone': 1, 'subject.age': 1, 'subject.sex': 1, 'files': 1}
        projection['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site}}
        sessions = list(self.dbc.find(query, projection)) # avoid permissions checking by not using ContainerList._get()
        session_measurements = {}
        if self.request.GET.get('measurements', '').lower() in ('1', 'true'):
            session_measurements = self.app.db.acquisitions.aggregate([
                {'$match': {'session': {'$in': [sess['_id'] for sess in sessions]}}},
                {'$group': {'_id': '$session', 'measurements': {'$addToSet': '$datatype'}}}
                ])
            session_measurements = {sess['_id']: sess['measurements'] for sess in session_measurements}
        for sess in sessions:
            sess['measurements'] = session_measurements.get(sess['_id'], None)
            sess['subject_code'] = sess.get('subject', {}).get('code', '') # FIXME when subject is pulled out of session
            sess['attachment_count'] = len([f for f in sess.get('files', []) if f.get('flavor') == 'attachment'])
        if self.debug:
            for sess in sessions:
                sid = str(sess['_id'])
                sess['details'] = self.uri_for('session', sid, _full=True) + '?user=' + self.request.GET.get('user', '')
                sess['acquisitions'] = self.uri_for('coll_acquisitions', cid, _full=True) + '?session=%s&user=%s' % (sid, self.request.GET.get('user', ''))
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
            acq['attachment_count'] = len([f for f in acq.get('files', []) if f.get('flavor') == 'attachment'])
        if self.debug:
            for acq in acquisitions:
                aid = str(acq['_id'])
                acq['details'] = self.uri_for('acquisition', aid, _full=True) + '?user=' + self.request.GET.get('user', '')
        return acquisitions

    def put(self):
        """Update many Acquisitions."""
        self.response.write('acquisitions put\n')
