# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('nimsapi')

import bson.json_util

import nimsdata
import nimsdata.medimg

import base
import projects
import sessions
import acquisitions


class Collections(base.AcquisitionAccessChecker, projects.Projects):

    """/collections """

    def __init__(self, request=None, response=None):
        super(Collections, self).__init__(request, response)
        self.dbc = self.app.db.collections

    def count(self):
        """Return the number of Collections."""
        if self.request.method == 'OPTIONS':
            return self.options()
        self.response.write(self.dbc.count())

    def post(self):
        """Create a new Collection."""
        name = self.request.get('name') or 'innominate'
        acq_ids = [bson.ObjectId(aid) for aid in self.request.get_all('acquisitions[]', [])]
        self.check_acq_list(acq_ids)
        _id = self.dbc.insert({'curator': self.uid, 'name': name, 'permissions': [{'uid': self.uid, 'access': 'read-write', 'share': True}]})
        for a_id in acq_ids:
            self.app.db.acquisitions.update({'_id': a_id}, {'$push': {'collections': _id}})

    def get(self):
        """Return the list of Collections."""

        projection = {
                'curator': 1, 'name': 1, 'notes': 1,
                'permissions': {'$elemMatch': {'uid': self.uid, 'site': self.source_site}},
                }
        collections = self._get(projection)
        for coll in collections:
            coll['site'] = self.app.config['site_id']
            coll['site_name'] = self.app.config['site_name']
        if self.debug:
            for coll in collections:
                cid = str(coll['_id'])
                coll['details'] = self.uri_for('collection', cid=cid, _full=True) + '?' + self.request.query_string
                coll['sessions'] = self.uri_for('coll_sessions', cid=cid, _full=True) + '?' + self.request.query_string
                coll['acquisitions'] = self.uri_for('coll_acquisitions', cid=cid, _full=True) + '?' + self.request.query_string
        return collections

    def put(self):
        """Update many Collections."""
        self.response.write('collections put\n')


class Collection(base.AcquisitionAccessChecker, base.Container):

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
                'items': base.Container.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id', 'group', 'name'], #FIXME
    }

    def __init__(self, request=None, response=None):
        super(Collection, self).__init__(request, response)
        self.dbc = self.app.db.collections

    def get(self, cid):
        """Return one Collection, conditionally with details."""
        _id = bson.ObjectId(cid)
        coll = self._get(_id)
        if self.debug:
            coll['sessions'] = self.uri_for('coll_sessions', cid=cid, _full=True) + '?' + self.request.query_string
            coll['acquisitions'] = self.uri_for('coll_acquisitions', cid=cid, _full=True) + '?' + self.request.query_string
        return coll

    def put(self, cid):
        """Update an existing Collection."""
        _id = bson.ObjectId(cid)
        self._get(_id, 'read-write')
        add_acq_ids = [bson.ObjectId(aid) for aid in self.request.get_all('add_acquisitions[]', [])]
        del_acq_ids = [bson.ObjectId(aid) for aid in self.request.get_all('del_acquisitions[]', [])]
        self.check_acq_list(add_acq_ids)
        self.check_acq_list(del_acq_ids)
        for a_id in add_acq_ids:
            self.app.db.acquisitions.update({'_id': a_id}, {'$addToSet': {'collections': _id}})
        for a_id in del_acq_ids:
            self.app.db.acquisitions.update({'_id': a_id}, {'$pull': {'collections': _id}})

    def delete(self, cid):
        """Delete a Collection."""
        _id = bson.ObjectId(cid)
        self._get(_id, 'read-write')
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
        projection = ['label', 'subject', 'notes']
        sessions = list(self.dbc.find(query, projection))
        for sess in sessions:
            sess['site'] = self.app.config['site_id']
        if self.debug:
            for sess in sessions:
                sid = str(sess['_id'])
                sess['details'] = self.uri_for('session', sid=sid, _full=True) + '?user=' + self.request.get('user')
                sess['acquisitions'] = self.uri_for('coll_acquisitions', cid=cid, _full=True) + '?session=%s&user=%s' % (sid, self.request.get('user'))
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
        projection = ['label', 'description', 'types', 'notes']
        acquisitions = list(self.dbc.find(query, projection))
        if self.debug:
            for acquisition in acquisitions:
                aid = str(acquisition['_id'])
                acquisition['details'] = self.uri_for('acquisition', aid=aid, _full=True) + '?user=' + self.request.get('user')
        return acquisitions

    def put(self):
        """Update many Acquisitions."""
        self.response.write('acquisitions put\n')
