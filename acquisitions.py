# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('nimsapi')

import bson.json_util

import nimsdata
import nimsdata.medimg

import base


class Acquisitions(base.RequestHandler):

    """/nimsapi/acquisitions """

    def __init__(self, request=None, response=None):
        super(Acquisitions, self).__init__(request, response)
        self.dbc = self.app.db.acquisitions

    def count(self):
        """Return the number of Acquisitions."""
        self.response.write(self.dbc.count())

    def post(self):
        """Create a new Acquisition."""
        self.response.write('acquisitions post\n')

    def get(self, sid):
        """Return the list of Session Acquisitions."""
        _id = bson.ObjectId(sid)
        if not self.app.db.sessions.find_one({'_id': _id}):
            self.abort(404, 'no such Session')
        query = {'session': _id}
        projection = {'label': 1, 'description': 1, 'types': 1, 'notes': 1}
        if self.public_request:
            query['public'] = True
        elif not self.superuser:
            query['permissions'] = {'$elemMatch': {'uid': self.uid, 'site': self.source_site}}
            projection['permissions'] = {'$elemMatch': {'uid': self.uid, 'site': self.source_site}}
        acquisitions = list(self.dbc.find(query, projection))
        if self.debug:
            for acquisition in acquisitions:
                aid = str(acquisition['_id'])
                acquisition['details'] = self.uri_for('acquisition', aid=aid, _full=True) + '?' + self.request.query_string
        return acquisitions

    def put(self):
        """Update many Acquisitions."""
        self.response.write('acquisitions put\n')


class Acquisition(base.Container):

    """/nimsapi/acquisitions/<aid> """

    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Acquisition',
        'type': 'object',
        'properties': {
            '_id': {
            },
            'session': {
            },
            'files': {
                'title': 'Files',
                'type': 'array',
                'items': base.Container.file_schema,
                'uniqueItems': True,
            },
        },
        'required': ['_id'], #FIXME
    }

    def __init__(self, request=None, response=None):
        super(Acquisition, self).__init__(request, response)
        self.dbc = self.app.db.acquisitions

    def schema(self, *args, **kwargs):
        return super(Acquisition, self).schema(nimsdata.medimg.medimg.MedImgReader.acquisition_properties)
        nimsdata.project_properties(ds_dict['project_type'])
        nimsdata.session_properties(ds_dict['session_type'])
        nimsdata.acquisition_properties(ds_dict['acquisition_type'])

    def get(self, aid):
        """Return one Acquisition, conditionally with details."""
        _id = bson.ObjectId(aid)
        return self._get(_id)

    def put(self, aid):
        """Update an existing Acquisition."""
        _id = bson.ObjectId(aid)
        self._get(_id, 'read-write')
        updates = {'$set': {'_id': _id}, '$unset': {'__null__': ''}}
        for k, v in self.request.params.iteritems():
                if v is not None and v != '':
                    updates['$set'][k] = v # FIXME: do appropriate type conversion
                else:
                    updates['$unset'][k] = None
        self.dbc.update({'_id': _id}, updates)

    def delete(self, aid):
        """Delete an Acquisition."""
        self.abort(501)
