# @author:  Gunnar Schaefer

import bson

import scitran.data.medimg

from . import util
from . import containers

ACQUISITION_POST_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'Acquisition',
    'type': 'object',
    'properties': {
        'label': {
            'type': 'string',
            'maxLength': 32,
        },
        'timestamp': {
            'type': 'string',
            'format': 'date-time',
        },
        'timezone': {
            'type': 'string',
            'enum': util.valid_timezones,
        },
    },
    'required': ['label'],
}

ACQUISITION_PUT_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'Acquisition',
    'type': 'object',
    'properties': {
        'label': {
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
        'description': {
            'type': 'string'
        },
    },
    'minProperties': 1,
    'additionalProperties': False,
}


class Acquisitions(containers.ContainerList):

    """/acquisitions """

    post_schema = ACQUISITION_POST_SCHEMA

    def __init__(self, request=None, response=None):
        super(Acquisitions, self).__init__(request, response)
        self.dbc = self.app.db.acquisitions

    def count(self):
        """Return the number of Acquisitions."""
        self.response.write(self.dbc.count())

    def post(self, sid):
        """Create a new Acquisition."""
        json_body = self._post()
        _id = bson.ObjectId(sid)
        session = self.app.db.sessions.find_one({'_id': _id}, ['permissions', 'public'])
        if not session:
            self.abort(404, 'no such session')
        if not self.superuser_request and util.user_perm(session['permissions'], self.uid).get('access') != 'admin':
            self.abort(400, 'must be session admin to create acquisition')
        json_body['session'] = _id
        json_body['permissions'] = session['permissions']
        json_body['public'] = session.get('public', False)
        json_body['files'] = []
        if 'timestamp' in json_body:
            json_body['timestamp'] = util.parse_timestamp(json_body['timestamp'])
        return {'_id': str(self.dbc.insert(json_body))}

    def get(self, sid):
        """Return the list of Session Acquisitions."""
        _id = bson.ObjectId(sid)
        if not self.app.db.sessions.find_one({'_id': _id}):
            self.abort(404, 'no such Session')
        query = {'session': _id}
        projection = ['label', 'description', 'modality', 'datatype', 'notes', 'timestamp', 'timezone']
        acquisitions = self._get(query, projection, self.request.GET.get('admin', '').lower() in ('1', 'true'))
        if self.debug:
            for acquisition in acquisitions:
                aid = str(acquisition['_id'])
                acquisition['debug'] = {}
                acquisition['debug']['details'] = self.uri_for('acquisition', aid, _full=True) + '?' + self.request.query_string
        return acquisitions


class Acquisition(containers.Container):

    """/acquisitions/<aid> """

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
                'items': containers.FILE_SCHEMA,
                'uniqueItems': True,
            },
        },
        'required': ['_id'], #FIXME
    }

    put_schema = ACQUISITION_PUT_SCHEMA

    def __init__(self, request=None, response=None):
        super(Acquisition, self).__init__(request, response)
        self.dbc = self.app.db.acquisitions

    def schema(self, *args, **kwargs):
        return super(Acquisition, self).schema(scitran.data.medimg.medimg.MedImgReader.acquisition_properties)
        # scitran.data.project_properties(ds_dict['project_type'])
        # scitran.data.session_properties(ds_dict['session_type'])
        # scitran.data.acquisition_properties(ds_dict['acquisition_type'])

    def get(self, aid):
        """Return one Acquisition, conditionally with details."""
        _id = bson.ObjectId(aid)
        acq, _ = self._get(_id)
        acq['session'] = str(acq['session'])
        return acq

    def put(self, aid):
        """Update an existing Acquisition."""
        _id = bson.ObjectId(aid)
        json_body = super(Acquisition, self)._put(_id)

    def delete(self, aid):
        """Delete an Acquisition."""
        _id = bson.ObjectId(aid)
        self._get(_id, 'admin', perm_only=True)
        self._delete(_id)
