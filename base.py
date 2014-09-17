# @author:  Gunnar Schaefer, Kevin S. Hahn

import logging
log = logging.getLogger('nimsapi')
logging.getLogger('urllib3').setLevel(logging.WARNING) # silence Requests library logging

import copy
import json
import base64
import webapp2
import datetime
import requests
import bson.json_util
import Crypto.Hash.SHA
import Crypto.PublicKey.RSA
import Crypto.Signature.PKCS1_v1_5

ROLES = [
        {
            'rid': 'anon-read',
            'name': 'Anonymized',
            'sort': 0,
            'public': True,
            },
        {
            'rid': 'read-only',
            'name': 'Read-Only',
            'sort': 1,
            'public': True,
            },
        {
            'rid': 'read-write',
            'name': 'Read-Write',
            'sort': 2,
            },
        ]

INTEGER_ROLES = {r['rid']: r['sort'] for r in ROLES}


class RequestHandler(webapp2.RequestHandler):

    """fetches pubkey from own self.db.remotes. needs to be aware of OWN site uid"""

    json_schema = None

    file_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'File',
        'type': 'object',
        'properties': {
            'name': {
                'title': 'Name',
                'type': 'string',
            },
            'ext': {
                'title': 'Extension',
                'type': 'string',
            },
            'size': {
                'title': 'Size',
                'type': 'integer',
            },
            'sha1': {
                'title': 'SHA-1',
                'type': 'string',
            },
            'type': {
                'title': 'Type',
                'type': 'string',
            },
            'kind': {
                'title': 'Kind',
                'type': 'array',
            },
            'state': {
                'title': 'State',
                'type': 'array',
            },
        },
        'required': ['state', 'datatype', 'filetype'], #FIXME
        'additionalProperties': False
    }

    def __init__(self, request=None, response=None):
        self.initialize(request, response)
        self.target_site = self.request.get('site', None)
        self.access_token = self.request.headers.get('Authorization', None)
        self.source_site = None       # requesting remote site; gets set if request from remote
        self.debug = self.app.config['insecure']

        # CORS header
        if 'Origin' in self.request.headers and self.request.headers['Origin'].startswith('https://'):
            self.response.headers['Access-Control-Allow-Origin'] = self.request.headers['Origin']

        if self.access_token and self.app.config['oauth2_id_endpoint']:
            r = requests.get(self.app.config['oauth2_id_endpoint'], headers={'Authorization': 'Bearer ' + self.access_token})
            if r.status_code == 200:
                self.uid = json.loads(r.content)['email']
            else:
                # TODO: add handlers for bad tokens
                # inform app of expired token, app will try to get new token, or ask user to log in again
                log.debug('ERR: ' + str(r.status_code) + r.reason + ' bad token')
        elif self.debug and self.request.get('user', None):
            self.uid = self.request.get('user')
        else:
            self.uid = '@public'

        user = self.app.db.users.find_one({'_id': self.uid}, ['superuser'])
        if user:
            self.user_is_superuser = user.get('superuser', None)
        elif self.uid == '@public':
            self.user_is_superuser = False
        else:
            self.abort(403, 'user ' + self.uid + ' does not exist')

        if self.target_site not in [None, self.app.config['site_id']]:
            self.rtype = 'to_remote'

            if not self.app.config['site_id']:
                self.abort(500, 'api site_id is not configured')
            if not self.app.config['ssl_cert']:
                self.abort(500, 'api ssl_cert is not configured')

            target = self.app.db.remotes.find_one({'_id': self.target_site}, {'_id': False, 'api_uri': True})
            if not target:
                self.abort(402, 'remote host ' + self.target_site + ' is not an authorized remote')

            # adjust headers
            self.headers = self.request.headers
            self.headers['User-Agent'] = 'NIMS Instance ' + self.app.config['site_id']
            self.headers['X-User'] = self.uid
            self.headers['X-Site'] = self.app.config['site_id']
            self.headers['Content-Length'] = len(self.request.body)
            del self.headers['Host']
            if self.headers.get('Authorization'): del self.headers['Authorization']

            # adjust params
            self.params = self.request.params.mixed()
            if self.params.get('user'): del self.params['user']
            del self.params['site']

            self.target_uri = target['api_uri'] + self.request.path.split('/nimsapi')[1]
            self.cert = self.app.config['ssl_cert']

        elif self.request.user_agent.startswith('NIMS Instance'):
            self.rtype = 'from_remote'
            self.uid = self.request.headers.get('X-User')
            self.source_site = self.request.headers.get('X-Site')
            log.debug('%s %s' % (self.uid, self.source_site))

            self.user_is_superuser = False
            if self.request.environ['SSL_CLIENT_VERIFY'] != 'SUCCESS':
                self.abort(401, 'no valid SSL client certificate')

            remote_instance = self.request.user_agent.replace('NIMS Instance', '').strip()
            requester = self.app.db.remotes.find_one({'_id': remote_instance})
            if not requester:
                self.abort(402, remote_instance + ' is not authorized')

        else:
            self.rtype = 'local'

    def dispatch(self):
        """dispatching and request forwarding"""
        log.debug('%s %s %s %s %s %s' % (self.rtype, self.uid, self.source_site, self.request.method, self.request.path, str(self.request.params.mixed())))
        if self.rtype in ['local', 'from_remote']:
            return super(RequestHandler, self).dispatch()
        else:
            if self.request.method == 'OPTIONS':
                return self.options()
            r = requests.request(self.request.method, self.target_uri, params=self.params, data=self.request.body, headers=self.headers, cert=self.cert)
            if r.status_code != 200:
                self.abort(r.status_code, 'InterNIMS p2p err: ' + r.reason)
            self.response.write(r.content)

    def abort(self, code, *args, **kwargs):
        log.warning(str(code) + ' ' + '; '.join(args))
        if 'Access-Control-Allow-Origin' in self.response.headers:
            headers = kwargs.setdefault('headers', {})
            headers['Access-Control-Allow-Origin'] = self.response.headers['Access-Control-Allow-Origin']
        webapp2.abort(code, *args, **kwargs)

    def options(self, *args, **kwargs):
        self.response.headers['Access-Control-Allow-Methods'] = 'GET, HEAD, POST, PUT, DELETE, OPTIONS'
        self.response.headers['Access-Control-Allow-Headers'] = 'Authorization'
        self.response.headers['Access-Control-Max-Age'] = '151200'

    def schema(self, updates={}):
        if self.request.method == 'OPTIONS':
            return self.options()
        json_schema = copy.deepcopy(self.json_schema)
        json_schema['properties'].update(updates)
        return json_schema


class Container(RequestHandler):

    def _get(self, _id, min_role=None): # TODO: take projection arg for added effiency; use empty projection for access checks
        container = self.dbc.find_one({'_id': _id})
        if not container:
            self.abort(404, 'no such ' + self.__class__.__name__)
        if not self.user_is_superuser:
            pub_idx = -1
            user_perm = None
            for i, perm in enumerate(container['permissions']):
                if perm['uid'] == self.uid and perm.get('site') == self.source_site:
                    user_perm = perm
                if perm['uid'] == '@public':
                    container['public'] = perm['access']
                    pub_idx = i
            if pub_idx != -1:
                container['permissions'].pop(pub_idx)
            if not user_perm:
                self.abort(403, self.uid + ' does not have permissions on this ' + self.__class__.__name__)
            if min_role and INTEGER_ROLES[user_perm['access']] < INTEGER_ROLES[min_role]:
                self.abort(403, self.uid + ' does not have at least ' + min_role + ' permissions on this ' + self.__class__.__name__)
            if not user_perm.get('share', False): # if user not allowed to share, mask permissions of other users
                container['permissions'] = user_perm
        else:
            pub_idx = -1
            for i, perm in enumerate(container['permissions']):
                if perm['uid'] == '@public':
                    container['public'] = perm['access']
                    pub_idx = i
                    break
            if pub_idx != -1:
                container['permissions'].pop(pub_idx)
        return container


class AcquisitionAccessChecker(object):

    def check_acq_list(self, acq_ids):
        if not self.user_is_superuser:
            for a_id in acq_ids:
                agg_res = self.app.db.acquisitions.aggregate([
                        {'$match': {'_id': a_id}},
                        {'$project': {'permissions': 1}},
                        {'$unwind': '$permissions'},
                        ])['result']
                if not agg_res:
                    self.abort(404, 'Acquisition %s does not exist' % a_id)
                for perm_doc in agg_res:
                    if perm_doc['permissions']['uid'] == self.uid:
                        break
                else:
                    self.abort(403, self.uid + ' does not have permissions on Acquisition %s' % a_id)
