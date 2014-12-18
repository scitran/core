# @author:  Gunnar Schaefer, Kevin S. Hahn

import logging
log = logging.getLogger('nimsapi')
logging.getLogger('requests').setLevel(logging.WARNING) # silence Requests library logging

import copy
import json
import base64
import webapp2
import datetime
import requests
import bson.json_util

ROLES = [
        {
            'rid': 'view',
            'name': 'View-Only',
            'sort': 0,
            },
        {
            'rid': 'download',
            'name': 'Download',
            'sort': 1,
            },
        {
            'rid': 'modify',
            'name': 'Modify',
            'sort': 2,
            },
        {
            'rid': 'admin',
            'name': 'Admin',
            'sort': 3,
            },
        ]

INTEGER_ROLES = {r['rid']: r['sort'] for r in ROLES}


def mongo_dict(d):
    def _mongo_list(d, pk=''):
        pk = pk and pk + '.'
        return sum([_mongo_list(v, pk+k) if isinstance(v, dict) else [(pk+k, v)] for k, v in d.iteritems()], [])
    return dict(_mongo_list(d))


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
            'kinds': {
                'title': 'Kinds',
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
        self.debug = self.app.config['insecure']

        # set uid, source_site, public_request, and superuser
        self.uid = None
        self.source_site = None
        access_token = self.request.headers.get('Authorization', None)
        if access_token and self.app.config['oauth2_id_endpoint']:
            r = requests.get(self.app.config['oauth2_id_endpoint'], headers={'Authorization': 'Bearer ' + access_token})
            if r.status_code == 200:
                self.uid = json.loads(r.content)['email']
            else:
                headers = {'WWW-Authenticate': 'Bearer realm="%s", error="invalid_token", error_description="Invalid OAuth2 token."' % self.app.config['site_id']}
                self.abort(401, 'invalid oauth2 token', headers=headers)
        elif self.debug and self.request.get('user'):
            self.uid = self.request.get('user')
        elif self.request.user_agent.startswith('NIMS Instance'):
            self.uid = self.request.headers.get('X-User')
            self.source_site = self.request.headers.get('X-Site')
            if self.request.environ['SSL_CLIENT_VERIFY'] != 'SUCCESS':
                self.abort(401, 'no valid SSL client certificate')
            remote_instance = self.request.user_agent.replace('NIMS Instance', '').strip()
            if not self.app.db.remotes.find_one({'_id': remote_instance}):
                self.abort(402, remote_instance + ' is not authorized')
        self.public_request = not bool(self.uid)
        if self.public_request or self.source_site:
            self.superuser_request = False
        else:
            user = self.app.db.users.find_one({'_id': self.uid}, ['root', 'wheel'])
            if not user:
                self.abort(403, 'user ' + self.uid + ' does not exist')
            self.superuser_request = user.get('root') and user.get('wheel')

    def dispatch(self):
        """dispatching and request forwarding"""
        target_site = self.request.get('site', self.app.config['site_id'])
        if target_site == self.app.config['site_id']:
            log.debug('from %s %s %s %s %s' % (self.source_site, self.uid, self.request.method, self.request.path, str(self.request.params.mixed())))
            return super(RequestHandler, self).dispatch()
        else:
            if not self.app.config['site_id']:
                self.abort(500, 'api site_id is not configured')
            if not self.app.config['ssl_cert']:
                self.abort(500, 'api ssl_cert is not configured')
            target = self.app.db.remotes.find_one({'_id': target_site}, ['api_uri'])
            if not target:
                self.abort(402, 'remote host ' + target_site + ' is not an authorized remote')
            # adjust headers
            self.headers = self.request.headers
            self.headers['User-Agent'] = 'NIMS Instance ' + self.app.config['site_id']
            self.headers['X-User'] = self.uid
            self.headers['X-Site'] = self.app.config['site_id']
            self.headers['Content-Length'] = len(self.request.body)
            del self.headers['Host']
            if 'Authorization' in self.headers: del self.headers['Authorization']
            # adjust params
            self.params = self.request.params.mixed()
            if 'user' in self.params: del self.params['user']
            del self.params['site']
            log.debug(' for %s %s %s %s %s' % (target_site, self.uid, self.request.method, self.request.path, str(self.request.params.mixed())))
            target_uri = target['api_uri'] + self.request.path.split('/nimsapi')[1]
            r = requests.request(self.request.method, target_uri,
                    params=self.params, data=self.request.body, headers=self.headers, cert=self.app.config['ssl_cert'])
            if r.status_code != 200:
                self.abort(r.status_code, 'InterNIMS p2p err: ' + r.reason)
            self.response.write(r.content)

    def abort(self, code, *args, **kwargs):
        log.warning(str(code) + ' ' + '; '.join(args))
        webapp2.abort(code, *args, **kwargs)

    def schema(self, updates={}):
        json_schema = copy.deepcopy(self.json_schema)
        json_schema['properties'].update(updates)
        return json_schema


class Container(RequestHandler):

    def _get(self, _id, min_role=None): # TODO: take projection arg for added effiency; use empty projection for access checks
        container = self.dbc.find_one({'_id': _id})
        if not container:
            self.abort(404, 'no such ' + self.__class__.__name__)
        if self.uid is None:
            if not container.get('public', False):
                self.abort(403, 'this ' + self.__class__.__name__ + 'is not public')
            del container['permissions']
        elif not self.superuser_request:
            user_perm = None
            for perm in container['permissions']:
                if perm['uid'] == self.uid and perm.get('site') == self.source_site:
                    user_perm = perm
                    break
            else:
                self.abort(403, self.uid + ' does not have permissions on this ' + self.__class__.__name__)
            if min_role and INTEGER_ROLES[user_perm['access']] < INTEGER_ROLES[min_role]:
                self.abort(403, self.uid + ' does not have at least ' + min_role + ' permissions on this ' + self.__class__.__name__)
            if not user_perm['access'] != 'admin': # if not admin, mask permissions of other users
                container['permissions'] = user_perm
        if self.request.get('paths').lower() in ('1', 'true'):
            for file_info in container['files']:
                file_info['path'] = str(_id)[-3:] + '/' + str(_id) + '/' + file_info['name'] + file_info['ext']
        return container


class AcquisitionAccessChecker(object):

    def check_acq_list(self, acq_ids):
        if not self.superuser_request:
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
