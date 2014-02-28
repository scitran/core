# @author:  Gunnar Schaefer, Kevin S. Hahn

import json
import base64
import webapp2
import datetime
import requests
import bson.json_util
import Crypto.Hash.SHA
import Crypto.PublicKey.RSA
import Crypto.Signature.PKCS1_v1_5

import logging
log = logging.getLogger('nimsapi')
logging.getLogger('requests').setLevel(logging.WARNING)                  # silence Requests library logging

INTEGER_ROLES = {
        'anon-read':  0,
        'read-only':  1,
        'read-write': 2,
        'admin':      3,
        }


class NIMSRequestHandler(webapp2.RequestHandler):

    """fetches pubkey from own self.db.remotes. needs to be aware of OWN site uid"""

    json_schema = None

    file_schema = {
        'title': 'File',
        'type': 'object',
        'properties': {
            'type': {
                'title': 'Type',
                'type': 'array',
            },
            'filename': {
                'title': 'File Name',
                'type': 'string',
            },
            'ext': {
                'title': 'File Name Extension',
                'type': 'string',
            },
            'md5': {
                'title': 'MD5',
                'type': 'string',
            },
            'size': {
                'title': 'Size',
                'type': 'integer',
            },
        }
    }

    def __init__(self, request=None, response=None):
        self.initialize(request, response)
        self.uid = '@public'  # @public is default user
        self.access_token = self.request.headers.get('Authorization', None)
        log.debug('accesstoken: ' + str(self.access_token))

        if self.access_token and self.app.config['oauth2_id_endpoint']:
            r = requests.request(method='GET', url = self.app.config['oauth2_id_endpoint'], headers={'Authorization': 'Bearer ' + self.access_token})
            if r.status_code == 200:
                oauth_user = json.loads(r.content)
                self.uid = oauth_user['email']
                log.debug('oauth user: ' + oauth_user['email'])
            else:
                #TODO: add handlers for bad tokens.
                log.debug('ERR: ' + str(r.status_code) + ' bad token')
        elif self.app.config['insecure'] and 'X-Requested-With' not in self.request.headers and self.request.get('user', None):
            self.uid = self.request.get('user')

        self.user = self.app.db.users.find_one({'uid': self.uid})
        self.user_is_superuser = self.user.get('superuser', None) if self.user else False

        # p2p request
        self.target_id = self.request.get('iid', None)
        self.p2p_user = self.request.headers.get('X-From', None)
        self.site_id = self.app.config['site_id']
        self.ssl_key = self.app.config['ssl_key']
        log.debug('X-From: ' + str(self.p2p_user))

        # CORS bare minimum
        self.response.headers.add('Access-Control-Allow-Origin', self.request.headers.get('origin', '*'))

        if not self.request.path.endswith('/nimsapi/log'):
            log.info(self.request.method + ' ' + self.request.path + ' ' + str(self.request.params.mixed()))

    def dispatch(self):
        """dispatching and request forwarding"""
        # dispatch to local instance
        if self.target_id in [None, self.site_id]:
            # request originates from remote instance
            if self.request.user_agent.startswith('NIMS Instance'):
                # is the requester an authorized remote site
                requester = self.request.user_agent.replace('NIMS Instance', '').strip()
                target = self.app.db.remotes.find_one({'_id':requester})
                if not target:
                    log.debug('remote host ' + requester + ' not in auth list. DENIED')
                    self.abort(403, requester + ' is not authorized')
                log.debug('request from ' + self.request.user_agent + ', interNIMS p2p initiated')
                # verify signature
                self.signature = base64.b64decode(self.request.headers.get('X-Signature'))
                payload = self.request.body
                key = Crypto.PublicKey.RSA.importKey(target['pubkey'])
                h = Crypto.Hash.SHA.new(payload)
                verifier = Crypto.Signature.PKCS1_v1_5.new(key)
                if verifier.verify(h, self.signature):
                    super(NIMSRequestHandler, self).dispatch()
                else:
                    log.warning('message/signature is not authentic')
                    self.abort(403, 'authentication failed')
            # request originates from self
            else:
                    super(NIMSRequestHandler, self).dispatch()

        # dispatch to remote instance
        elif self.ssl_key is not None and self.site_id is not None:
            log.debug('dispatching to remote ' + self.target_id)
            # is target registered?
            target = self.app.db.remotes.find_one({'_id': self.target_id}, {'_id':False, 'api_uri':True})
            if not target:
                log.debug('remote host ' + self.target_id + ' not in auth list. DENIED')
                self.abort(403, self.target_id + 'is not authorized')

            # disassemble the incoming request
            reqparams = self.request.params
            reqpayload = self.request.body                                      # request payload, almost always empty
            reqheaders = self.request.headers
            reqheaders['User-Agent'] = 'NIMS Instance ' + self.site_id
            reqheaders['X-From'] = self.uid
            reqheaders['Content-Length'] = len(reqpayload)
            del reqheaders['Host']                                              # delete old host destination
            try:
                del reqheaders['Authorization']                                 # delete access_token
            except KeyError as e:
                pass                                                            # not all requests will have access_token

            # build up a description of request to sign
            # msg = self.request.method + self.request.path + str(self.request.params) + str(self.request.headers) + self.request.body
            # log.debug(msg)

            # create a signature of the incoming request payload
            h = Crypto.Hash.SHA.new(reqpayload)
            signature = Crypto.Signature.PKCS1_v1_5.new(self.ssl_key).sign(h)
            reqheaders['X-Signature'] = base64.b64encode(signature)

            # construct outgoing request
            target_api = 'https://' + target['api_uri'] + self.request.path.split('/nimsapi')[1]
            r = requests.request(method=self.request.method, data=reqpayload, url=target_api, params=reqparams, headers=reqheaders, verify=False)

            # return response content
            # TODO: think about: are the headers even useful?
            self.response.write(r.content)

        elif self.ssl_key is None or self.site_id is None:
            log.debug('ssl key or site id undefined, cannot dispatch to remote')

    def schema(self, *args, **kwargs):
        self.response.write(json.dumps(self.json_schema, default=bson.json_util.default))

    def get_collection(self, cid, min_role='anon-read'):
        collection = self.app.db.collections.find_one({'_id': cid})
        if not collection:
            self.abort(404)
        if not self.user_is_superuser:
            for perm in collection['permissions']:
                if perm['uid'] == self.uid:
                    break
            else:
                self.abort(403, self.uid + ' does not have permission to this Collection')
            if INTEGER_ROLES[perm['role']] < INTEGER_ROLES[min_role]:
                self.abort(403, self.uid + ' does not have at least ' + min_role + ' on this Collection')
            if perm['role'] != 'admin': # if not admin, mask all other permissions
                    collection['permissions'] = [{'uid': self.uid, 'role': perm['role']}]
        return collection

    def get_experiment(self, xid, min_role='anon-read'):
        experiment = self.app.db.experiments.find_one({'_id': xid})
        if not experiment:
            self.abort(404)
        if not self.user_is_superuser:
            for perm in experiment['permissions']:
                if perm['uid'] == self.uid:
                    break
            else:
                self.abort(403, self.uid + ' does not have permission to this Experiment')
            if INTEGER_ROLES[perm['role']] < INTEGER_ROLES[min_role]:
                self.abort(403, self.uid + ' does not have at least ' + min_role + ' on this Experiment')
            if perm['role'] != 'admin': # if not admin, mask all other permissions
                    experiment['permissions'] = [{'uid': self.uid, 'role': perm['role']}]
        return experiment

    def get_session(self, sid, min_role='anon-read'):
        #FIXME: implement min_role logic
        session = self.app.db.sessions.find_one({'_id': sid})
        if not session:
            self.abort(404)
        experiment = self.app.db.experiments.find_one({'_id': session['experiment']})
        if not experiment:
            self.abort(500)
        if not self.user_is_superuser:
            for perm in experiment['permissions']:
                if perm['uid'] == self.uid:
                    break
            else:
                self.abort(403, 'user does not have permission to this Session')
        return session

    def get_epoch(self, eid, min_role='anon-read'):
        #FIXME: implement min_role logic
        epoch = self.app.db.epochs.find_one({'_id': eid})
        if not epoch:
            self.abort(404)
        session = self.app.db.sessions.find_one({'_id': epoch['session']})
        if not session:
            self.abort(500)
        experiment = self.app.db.experiments.find_one({'_id': session['experiment']})
        if not experiment:
            self.abort(500)
        if not self.user_is_superuser:
            for perm in experiment['permissions']:
                if perm['uid'] == self.uid:
                    break
            else:
                self.abort(403, 'user does not have permission to this Epoch')
        return epoch
