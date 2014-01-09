# @author:  Gunnar Schaefer, Kevin S. Hahn

import json
import base64
import socket       # socket.gethostname()
import logging
import webapp2
import datetime
import requests
import bson.json_util
import Crypto.Hash.SHA
import Crypto.PublicKey.RSA
import Crypto.Signature.PKCS1_v1_5

log = logging.getLogger('nimsapi')
requests_log = logging.getLogger('requests')            # configure Requests logging
requests_log.setLevel(logging.WARNING)                  # set level to WARNING (default is INFO)


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
        self.request.remote_user = self.request.get('user', None) # FIXME: auth system should set REMOTE_USER
        self.userid = self.request.remote_user or '@public'
        self.user = self.app.db.users.find_one({'_id': self.userid})
        self.user_is_superuser = self.user.get('superuser')
        self.target_id = self.request.get('iid', None)
        self.site_id = self.app.config.get('site_id')           # is ALREADY 'None' if not specified in args, never empty
        self.privkey = self.app.config.get('privkey')           # is ALREADY 'None' if not specified in args, never empty

    def dispatch(self):
        """dispatching and request forwarding"""
        # dispatch to local instance
        if self.target_id in [None, self.site_id]:
            log.debug(socket.gethostname() + ' dispatching to local ' + self.request.url)
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
                self.signature = base64.b64decode(self.request.headers.get('Authorization'))
                payload = self.request.body
                key = Crypto.PublicKey.RSA.importKey(target['pubkey'])
                h = Crypto.Hash.SHA.new(payload)
                verifier = Crypto.Signature.PKCS1_v1_5.new(key)
                if verifier.verify(h, self.signature):
                    log.debug('message/signature is authentic')
                    super(NIMSRequestHandler, self).dispatch()
                else:
                    log.debug('message/signature is not authentic')
                    self.abort(403, 'authentication failed')
            # request originates from self
            else:
                    super(NIMSRequestHandler, self).dispatch()

        # dispatch to remote instance
        elif self.privkey is not None and self.site_id is not None:
            log.debug(socket.gethostname() + ' dispatching to remote ' + self.target_id)
            # is target registered?
            target = self.app.db.remotes.find_one({'_id': self.target_id}, {'_id':False, 'hostname':True})
            if not target:
                log.debug('remote host ' + self.target_id + ' not in auth list. DENIED')
                self.abort(403, self.target_id + 'is not authorized')

            # disassemble the incoming request
            reqparams = dict(self.request.params)
            reqpayload = self.request.body                                      # request payload, almost always empty
            reqheaders = dict(self.request.headers)
            reqheaders['User-Agent'] = 'NIMS Instance ' + self.site_id
            del reqheaders['Host']                                              # delete old host destination

            # create a signature of the incoming request payload
            h = Crypto.Hash.SHA.new(reqpayload)
            signature = Crypto.Signature.PKCS1_v1_5.new(self.privkey).sign(h)
            reqheaders['Authorization'] = base64.b64encode(signature)

            # construct outgoing request
            target_api = 'http://' + target['hostname'] + self.request.path      # TODO: switch to https
            # target_api = 'https://' + target['hostname'] + self.request.path)
            r = requests.request(method=self.request.method, data=reqpayload, url=target_api, params=reqparams, headers=reqheaders, verify=False)

            # return response content
            # TODO: headers
            self.response.write(r.content)

        elif self.privkey is None or self.site_id is None:
            log.debug('no private key (privkey), or local instance id (iid). cannot dispatch to remote')

    def schema(self, *args, **kwargs):
        self.response.write(json.dumps(self.json_schema, default=bson.json_util.default))
