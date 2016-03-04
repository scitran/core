import copy
import datetime
import json
import jsonschema
import pymongo
import requests
import traceback
import urllib
import urlparse
import webapp2

from . import util
from . import config
from .types import Origin
from . import validators
from .dao import APIConsistencyException

log = config.log

# When authenticating as a drone, the user agent must start with this prefix.
DRONE_PREFIX = 'SciTran Drone '

class RequestHandler(webapp2.RequestHandler):

    json_schema = None

    def __init__(self, request=None, response=None):
        self.initialize(request, response)
        self.debug = config.get_item('core', 'insecure')
        request_start = datetime.datetime.utcnow()
        provider_avatar = None

        # set uid, source_site, public_request, and superuser
        self.uid = None
        self.source_site = None
        drone_request = False
        drone_name = ''

        user_agent = self.request.headers.get('User-Agent', '')
        access_token = self.request.headers.get('Authorization', None)
        drone_secret = self.request.headers.get('X-SciTran-Auth', None)

        site_id = config.get_item('site', 'id')
        if site_id is None:
            self.abort(503, 'Database not initialized')

        # User (oAuth) authentication
        if access_token:
            cached_token = config.db.authtokens.find_one({'_id': access_token})
            if cached_token:
                self.uid = cached_token['uid']
                log.debug('looked up cached token in %dms' % ((datetime.datetime.utcnow() - request_start).total_seconds() * 1000.))
            else:
                r = requests.get(config.get_item('auth', 'id_endpoint'), headers={'Authorization': 'Bearer ' + access_token})
                if r.ok:
                    identity = json.loads(r.content)
                    self.uid = identity.get('email')
                    if not self.uid:
                        self.abort(400, 'OAuth2 token resolution did not return email address')
                    config.db.authtokens.replace_one({'_id': access_token}, {'uid': self.uid, 'timestamp': request_start}, upsert=True)
                    config.db.users.update_one({'_id': self.uid, 'firstlogin': None}, {'$set': {'firstlogin': request_start}})
                    config.db.users.update_one({'_id': self.uid}, {'$set': {'lastlogin': request_start}})
                    log.debug('looked up remote token in %dms' % ((datetime.datetime.utcnow() - request_start).total_seconds() * 1000.))

                    # Set user's auth provider avatar
                    # TODO: switch on auth.provider rather than manually comparing endpoint URL.
                    if config.get_item('auth', 'id_endpoint') == 'https://www.googleapis.com/plus/v1/people/me/openIdConnect':
                        provider_avatar = identity.get('picture', '')
                        # Remove attached size param from URL.
                        u = urlparse.urlparse(provider_avatar)
                        query = urlparse.parse_qs(u.query)
                        query.pop('sz', None)
                        u = u._replace(query=urllib.urlencode(query, True))
                        provider_avatar = urlparse.urlunparse(u)
                else:
                    headers = {'WWW-Authenticate': 'Bearer realm="{}", error="invalid_token", error_description="Invalid OAuth2 token."'.format(site_id)}
                    self.abort(401, 'invalid oauth2 token', headers=headers)

        # 'Debug' (insecure) setting: allow request to act as requested user
        elif self.debug and self.get_param('user'):
            self.uid = self.get_param('user')

        # Drone shared secret authentication
        elif drone_secret is not None and user_agent.startswith(DRONE_PREFIX):
            if config.get_item('core', 'drone_secret') is None:
                self.abort(401, 'drone secret not configured')
            if drone_secret != config.get_item('core', 'drone_secret'):
                self.abort(401, 'invalid drone secret')
            drone_request = True
            drone_name = user_agent.replace(DRONE_PREFIX, '')
            log.info('drone "' + drone_name + '" request accepted')

        # Cross-site authentication
        elif user_agent.startswith('SciTran Instance '):
            if self.request.environ['SSL_CLIENT_VERIFY'] == 'SUCCESS':
                self.uid = self.request.headers.get('X-User')
                self.source_site = self.request.headers.get('X-Site')
                remote_instance = user_agent.replace('SciTran Instance', '').strip()
                if not config.db.sites.find_one({'_id': remote_instance}):
                    self.abort(402, remote_instance + ' is not an authorized remote instance')
            else:
                self.abort(401, 'no valid SSL client certificate')
        self.user_site = self.source_site or site_id

        self.public_request = not drone_request and not self.uid

        if self.public_request or self.source_site:
            self.superuser_request = False
        elif drone_request:
            self.superuser_request = True
        else:
            user = config.db.users.find_one({'_id': self.uid}, ['root'])
            if not user:
                self.abort(403, 'user ' + self.uid + ' does not exist')
            if provider_avatar:
                config.db.users.update_one({'_id': self.uid, 'avatar': None}, {'$set':{'avatar': provider_avatar, 'modified': request_start}})
                config.db.users.update_one({'_id': self.uid, 'avatars.provider': {'$ne': provider_avatar}}, {'$set':{'avatars.provider': provider_avatar, 'modified': request_start}})
            if self.is_true('root'):
                if user.get('root'):
                    self.superuser_request = True
                else:
                    self.abort(403, 'user ' + self.uid + ' is not authorized to make superuser requests')
            else:
                self.superuser_request = False

        self.set_origin(drone_request, drone_name)

    def set_origin(self, drone_request, drone_name):
        """
        Add an origin to the request object. Used later in request handler logic.

        Pretty clear duplication of logic with superuser_request / drone_request;
        this map serves a different purpose, and specifically matches the desired file-origin map.
        Might be a good future project to remove one or the other.
        """

        if self.uid is not None:
            self.origin = {
                'type': str(Origin.user),
                'id': self.uid
            }
        elif drone_request:
            self.origin = {
                'type': str(Origin.device),
                'id': drone_name
            }

            # Upsert device record, with last-contacted time.
            # In the future, consider merging any keys into self.origin?
            device_record = config.db['devices'].find_one_and_update({
                    '_id': self.origin['id']
                }, {
                    '$set': {
                        '_id': self.origin['id'],
                        'last-seen': datetime.datetime.utcnow()
                    }
                },
                upsert=True,
                return_document=pymongo.collection.ReturnDocument.AFTER
            )

            # Bit hackish - detect from route if a job is the origin, and if so what job ID.
            # Could be removed if routes get reorganized. POST /api/jobs/id/result, maybe?
            is_job_upload = self.request.path.startswith('/api/engine')
            job_id        = self.request.GET.get('job')

            # This runs after the standard drone-request upsert above so that we can still update the last-seen timestamp.
            if is_job_upload and job_id is not None:
                self.origin = {
                    'type': str(Origin.job),
                    'id': job_id
                }
        else:
            self.origin = {
                'type': str(Origin.unknown),
                'id': None
            }

        # print json.dumps(self.origin)

    def is_true(self, param):
        return self.request.GET.get(param, '').lower() in ('1', 'true')

    def get_param(self, param, default=None):
        return self.request.GET.get(param, default)

    def handle_exception(self, exception, debug):
        # Log the error.
        tb = traceback.format_exc()
        log.error(tb)

        # If the exception is a HTTPException, use its error code.
        # Otherwise use a generic 500 error code.
        if isinstance(exception, webapp2.HTTPException):
            code = exception.code
        elif isinstance(exception, validators.InputValidationException):
            code = 400
        elif isinstance(exception, dao.APIConsistencyException):
            code = 400
        else:
            code = 500
        util.send_json_http_exception(self.response, str(exception), code)

    def dispatch(self):
        """dispatching and request forwarding"""
        site_id = config.get_item('site', 'id')
        target_site = self.get_param('site', site_id)
        if target_site == site_id:
            log.debug('from %s %s %s %s %s' % (self.source_site, self.uid, self.request.method, self.request.path, str(self.request.GET.mixed())))
            return super(RequestHandler, self).dispatch()
        else:
            if not site_id:
                self.abort(500, 'api site.id is not configured')
            if not config.get_item('site', 'ssl_cert'):
                self.abort(500, 'api ssl_cert is not configured')
            target = config.db.sites.find_one({'_id': target_site}, ['api_uri'])
            if not target:
                self.abort(402, 'remote host ' + target_site + ' is not an authorized remote')
            # adjust headers
            self.headers = self.request.headers
            self.headers['User-Agent'] = 'SciTran Instance ' + site_id
            self.headers['X-User'] = self.uid
            self.headers['X-Site'] = site_id
            self.headers['Content-Length'] = len(self.request.body)
            del self.headers['Host']
            if 'Authorization' in self.headers: del self.headers['Authorization']
            # adjust params
            self.params = self.request.GET.mixed()
            if 'user' in self.params: del self.params['user']
            del self.params['site']
            log.debug(' for %s %s %s %s %s' % (target_site, self.uid, self.request.method, self.request.path, str(self.request.GET.mixed())))
            target_uri = target['api_uri'] + self.request.path.split('/api')[1]
            r = requests.request(
                    self.request.method,
                    target_uri,
                    stream=True,
                    params=self.params,
                    data=self.request.body_file,
                    headers=self.headers,
                    cert=config.get_item('site', 'ssl_cert'))
            if r.status_code != 200:
                self.abort(r.status_code, 'InterNIMS p2p err: ' + r.reason)
            self.response.app_iter = r.iter_content(2**20)
            for header in ['Content-' + h for h in 'Length', 'Type', 'Disposition']:
                if header in r.headers:
                    self.response.headers[header] = r.headers[header]

    def abort(self, code, detail=None, **kwargs):
        if isinstance(detail, jsonschema.ValidationError):
            detail = {
                'relative_path': list(detail.relative_path),
                'instance': detail.instance,
                'validator': detail.validator,
                'validator_value': detail.validator_value,
            }
        log.warning(str(self.uid) + ' ' + str(code) + ' ' + str(detail))
        webapp2.abort(code, detail=detail, **kwargs)

    def schema(self, updates={}):
        json_schema = copy.deepcopy(self.json_schema)
        json_schema['properties'].update(updates)
        return json_schema

