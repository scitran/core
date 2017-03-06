import datetime
import requests
import json
import urllib
import urlparse

from . import APIAuthProviderException, APIUnknownUserException
from .. import config, util

log = config.log

class AuthProvider(object):
    """
    This class provides access to mongodb collection elements (called containers).
    It is used by ContainerHandler istances for get, create, update and delete operations on containers.
    Examples: projects, sessions, acquisitions and collections
    """

    def __init__(self, auth_type):
        self.auth_type = auth_type
        try:
            self.config =  config.get_auth(auth_type)
        except KeyError:
            raise NotImplementedError('Auth type {} is not supported by this instance'.format(auth_type))

    @staticmethod
    def factory(auth_type):
        """
        Factory method to aid in the creation of an AuthProvider instance
        when auth_type is dynamic.
        """
        if auth_type in AuthProviders:
            provider_class = AuthProviders[auth_type]
            return provider_class()
        else:
            raise NotImplementedError('Auth type {} is not supported'.format(auth_type))

    def validate_code(self, code, **kwargs):
        raise NotImplementedError

    def ensure_user_exists(self, uid):
        user = config.db.users.find_one({'_id': uid})
        if not user:
            raise APIUnknownUserException('User {} does not exist.'.format(uid))
        if user.get('disabled', False) is True:
            raise APIUnknownUserException('User {} is disabled'.format(uid))

    def set_user_gravatar(self, uid, email):
        if email and uid:
            gravatar = util.resolve_gravatar(email)
            if gravatar is not None:
                timestamp = datetime.datetime.utcnow()
                # Update the user's gravatar if it has changed.
                config.db.users.update_one({'_id': uid, 'avatars.gravatar': {'$ne': gravatar}}, {'$set':{'avatars.gravatar': gravatar, 'modified': timestamp}})



class JWTAuthProvider(AuthProvider):

    def __init__(self):
        super(JWTAuthProvider,self).__init__('ldap')

    def validate_code(self, code, **kwargs):
        uid = self.validate_user(code)
        return code, None, uid

    def validate_user(self, token):
        r = requests.post(self.config['verify_endpoint'], data={'token': token})
        if not r.ok:
            raise APIAuthProviderException('User token not valid')
        uid = json.loads(r.content).get('mail')
        if not uid:
            raise APIAuthProviderException('Auth provider did not provide user email')

        self.ensure_user_exists(uid)
        self.set_user_gravatar(uid, uid)

        return {
            'access_token': token,
            'uid': uid,
            'auth_type': self.auth_type,
            'expires': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        }


class GoogleOAuthProvider(AuthProvider):

    def __init__(self):
        super(GoogleOAuthProvider,self).__init__('google')

    def validate_code(self, code, **kwargs):
        payload = {
            'client_id':        self.config['client_id'],
            'client_secret':    self.config['client_secret'],
            'code':             code,
            'grant_type':       'authorization_code',
            'redirect_uri':     config.get_item('site', 'redirect_url')
        }

        r = requests.post(self.config['token_endpoint'], data=payload)
        if not r.ok:
            raise APIAuthProviderException('User code not valid')

        response = json.loads(r.content)
        token = response['access_token']

        uid = self.validate_user(token)

        return {
            'refresh_token': response.get('refresh_token'),
            'access_token': token,
            'uid': uid,
            'auth_type': self.auth_type,
            'expires': datetime.datetime.utcnow() + datetime.timedelta(seconds=response['expires_in'])
        }

    def refresh_token(self, token):
        payload = {
            'client_id':        self.config['client_id'],
            'client_secret':    self.config['client_secret'],
            'refresh_token':    token,
            'grant_type':       'refresh_token',
        }
        r = requests.post(self.config['refresh_endpoint'], data=payload)
        if not r.ok:
            raise APIAuthProviderException('Unable to refresh token.')

        response = json.loads(r.content)
        return {
            'refresh_token': response['refresh_token'],
            'access_token': response['access_token'],
            'expires': datetime.datetime.utcnow() + datetime.timedelta(seconds=response['expires_in'])
        }

    def validate_user(self, token):
        r = requests.get(self.config['id_endpoint'], headers={'Authorization': 'Bearer ' + token})
        if not r.ok:
            raise APIAuthProviderException('User token not valid')
        identity = json.loads(r.content)
        uid = identity.get('email')
        if not uid:
            raise APIAuthProviderException('Auth provider did not provide user email')

        self.ensure_user_exists(uid)
        self.set_user_avatar(uid, identity)
        self.set_user_gravatar(uid, uid)

        return uid

    def set_user_avatar(self, uid, identity):
        # A google-specific avatar URL is provided in the identity return.
        provider_avatar = identity.get('picture', '')

        # Remove attached size param from URL.
        u = urlparse.urlparse(provider_avatar)
        query = urlparse.parse_qs(u.query)
        query.pop('sz', None)
        u = u._replace(query=urllib.urlencode(query, True))
        provider_avatar = urlparse.urlunparse(u)

        timestamp = datetime.datetime.utcnow()
        # Update the user's provider avatar if it has changed.
        config.db.users.update_one({'_id': uid, 'avatars.provider': {'$ne': provider_avatar}}, {'$set':{'avatars.provider': provider_avatar, 'modified': timestamp}})
        # If the user has no avatar set, mark their provider_avatar as their chosen avatar.
        config.db.users.update_one({'_id': uid, 'avatar': {'$exists': False}}, {'$set':{'avatar': provider_avatar, 'modified': timestamp}})


class WechatOAuthProvider(AuthProvider):

    def __init__(self):
        super(WechatOAuthProvider,self).__init__('wechat')

    def validate_code(self, code, **kwargs):
        payload = {
            'appid':        self.config['client_id'],
            'secret':       self.config['client_secret'],
            'code':         code,
            'grant_type':   'authorization_code'
        }
        r = requests.post(self.config['token_endpoint'], params=payload)
        if not r.ok:
            raise APIAuthProviderException('User code not valid')

        response = json.loads(r.content)
        config.log.debug(response)
        openid = response['openid']

        registration_code = kwargs.get('registration_code')
        uid = self.validate_user(openid, registration_code=registration_code)

        return {
            'refresh_token': response['refresh_token'],
            'access_token': response['access_token'],
            'uid': uid,
            'auth_type': self.auth_type,
            'expires': datetime.datetime.utcnow() + datetime.timedelta(seconds=response['expires_in'])
        }

    def refresh_token(self, token):
        payload = {
            'appid':            self.config['client_id'],
            'refresh_token':    token,
            'grant_type':       'refresh_token'
        }
        r = requests.post(self.config['refresh_endpoint'], params=payload)
        if not r.ok:
            raise APIAuthProviderException('Unable to refresh token.')

        response = json.loads(r.content)
        return {
            'refresh_token': response['refresh_token'],
            'access_token': response['access_token'],
            'expires': datetime.datetime.utcnow() + datetime.timedelta(seconds=response['expires_in'])
        }

    def validate_user(self, openid, registration_code=None):
        if registration_code:
            user = config.db.users.find_one({'wechat.registration_code': registration_code})
            if user is None:
                raise APIUnknownUserException('Invalid or expired registration code.')
            update = {
                '$set': {
                    'wechat.openid': openid
                },
                '$unset': {
                    'wechat.registration_code':''
                }
            }
            config.db.users.update_one({'_id': user['_id']}, update)
        else:
            user = config.db.users.find_one({'wechat.openid': openid})
        if not user:
            raise APIUnknownUserException('User not registered.')
        if user.get('disabled', False) is True:
            raise APIUnknownUserException('User {} is disabled'.format(user['_id']))

        return user['_id']

    def set_user_avatar(self, uid, identity):
        pass

AuthProviders = {
    'google'    : GoogleOAuthProvider,
    'ldap'      : JWTAuthProvider,
    'wechat'    : WechatOAuthProvider
}
