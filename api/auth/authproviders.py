import requests
import json
from . import APIAuthProviderException
from .. import config, util


log = config.log

class AuthProvider(object):
    """
    This class provides access to mongodb collection elements (called containers).
    It is used by ContainerHandler istances for get, create, update and delete operations on containers.
    Examples: projects, sessions, acquisitions and collections
    """

    def __init__(self, auth_type):
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


class JWTAuthProvider(AuthProvider):

    def __init__(self):
        super(JWTAuthProvider,self).__init__('ldap')

    def validate_code(self, code):
        uid = self.validate_user_exists(code)
        return code, None, uid

    def validate_user_exists(self, token):
        r = requests.post(self.config['id_endpoint'], data={'token': token})
        if not r.ok:
            raise APIAuthProviderException('User token not valid')
        uid = json.loads(r.content).get('mail')
        if not uid:
            raise APIAuthProviderException('Auth provider did not provide user email')
        return uid


class GoogleOAuthProvider(AuthProvider):

    def __init__(self):
        super(GoogleOAuthProvider,self).__init__('google')

    def validate_code(self, code):
        payload = {
            'client_id':        self.config['client_id'],
            'client_secret':    self.config['client_secret'],
            'code':             code,
            'grant_type':       'authorization_code'
        }
        r = requests.post(self.config['token_endpoint'], data=payload)
        if not r.ok:
            raise APIAuthProviderException('User code not valid')

        response = json.loads(r.content)
        token = response['access_token']
        refresh_token = response['refresh_token']
        uid = self.validate_user_exists(token)

        return token, refresh_token, uid

    def validate_user_exists(self, token):
        r = requests.get(self.config['id_endpoint'], headers={'Authorization': 'Bearer ' + token})
        if not r.ok:
            raise APIAuthProviderException('User token not valid')
        uid = json.loads(r.content).get('email')
        if not uid:
            raise APIAuthProviderException('Auth provider did not provide user email')
        return uid


class WechatOAuthProvider(AuthProvider):

    def __init__(self):
        super(WechatOAuthProvider,self).__init__('wechat')

    def validate_code(self, code):
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
        token = response['access_token']
        refresh_token = response['refresh_token']
        uid = response['openid']

        return token, refresh_token, uid

AuthProviders = {
    'google'    : GoogleOAuthProvider,
    'ldap'      : JWTAuthProvider,
    'wechat'    : WechatOAuthProvider
}
