import bson
import datetime

from . import APIAuthProviderException
from .. import config, util

log = config.log

class APIKey(object):
    """
    Abstract API key class
    """

    @staticmethod
    def _preprocess_key(key):
        """
        Convention for API keys is that they can have arbitrary information, separated by a :,
        before the actual key. Generally, this will have a connection string in it.
        Strip this preamble, if any, before processing the key.
        """

        return key.split(":")[-1] # Get the last segment of the string after any : separators

    @staticmethod
    def validate(key):
        """
        AuthN for user accounts via api key.

        401s via APIAuthProviderException on failure.
        """
        key = APIKey._preprocess_key(key)

        timestamp = datetime.datetime.utcnow()
        api_key = config.db.apikeys.find_one_and_update({'_id': key}, {'$set': {'last_used': timestamp}})

        if api_key:

            # Some api keys may have additional requirements that must be met
            try:
                APIKeyTypes[api_key['type']].check(api_key)
            except KeyError:
                log.warning('Unknown API key type ({})'.format(api_key.get('type')))
                APIAuthProviderException('Invalid API key')

            return api_key

        else:
            raise APIAuthProviderException('Invalid API key')

    @staticmethod
    def generate_api_key(key_type):
        return {
            '_id': util.create_nonce(),
            'created': datetime.datetime.utcnow(),
            'type': key_type,
            'last_used': None
        }


class UserApiKey(APIKey):

    key_type = 'user'

    @classmethod
    def generate(cls, uid):
        """
        Generates API key for user, replaces existing API key if exists
        """
        api_key = cls.generate_api_key(cls.key_type)
        api_key['uid'] = uid
        config.db.apikeys.delete_many({'uid': uid, 'type': cls.key_type})
        config.db.apikeys.insert_one(api_key)
        return api_key['_id']

    @classmethod
    def get(cls, uid):
        return config.db.apikeys.find_one({'uid': uid, 'type': cls.key_type})

    @classmethod
    def check(cls, api_key):
        pass

class JobApiKey(APIKey):
    """
    API key that grants API access as a specified user during execution of a job
    Job must be in 'running' state to user API key
    """

    key_type = 'job'

    @classmethod
    def generate(cls, uid, job_id):
        """
        Generates an API key for user for use by a specific job
        """
        api_key = cls.generate_api_key(cls.key_type)
        api_key['uid'] = uid
        api_key['job'] = job_id

        config.db.apikeys.insert_one(api_key)
        return api_key['_id']

    @classmethod
    def remove(cls, job_id):
        config.db.apikeys.delete({'type': cls.key_type, 'job': bson.ObjectId(job_id)})

    @classmethod
    def check(cls, api_key):
        job_id = api_key['job']
        if config.db.jobs.count({'_id': bson.ObjectId(job_id), 'state': 'running'}) != 1:
            raise APIAuthProviderException('Use of API key requires job to be in progress')


APIKeyTypes = {
    'user'    	: UserApiKey,
    'job'      	: JobApiKey
}
