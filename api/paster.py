import os
import sys
import logging
import pymongo
import datetime

from . import api
from .util import log

logging.getLogger('scitran.data').setLevel(logging.WARNING) # silence scitran.data logging
logging.getLogger('paste.httpserver').setLevel(logging.WARNING) # silence paster

os.environ['PYTHON_EGG_CACHE'] = '/tmp/python_egg_cache'
os.umask(0o022)


def app_factory(global_config, **config):
    config = process_config(config, log)
    api.app.config = config
    api.app.db = pymongo.MongoClient(host=config['db_uri'], j=True).get_default_database()
    log.setLevel(getattr(logging, config['log_level'].upper()))

    api.app.db.projects.create_index([('gid', 1), ('name', 1)])
    api.app.db.sessions.create_index('project')
    api.app.db.sessions.create_index('uid')
    api.app.db.acquisitions.create_index('session')
    api.app.db.acquisitions.create_index('uid')
    api.app.db.acquisitions.create_index('collections')
    api.app.db.authtokens.create_index('timestamp', expireAfterSeconds=600)
    api.app.db.uploads.create_index('timestamp', expireAfterSeconds=60)
    api.app.db.downloads.create_index('timestamp', expireAfterSeconds=60)

    now = datetime.datetime.utcnow()
    api.app.db.groups.update_one({'_id': 'unknown'}, {'$setOnInsert': { 'created': now, 'modified': now, 'name': 'Unknown', 'roles': []}}, upsert=True)
    api.app.db.sites.replace_one({'_id': config['site_id']}, {'name': config['site_name'], 'api_uri': config['api_uri']}, upsert=True)

    if config.get('new_relic'):
        try:
            import newrelic.agent, newrelic.api.exceptions
            newrelic.agent.initialize(config.new_relic)
            api.app = newrelic.agent.WSGIApplicationWrapper(api.app)
            log.info('New Relic detected and loaded. Monitoring enabled.')
        except ImportError:
            log.critical('New Relic libraries not found.')
            sys.exit(1)
        except newrelic.api.exceptions.ConfigurationError:
            log.critical('New Relic detected, but configuration invalid.')
            sys.exit(1)

    return api.app


def process_config(config, log):
    if 'log_level' not in config:
        config['log_level'] = 'info'
    # SciTran DB URI
    if 'db_uri' not in config:
        config['db_uri'] = 'mongodb://localhost:9001/scitran'
    # Site ID for Scitran Central [local]
    if config.get('site_id', 'local') == 'local':
        config['site_id'] = 'local'
        log.warning('site_id not configured -> SciTran Central functionality disabled')
    if not config.get('site_name'):
        config['site_name'] = 'Local'
    # OAuth2 provider ID endpoint
    if 'oauth2_id_endpoint' not in config:
        config['oauth2_id_endpoint'] = 'https://www.googleapis.com/plus/v1/people/me/openIdConnect'
    # allow user info as urlencoded param
    if 'insecure' not in config:
        config['insecure'] = False
    # shared drone secret
    if 'drone_secret' not in config:
        config['drone_secret'] = None
        log.warning('drone_secret not configured -> Drone functionality disabled')
    if not config.get('data_path'):
        raise Exception('data_path - path to storage area must be sepecified')
    if not config.get('ssl_cert'):
        config['ssl_cert'] = None
        config['ssl'] = False
        log.warning('ssl_cert not configured -> SciTran Central functionality disabled')
    else:
        config['ssl'] = True
    if not config.get('api_uri'):
        raise Exception('api_uri - api uri, with https:// prefix')
    if not config.get('central_uri'):
        config['central_uri'] = 'https://sdmc.scitran.io/api'

    config['quarantine_path'] = os.path.join(config['data_path'], 'quarantine')
    config['upload_path'] = os.path.join(config['data_path'], 'upload')

    # Create necessary directories based on config
    if not os.path.exists(config['data_path']):
        os.makedirs(config['data_path'])
    if not os.path.exists(config['quarantine_path']):
        os.makedirs(config['quarantine_path'])
    if not os.path.exists(config['upload_path']):
        os.makedirs(config['upload_path'])

    return config
