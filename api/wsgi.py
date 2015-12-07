# vim: filetype=python

import os
import sys

os.environ['PYTHON_EGG_CACHE'] = '/tmp/python_egg_cache'
os.umask(0o022)

def app_factory(global_config, **config):
    args = config
    from . import mongo
    mongo.configure_db(args['db_uri'])

    # imports delayed after mongo has been fully initialized
    from . import api
    from . import config

    log = config.log
    args = process_config(args, log)
    config.set_log_level(log, args['log_level'])

    api.app.config = args

    if 'new_relic' in args and args['new_relic'] is not None:
        try:
            import newrelic.agent, newrelic.api.exceptions
            newrelic.agent.initialize(args.new_relic)
            api.app = newrelic.agent.WSGIApplicationWrapper(api.app)
            log.info('New Relic detected and loaded. Monitoring enabled.')
        except ImportError:
            log.critical('New Relic libraries not found.')
            sys.exit(1)
        except newrelic.api.exceptions.ConfigurationError:
            log.critical('New Relic detected, but configuration invalid.')
            sys.exit(1)

    api.app.db = mongo.db

    return api.app

def process_config(config, log):
    if 'log_level' not in config:
        config['log_level'] = 'info'
    # SciTran DB URI
    if 'db_uri' not in config:
        config['db_uri'] = 'mongodb://localhost/scitran'
    # Site ID for Scitran Central [local]
    if 'site_id' not in config or config['site_id'] == 'local':
        config['site_id'] = 'local'
        log.warning('site_id not configured -> SciTran Central functionality disabled')
    if 'site_name' not in config:
        config['site_name'] = 'Local'
    # OAuth2 provider ID endpoint
    if 'oauth2_id_endpoint' not in config:
        config['oauth2_id_endpoint'] = 'https://www.googleapis.com/plus/v1/people/me/openIdConnect'
    # allow user info as urlencoded param
    if 'insecure' not in config:
        config['insecure'] = False
    # shared drone secret
    if 'drone_secret' not in config:
        config['drone_secret'] = ''
        log.warning('drone_secret not configured -> Drone functionality disabled')
    if 'data_path' not in config or not config['data_path']:
        raise Exception('data_path - path to storage area must be sepecified')
    if 'ssl_cert' not in config or not config['ssl_cert'] or config['ssl_cert'] != '*':
    #     raise Exception('ssl_cert - path to SSL cert must be specified')
        config['ssl_cert'] = ''
        config['ssl'] = False
        log.warning('ssl_cert not configured -> SciTran Central functionality disabled')
    else:
        config['ssl'] = True
    if 'api_uri' not in config or not config['api_uri']:
        raise Exception('api_uri - api uri, with https:// prefix')
    if 'central_uri' not in config or not config['central_uri']:
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
