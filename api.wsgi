# @author:  Gunnar Schaefer, Kevin S. Hahn

import os
import sys
import site
import time

import ConfigParser

configfile = '../production.ini'
config = ConfigParser.ConfigParser(allow_no_value=True)
config.read(configfile)

site.addsitedir(os.path.join(config.get('nims', 'virtualenv'), 'lib/python2.7/site-packages'))
sys.path.append(config.get('nims', 'here'))
os.environ['PYTHON_EGG_CACHE'] = config.get('nims', 'python_egg_cache')
os.umask(0o022)

import pymongo
import uwsgidecorators

import logging
import logging.config
logging.config.fileConfig(configfile, disable_existing_loggers=False)
log = logging.getLogger('nimsapi')

import api
import internimsclient

# configure uwsgi application
application = api.app
application.config['data_path'] = os.path.join(config.get('nims', 'data_path'), 'nims')
application.config['quarantine_path'] = os.path.join(config.get('nims', 'data_path'), 'quarantine')
application.config['log_path'] = config.get('nims', 'log_path')
application.config['site_name'] = config.get('nims', 'site_name')
application.config['site_id'] = config.get('nims', 'site_id')
application.config['ssl_cert'] = config.get('nims', 'ssl_cert')
application.config['oauth2_id_endpoint'] = config.get('oauth2', 'id_endpoint')
application.config['insecure'] = config.getboolean('nims', 'insecure')

if not os.path.exists(application.config['data_path']):
    os.makedirs(application.config['data_path'])
if not os.path.exists(application.config['quarantine_path']):
    os.makedirs(application.config['quarantine_path'])

# connect to db
kwargs = dict(tz_aware=True)
db_uri = config.get('nims', 'db_uri')
db_client = None
application.db = None

for x in range(0, 30):
    try:
        db_client = pymongo.MongoReplicaSetClient(db_uri, **kwargs) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri, **kwargs)
        application.db = db_client.get_default_database()
    except:
        time.sleep(1)
        pass
    else:
        break
else:
    raise Exception("Could not connect to MongoDB")


# internims, send is-alive signals
site_id = config.get('nims', 'site_id')
site_name = config.get('nims', 'site_name')
ssl_cert = config.get('nims', 'ssl_cert')
api_uri = config.get('nims', 'api_uri')
try:
    internims_url = config.get('nims', 'internims_url')
except ConfigParser.NoOptionError:
    internims_url = None
fail_count = 0

if not internims_url or internims_url == u'':
    log.debug('internims url not configured. internims disabled.')
else:
    @uwsgidecorators.timer(60)
    def internimsclient_timer(signum):
        global fail_count
        if not internimsclient.update(application.db, api_uri, site_name, site_id, ssl_cert, internims_url):
            fail_count += 1
        else:
            fail_count = 0

        if fail_count == 3:
            log.debug('InterNIMS unreachable, purging all remotes info')
            internimsclient.clean_remotes(application.db)
