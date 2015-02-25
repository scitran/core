# @author:  Gunnar Schaefer, Kevin S. Hahn

import os
import time
import logging
import pymongo
import argparse
import uwsgidecorators

import api
import internimsclient

os.environ['PYTHON_EGG_CACHE'] = '/tmp/python_egg_cache'
os.umask(0o022)

ap = argparse.ArgumentParser()
ap.add_argument('--db_uri', help='mongodb uri', required=True)
ap.add_argument('--data_path', help='path to data', required=True)
ap.add_argument('--log_path', help='path to log', required=True)     # for SHOWING the log, not where to write
ap.add_argument('--ssl_cert', help='path to ssl cert in pem format, key+cert', required=True)
ap.add_argument('--api_uri', help='api uri')
ap.add_argument('--site_id', help='site id')
ap.add_argument('--site_name', help='site name', nargs='+')
ap.add_argument('--oauth2_id_endpoint', help='oauth2 id endpoint url', default='https://www.googleapis.com/plus/v1/people/me/openIdConnect')
ap.add_argument('--demo', help='demo mode, enables auto user creation', action='store_true', default=False)
ap.add_argument('--insecure', help='insecure mode', action='store_true', default=False)
ap.add_argument('--central_uri', help='scitran central api', default='https://sdmc.scitran.io/api')
ap.add_argument('--log_level', help='log level [info]', default='info')
args = ap.parse_args()
args.site_name = ' '.join(args.site_name) if args.site_name else None  # site_name as string

logging.basicConfig(level=getattr(logging, args.log_level.upper()))
log = logging.getLogger('nimsapi')

# configure uwsgi application
application = api.app
application.config['site_id'] = args.site_id
application.config['site_name'] = args.site_name
application.config['data_path'] = os.path.join(args.data_path, 'nims')
application.config['quarantine_path'] = os.path.join(args.data_path, 'quarantine')
application.config['log_path'] = args.log_path
application.config['ssl_cert'] = args.ssl_cert
application.config['oauth2_id_endpoint'] = args.oauth2_id_endpoint
application.config['insecure'] = args.insecure
application.config['demo'] = args.demo

if not os.path.exists(application.config['data_path']):
    os.makedirs(application.config['data_path'])
if not os.path.exists(application.config['quarantine_path']):
    os.makedirs(application.config['quarantine_path'])

# connect to db
kwargs = dict(tz_aware=True)
application.db = None
for x in range(0, 30):
    try:
        db_client = pymongo.MongoReplicaSetClient(args.db_uri, **kwargs) if 'replicaSet' in args.db_uri else pymongo.MongoClient(args.db_uri, **kwargs)
        application.db = db_client.get_default_database()
    except:
        time.sleep(1)
        pass
    else:
        break
else:
    raise Exception('Could not connect to MongoDB')

if not args.ssl_cert:
    log.warning('SSL certificate not specified, Scitran Central functionality disabled')
elif not args.api_uri:
    log.warning('api_uri not configured. scitran central functionality disabled.')
elif not args.site_name:
    log.warning('site_name not configured. scitran central functionality disabled.')
elif not args.site_id:
    log.warning('site_id not configured. scitran central functionality disabled.')
elif not args.central_uri:
    log.warning('central_uri not configured. scitran central functionality disabled.')
else:
    fail_count = 0

    @uwsgidecorators.timer(60)
    def internimsclient_timer(signum):
        global fail_count
        if not internimsclient.update(application.db, args.api_uri, args.site_name, args.site_id, args.ssl_cert, args.central_uri):
            fail_count += 1
        else:
            fail_count = 0

        if fail_count == 3:
            log.debug('InterNIMS unreachable, purging all remotes info')
            internimsclient.clean_remotes(application.db)
