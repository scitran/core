# @author:  Gunnar Schaefer, Kevin S. Hahn

import os
import time
import logging
import pymongo
import argparse
import uwsgidecorators

import api
import centralclient

os.environ['PYTHON_EGG_CACHE'] = '/tmp/python_egg_cache'
os.umask(0o022)

ap = argparse.ArgumentParser()
ap.add_argument('--db_uri', help='SciTran DB URI', required=True)
ap.add_argument('--data_path', help='path to storage area', required=True)
ap.add_argument('--ssl_cert', help='path to SSL certificate file, containing private key and certificate chain', required=True)
ap.add_argument('--api_uri', help='api uri, with https:// prefix')
ap.add_argument('--site_id', help='site ID for Scitran Central')
ap.add_argument('--site_name', help='site name')
ap.add_argument('--oauth2_id_endpoint', help='OAuth2 provider ID endpoint', default='https://www.googleapis.com/plus/v1/people/me/openIdConnect')
ap.add_argument('--demo', help='enable automatic user creation', action='store_true', default=False)
ap.add_argument('--insecure', help='allow user info as urlencoded param', action='store_true', default=False)
ap.add_argument('--central_uri', help='scitran central api', default='https://sdmc.scitran.io/api')
ap.add_argument('--log_level', help='log level [info]', default='info')
args = ap.parse_args()

args.data_path = os.path.join(args.data_path, 'scitran')
args.quarantine_path = os.path.join(args.data_path, 'quarantine')

logging.basicConfig(level=getattr(logging, args.log_level.upper())) #FIXME probably not necessary, because done in api.py
log = logging.getLogger('nimsapi')

# configure uwsgi application
application = api.app
application.config = vars(args)

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
    def centralclient_timer(signum):
        global fail_count
        if not centralclient.update(application.db, args.api_uri, args.site_name, args.site_id, args.ssl_cert, args.central_uri):
            fail_count += 1
        else:
            fail_count = 0

        if fail_count == 3:
            log.debug('scitran central unreachable, purging all remotes info')
            centralclient.clean_remotes(application.db)
