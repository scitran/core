# @author:  Gunnar Schaefer, Kevin S. Hahn

import os
import re
import time
import logging
import pymongo
import argparse
import datetime
import uwsgidecorators

import api
import centralclient

os.environ['PYTHON_EGG_CACHE'] = '/tmp/python_egg_cache'
os.umask(0o022)

ap = argparse.ArgumentParser()
ap.add_argument('--db_uri', help='SciTran DB URI', required=True)
ap.add_argument('--data_path', help='path to storage area', required=True)
ap.add_argument('--apps_path', help='path to apps storage')
ap.add_argument('--ssl_cert', help='path to SSL certificate file, containing private key and certificate chain', required=True)
ap.add_argument('--api_uri', help='api uri, with https:// prefix')
ap.add_argument('--site_id', help='site ID for Scitran Central [local]', default='local')
ap.add_argument('--site_name', help='site name', nargs='*', default=['Local'])  # hack for uwsgi --pyargv
ap.add_argument('--oauth2_id_endpoint', help='OAuth2 provider ID endpoint', default='https://www.googleapis.com/plus/v1/people/me/openIdConnect')
ap.add_argument('--demo', help='enable automatic user creation', action='store_true', default=False)
ap.add_argument('--insecure', help='allow user info as urlencoded param', action='store_true', default=False)
ap.add_argument('--central_uri', help='scitran central api', default='https://sdmc.scitran.io/api')
ap.add_argument('--log_level', help='log level [info]', default='info')
args = ap.parse_args()

# HACK to allow setting the --site_name in the same way as api.py
# --site_name 'Example Site' or --site_name "Example Site"
args.site_name = ' '.join(args.site_name).strip('"\'')
args.quarantine_path = os.path.join(args.data_path, 'quarantine')
args.upload_path = os.path.join(args.data_path, 'upload')

logging.basicConfig(level=getattr(logging, args.log_level.upper())) #FIXME probably not necessary, because done in api.py
log = logging.getLogger('scitran')

# configure uwsgi application
application = api.app
application.config = vars(args)

if not os.path.exists(application.config['data_path']):
    os.makedirs(application.config['data_path'])
if not os.path.exists(application.config['quarantine_path']):
    os.makedirs(application.config['quarantine_path'])
if not os.path.exists(application.config['upload_path']):
    os.makedirs(application.config['upload_path'])
if not application.config['apps_path']:
    log.warning('apps_path is not defined.  Apps functionality disabled')
else:
    if not os.path.exists(application.config['apps_path']):
        os.makedirs(application.config['apps_path'])

# connect to db
application.db = None
for x in range(0, 30):
    try:
        db_client = pymongo.MongoReplicaSetClient(args.db_uri) if 'replicaSet' in args.db_uri else pymongo.MongoClient(args.db_uri)
        application.db = db_client.get_default_database()
    except:
        time.sleep(1)
        pass
    else:
        break
else:
    raise Exception('Could not connect to MongoDB')

# TODO: make api_uri a required arg?
application.db.sites.update({'_id': args.site_id}, {'_id': args.site_id, 'name': args.site_name, 'api_uri': args.api_uri}, upsert=True)

@uwsgidecorators.cron(0, -1, -1, -1, -1)  # top of every hour
def upload_storage_cleaning(num):
    upload_path = application.config['upload_path']
    for f in os.listdir(upload_path):
        fp = os.path.join(upload_path, f)
        timestamp = datetime.datetime.utcfromtimestamp(int(os.stat(fp).st_mtime))
        if timestamp < (datetime.datetime.utcnow() - datetime.timedelta(hours=1)):
            log.debug('upload %s was last modified %s' % (fp, str(timestamp)))
            os.remove(fp)

if not args.ssl_cert:
    log.warning('SSL certificate not specified, Scitran Central functionality disabled')
elif not args.api_uri:
    log.warning('api_uri not configured. scitran central functionality disabled.')
elif not args.site_name:
    log.warning('site_name not configured. scitran central functionality disabled.')
elif args.site_id == 'local':
    log.warning('site_id is local. scitran central functionality disabled.')
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
            centralclient.clean_remotes(application.db, args.site_id)
