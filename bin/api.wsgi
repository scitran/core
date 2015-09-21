# vim: filetype=python

import os
import time
import logging
import pymongo
import argparse
import datetime

from api import app, centralclient, jobs
from api.util import log
from api import jobs

logging.getLogger('scitran.data').setLevel(logging.WARNING) # silence scitran.data logging

try:
    import newrelic.agent
    newrelic.agent.initialize('../../newrelic.ini')
    log.info('New Relic detected and loaded. Monitoring enabled.')
except ImportError:
    log.info('New Relic not detected. Monitoring disabled.')
except newrelic.api.exceptions.ConfigurationError:
    log.warn('New Relic detected but configuration was not valid. Please ensure newrelic.ini is present. Monitoring disabled.')


os.environ['PYTHON_EGG_CACHE'] = '/tmp/python_egg_cache'
os.umask(0o022)

ap = argparse.ArgumentParser()
ap.add_argument('--db_uri', help='SciTran DB URI', default='mongodb://localhost/scitran')
ap.add_argument('--data_path', help='path to storage area', required=True)
ap.add_argument('--apps_path', help='path to apps storage')
ap.add_argument('--ssl_cert', help='path to SSL certificate file, containing private key and certificate chain', required=True)
ap.add_argument('--api_uri', help='api uri, with https:// prefix')
ap.add_argument('--site_id', help='site ID for Scitran Central [local]', default='local')
ap.add_argument('--site_name', help='site name', nargs='*', default=['Local'])  # hack for uwsgi --pyargv
ap.add_argument('--oauth2_id_endpoint', help='OAuth2 provider ID endpoint', default='https://www.googleapis.com/plus/v1/people/me/openIdConnect')
ap.add_argument('--insecure', help='allow user info as urlencoded param', action='store_true', default=False)
ap.add_argument('--central_uri', help='scitran central api', default='https://sdmc.scitran.io/api')
ap.add_argument('--log_level', help='log level [info]', default='info')
ap.add_argument('--drone_secret', help='shared drone secret')

if __name__ == '__main__':
    import paste.httpserver
    logging.getLogger('paste.httpserver').setLevel(logging.WARNING) # silence paste logging

    ap.add_argument('--host', default='127.0.0.1', help='IP address to bind to [127.0.0.1]')
    ap.add_argument('--port', default='8080', help='TCP port to listen on [8080]')

args = ap.parse_args()
log.setLevel(getattr(logging, args.log_level.upper()))

# uwsgi-related HACK to allow --site_name 'Example Site' or --site_name "Example Site"
args.site_name = ' '.join(args.site_name).strip('"\'')

args.quarantine_path = os.path.join(args.data_path, 'quarantine')
args.upload_path = os.path.join(args.data_path, 'upload')

app.config = vars(args)

centralclient_enabled = True
if not app.config['ssl_cert']:
    centralclient_enabled = False
    log.warning('ssl_cert not configured -> SciTran Central functionality disabled')
if app.config['site_id'] == 'local':
    centralclient_enabled = False
    log.warning('site_id not configured -> SciTran Central functionality disabled')
elif not app.config['api_uri']:
    centralclient_enabled = False
    log.warning('api_uri not configured -> SciTran Central functionality disabled')
if not app.config['central_uri']:
    centralclient_enabled = False
    log.warning('central_uri not configured -> SciTran Central functionality disabled')
if not app.config['drone_secret']:
    log.warning('drone_secret not configured -> Drone functionality disabled')
if not app.config['apps_path']:
    log.warning('apps_path is not defined -> App functionality disabled')
elif not os.path.exists(app.config['apps_path']):
    os.makedirs(app.config['apps_path'])
if not os.path.exists(app.config['data_path']):
    os.makedirs(app.config['data_path'])
if not os.path.exists(app.config['quarantine_path']):
    os.makedirs(app.config['quarantine_path'])
if not os.path.exists(app.config['upload_path']):
    os.makedirs(app.config['upload_path'])

for x in range(10):
    try:
        app.db = pymongo.MongoClient(args.db_uri).get_default_database()
    except:
        time.sleep(6)
    else:
        break
else:
    raise Exception('Could not connect to MongoDB')

# TODO jobs indexes
# TODO review all indexes
app.db.projects.create_index([('gid', 1), ('name', 1)])
app.db.sessions.create_index('project')
app.db.sessions.create_index('uid')
app.db.acquisitions.create_index('session')
app.db.acquisitions.create_index('uid')
app.db.acquisitions.create_index('collections')
app.db.authtokens.create_index('timestamp', expireAfterSeconds=600)
app.db.uploads.create_index('timestamp', expireAfterSeconds=60)
app.db.downloads.create_index('timestamp', expireAfterSeconds=60)

now = datetime.datetime.utcnow()
app.db.groups.update_one({'_id': 'unknown'}, {'$setOnInsert': { 'created': now, 'modified': now, 'name': 'Unknown', 'roles': []}}, upsert=True)
app.db.sites.replace_one({'_id': args.site_id}, {'name': args.site_name, 'api_uri': args.api_uri}, upsert=True)


if __name__ == '__main__':
    app.debug = True # send stack trace for uncaught exceptions to client
    paste.httpserver.serve(app, host=args.host, port=args.port, ssl_pem=args.ssl_cert)
else:
    application = app # needed for uwsgi

    import uwsgidecorators

    @uwsgidecorators.cron(0, -1, -1, -1, -1)  # top of every hour
    def upload_storage_cleaning(signum):
        upload_path = application.config['upload_path']
        for f in os.listdir(upload_path):
            fp = os.path.join(upload_path, f)
            timestamp = datetime.datetime.utcfromtimestamp(int(os.stat(fp).st_mtime))
            if timestamp < (datetime.datetime.utcnow() - datetime.timedelta(hours=1)):
                log.debug('upload %s was last modified %s' % (fp, str(timestamp)))
                os.remove(fp)

    if centralclient_enabled:
        fail_count = 0
        @uwsgidecorators.timer(60)
        def centralclient_timer(signum):
            global fail_count
            if not centralclient.update(application.db, args.api_uri, args.site_name, args.site_id, args.ssl_cert, args.central_uri):
                fail_count += 1
            else:
                fail_count = 0
            if fail_count == 3:
                log.warning('scitran central unreachable, purging all remotes info')
                centralclient.clean_remotes(application.db, args.site_id)

    def job_creation(signum):
        for c_type in ['projects', 'collections', 'sessions', 'acquisitions']:
            for c in application.db[c_type].find({'files.dirty': True}, ['files']):
                containers = [(c_type, c)] # TODO: this should be the full container hierarchy
                for f in c['files']:
                    if f.get('dirty'):
                        jobs.spawn_jobs(application.db, containers, f)
                        r = application.db[c_type].update_one(
                                {
                                    '_id': c['_id'],
                                    'files': {
                                        '$elemMatch': {
                                            'filename': f['filename'],
                                            'filehash': f['filehash'],
                                        },
                                    },
                                },
                                {
                                    '$set': {
                                        'files.$.dirty': False,
                                    },
                                },
                                )
                        if not r.matched_count:
                            log.info('file modified or removed, not marked as clean: %s %s, %s' % (c_type, c, f['filename']))
        while True:
            j = application.db.jobs.find_one_and_update(
                {
                    'state': 'running',
                    'heartbeat': {'$lt': datetime.datetime.utcnow() - datetime.timedelta(seconds=100)},
                },
                {
                    '$set': {
                        'state': 'failed',
                    },
                },
                )
            if j is None:
                break
            else:
                jobs.retry_job(application.db, j)


    # Run job creation immediately on start, then every 30 seconds thereafter.
    # This saves sysadmin irritation waiting for the engine, or an initial job load conflicting with timed runs.
    log.info('Loading jobs queue for initial processing. This may take some time.')
    job_creation(None)
    log.info('Loading jobs queue complete.')

    @uwsgidecorators.timer(30)
    def job_creation_timer(signum):
        job_creation(signum)
