# vim: filetype=python

import logging
logging.basicConfig(
        format='%(asctime)s %(name)16.16s:%(levelname)4.4s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        )
log = logging.getLogger('scitran.api')
logging.getLogger('scitran.data').setLevel(logging.WARNING) # silence scitran.data logging

try:
    import newrelic.agent
    newrelic.agent.initialize('../../newrelic.ini')
    log.info('New Relic detected and loaded. Monitoring enabled.')
except ImportError:
    log.info('New Relic not detected. Monitoring disabled.')
except newrelic.api.exceptions.ConfigurationError:
    log.warn('New Relic detected but configuration was not valid. Please ensure newrelic.ini is present. Monitoring disabled.')

import os
import time
import pymongo
import argparse

from api import api, centralclient, jobs


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

api.app.config = vars(args)

centralclient_enabled = True
if not api.app.config['ssl_cert']:
    centralclient_enabled = False
    log.warning('ssl_cert not configured -> SciTran Central functionality disabled')
if api.app.config['site_id'] == 'local':
    centralclient_enabled = False
    log.warning('site_id not configured -> SciTran Central functionality disabled')
elif not api.app.config['api_uri']:
    centralclient_enabled = False
    log.warning('api_uri not configured -> SciTran Central functionality disabled')
if not api.app.config['central_uri']:
    centralclient_enabled = False
    log.warning('central_uri not configured -> SciTran Central functionality disabled')
if not api.app.config['drone_secret']:
    log.warning('drone_secret not configured -> Drone functionality disabled')
if not api.app.config['apps_path']:
    log.warning('apps_path is not defined -> App functionality disabled')
elif not os.path.exists(api.app.config['apps_path']):
    os.makedirs(api.app.config['apps_path'])
if not os.path.exists(api.app.config['data_path']):
    os.makedirs(api.app.config['data_path'])
if not os.path.exists(api.app.config['quarantine_path']):
    os.makedirs(api.app.config['quarantine_path'])
if not os.path.exists(api.app.config['upload_path']):
    os.makedirs(api.app.config['upload_path'])

for x in range(10):
    try:
        api.app.db = pymongo.MongoClient(args.db_uri).get_default_database()
    except:
        time.sleep(6)
    else:
        break
else:
    raise Exception('Could not connect to MongoDB')

api.app.db.sites.update({'_id': args.site_id}, {'_id': args.site_id, 'name': args.site_name, 'api_uri': args.api_uri}, upsert=True)


if __name__ == '__main__':
    api.app.debug = True # send stack trace for uncaught exceptions to client
    paste.httpserver.serve(api.app, host=args.host, port=args.port, ssl_pem=args.ssl_cert)
else:
    application = api.app # needed for uwsgi

    import datetime
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

    @uwsgidecorators.timer(30)
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
            if j['attempt'] < 3:
                job_id = jobs.queue_job(application.db, j['algorithm_id'], j['container_type'], j['container_id'], j['filename'], j['filehash'], j['attempt']+1, j['_id'])
                log.info('respawned job %s as %s (attempt %d)' % (j['_id'], job_id, j['attempt']+1))
            else:
                log.info('permanently failed job %s (after %d attempts)' % (j['_id'], j['attempt']))
