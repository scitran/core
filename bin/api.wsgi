# vim: filetype=python

import os
import time
import logging
import argparse
import datetime


os.environ['PYTHON_EGG_CACHE'] = '/tmp/python_egg_cache'
os.umask(0o022)

ap = argparse.ArgumentParser()
ap.add_argument('--db_uri', help='SciTran DB URI', default='mongodb://localhost/scitran')
ap.add_argument('--data_path', help='path to storage area', required=True)
ap.add_argument('--ssl', action='store_true', help='enable SSL')
ap.add_argument('--ssl_cert', default='*', help='path to SSL key and cert file')
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

args.quarantine_path = os.path.join(args.data_path, 'quarantine')
args.upload_path = os.path.join(args.data_path, 'upload')
args.ssl = args.ssl or args.ssl_cert != '*'

from api import mongo
mongo.configure_db(args.db_uri)

# imports delayed after mongo has been fully initialized
from api.util import log
from api import api
from api import centralclient, jobs
from api import jobs

try:
    import newrelic.agent
    newrelic.agent.initialize('../../newrelic.ini')
    log.info('New Relic detected and loaded. Monitoring enabled.')
except ImportError:
    log.info('New Relic not detected. Monitoring disabled.')
except newrelic.api.exceptions.ConfigurationError:
    log.warn('New Relic detected but configuration was not valid. Please ensure newrelic.ini is present. Monitoring disabled.')

log.setLevel(getattr(logging, args.log_level.upper()))

api.app.config = vars(args)

centralclient_enabled = True
if not api.app.config['ssl_cert']:
    centralclient_enabled = False
    log.warning('ssl_cert not configured -> SciTran Central functionality disabled')
if not api.app.config['central_uri']:
    centralclient_enabled = False
    log.warning('central_uri not configured -> SciTran Central functionality disabled')
if not api.app.config['drone_secret']:
    log.warning('drone_secret not configured -> Drone functionality disabled')
if not os.path.exists(api.app.config['data_path']):
    os.makedirs(api.app.config['data_path'])
if not os.path.exists(api.app.config['quarantine_path']):
    os.makedirs(api.app.config['quarantine_path'])
if not os.path.exists(api.app.config['upload_path']):
    os.makedirs(api.app.config['upload_path'])


# FIXME All code shoud use the mongo module and this line should be deleted.
api.app.db = mongo.db


if __name__ == '__main__':
    api.app.debug = True # send stack trace for uncaught exceptions to client
    paste.httpserver.serve(api.app, host=args.host, port=args.port, ssl_pem=args.ssl_cert)
else:
    application = api.app # needed for uwsgi

    import uwsgidecorators

    if centralclient_enabled:
        fail_count = 0
        @uwsgidecorators.timer(60)
        def centralclient_timer(signum):
            global fail_count
            if not centralclient.update(mongo.db, args.ssl_cert, args.central_uri):
                fail_count += 1
            else:
                fail_count = 0
            if fail_count == 3:
                log.warning('scitran central unreachable, purging all remotes info')
                centralclient.clean_remotes(mongo.db)

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
                    'modified': {'$lt': datetime.datetime.utcnow() - datetime.timedelta(seconds=100)},
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
