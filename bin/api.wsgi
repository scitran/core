# vim: filetype=python

from api import api

application = api.app_factory()


# FIXME: all code below should removed and ported into an app server independent framework

import datetime
import uwsgidecorators

from api import rules
from api import config
from api import jobs

log = config.log


if config.get_item('site', 'registered'):
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

       # This projection needs every field required to know what type of container it is & navigate to its project
       containers = config.db[c_type].find({'files.unprocessed': True}, ['files', 'session', 'project'])

       for c in containers:
            for f in c['files']:
                if f.get('unprocessed'):
                    rules.create_jobs(config.db, c, c_type, f)
                    r = config.db[c_type].update_one(
                            {
                                '_id': c['_id'],
                                'files': {
                                    '$elemMatch': {
                                        'name': f['name'],
                                        'hash': f['hash'],
                                    },
                                },
                            },
                            {
                                '$set': {
                                    'files.$.unprocessed': False,
                                },
                            },
                            )
                    if not r.matched_count:
                        log.info('file modified or removed, not marked as clean: %s %s, %s' % (c_type, c, f['name']))
    while True:
        j = config.db.jobs.find_one_and_update(
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
            jobs.retry_job(config.db, j)


# Run job creation immediately on start, then every 30 seconds thereafter.
# This saves sysadmin irritation waiting for the engine, or an initial job load conflicting with timed runs.
log.info('Loading jobs queue for initial processing. This may take some time.')
job_creation(None)
log.info('Loading jobs queue complete.')

@uwsgidecorators.timer(30)
def job_creation_timer(signum):
    job_creation(signum)
