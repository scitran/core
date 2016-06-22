"""
A simple FIFO queue for jobs.
"""

import bson
import copy
import pymongo
import datetime

from .. import config
from .jobs import Job
from .gears import get_gear_by_name

log = config.log

JOB_STATES = [
    'pending',  # Job is queued
    'running',  # Job has been handed to an engine and is being processed
    'failed',   # Job has an expired heartbeat (orphaned) or has suffered an error
    'complete', # Job has successfully completed
]

JOB_STATES_ALLOWED_MUTATE = [
    'pending',
    'running',
]

JOB_TRANSITIONS = [
    'pending --> running',
    'running --> failed',
    'running --> complete',
]

# How many times a job should be retried
def max_attempts():
    return config.get_item('queue', 'max_retries')

# Should a job be retried when explicitly failed.
# Does not affect orphaned jobs.
def retry_on_explicit_fail():
    return config.get_item('queue', 'retry_on_fail')

def valid_transition(from_state, to_state):
    return (from_state + ' --> ' + to_state) in JOB_TRANSITIONS or from_state == to_state

class Queue(object):

    @staticmethod
    def mutate(job, mutation):
        """
        Validate and save a job mutation
        """

        if job.state not in JOB_STATES_ALLOWED_MUTATE:
            raise Exception('Cannot mutate a job that is ' + job.state + '.')

        if 'state' in mutation and not valid_transition(job.state, mutation['state']):
            raise Exception('Mutating job from ' + job.state + ' to ' + mutation['state'] + ' not allowed.')

        # Any modification must be a timestamp update
        mutation['modified'] = datetime.datetime.utcnow()

        # Create an object with all the fields that must not have changed concurrently.
        job_query =  {
            '_id': bson.ObjectId(job._id),
            'state': job.state,
        }

        result = config.db.jobs.update_one(job_query, {'$set': mutation})
        if result.modified_count != 1:
            raise Exception('Job modification not saved')

        # If the job did not succeed, check to see if job should be retried.
        if 'state' in mutation and mutation['state'] == 'failed' and retry_on_explicit_fail():
            job.state = 'failed'
            Queue.retry(job)

    @staticmethod
    def retry(job, force=False):
        """
        Given a failed job, either retry the job or fail it permanently, based on the attempt number.
        Can override the attempt limit by passing force=True.
        """

        if job.attempt >= max_attempts() and not force:
            log.info('Permanently failed job %s (after %d attempts)' % (job._id, job.attempt))
            return

        if job.state != 'failed':
            raise Exception('Can only retry a job that is failed')

        # Race condition: jobs should only be marked as failed once a new job has been spawned for it (if any).
        # No transactions in our database, so we can't do that.
        # Instead, make a best-hope attempt.
        check = config.db.jobs.find_one({'previous_job_id': job._id })
        if check is not None:
            found = Job.load(check)
            raise Exception('Job ' + job._id + ' has already been retried as ' + str(found._id))

        new_job = copy.deepcopy(job)
        new_job._id = None
        new_job.previous_job_id = job._id

        new_job.state = 'pending'
        new_job.attempt += 1

        now = datetime.datetime.utcnow()
        new_job.created = now
        new_job.modified = now

        new_id = new_job.insert()
        log.info('respawned job %s as %s (attempt %d)' % (job._id, new_id, new_job.attempt))

        return new_id

    @staticmethod
    def start_job(tags=None):
        """
        Atomically change a 'pending' job to 'running' and returns it. Updates timestamp.
        Will return None if there are no jobs to offer.

        Potential jobs must match at least one tag, if provided.
        """

        query = { 'state': 'pending' }

        if tags is not None:
            query['tags'] = {'$in': tags }

        # First, atomically mark document as running.
        result = config.db.jobs.find_one_and_update(
            query,

            { '$set': {
                'state': 'running',
                'modified': datetime.datetime.utcnow()}
            },
            sort=[('modified', 1)],
            return_document=pymongo.collection.ReturnDocument.AFTER
        )

        if result is None:
            return None

        job = Job.load(result)

        if job.request is not None:
            log.info('Job ' + job._id + ' already has a request, so not generating')
            print job.request
            return result

        # Generate, save, and return a job request.
        request = job.generate_request(get_gear_by_name(job.name))
        result = config.db.jobs.find_one_and_update(
            {
                '_id': bson.ObjectId(job._id)
            },
            { '$set': {
                'request': request }
            },
            return_document=pymongo.collection.ReturnDocument.AFTER
        )

        if result is None:
            raise Exception('Marked job as running but could not generate and save formula')

        return result

    @staticmethod
    def search(container, states=None, tags=None):
        """
        Search the queue for jobs that mention a specific container and (optionally) match some set of states or tags.
        """

        filter = """
            for (var key in this['inputs']) {
                var ct = this['inputs'][key]['type']
                var ci = this['inputs'][key]['id']
                if (ct === '$cT$' && ci == '$cI$') { return true }
            }
        """.replace('$cT$', container.type).replace('$cI$', container.id)

        query = { "$where": filter }

        if states is not None and len(states) > 0:
            query['state'] = {"$in": states}

        if tags is not None and len(tags) > 0:
            query['tags'] = {"$in": tags}

        # For now, mandate reverse-crono sort
        return config.db.jobs.find(query).sort([
            ('modified', pymongo.DESCENDING)
        ])

    @staticmethod
    def get_statistics():
        """
        Return a variety of interesting information about the job queue.
        """

        # Count jobs by state
        result = config.db.jobs.aggregate([{"$group": {"_id": "$state", "count": {"$sum": 1}}}])
        # Map mongo result to a useful object
        by_state = {s: 0 for s in JOB_STATES}
        by_state.update({r['_id']: r['count'] for r in result})

        # Count jobs by tag grouping
        result = list(config.db.jobs.aggregate([{"$group": {"_id": "$tags", "count": {"$sum": 1}}}]))
        by_tag = []
        for r in result:
            by_tag.append({'tags': r['_id'], 'count': r['count']})

        # Count jobs that will not be retried
        permafailed = config.db.jobs.count({"attempt": {"$gte": max_attempts()}, "state":"failed"})

        return {
            'by-state': by_state,
            'by-tag': by_tag,
            'permafailed': permafailed
        }

    @staticmethod
    def scan_for_orphans():
        """
        Scan the queue for orphaned jobs, mark them as failed, and possibly retry them.
        Should be called periodically.
        """

        orphaned = 0

        while True:
            doc = config.db.jobs.find_one_and_update(
                {
                    'state': 'running',
                    'modified': {'$lt': datetime.datetime.utcnow() - datetime.timedelta(seconds=100)},
                },
                {
                    '$set': {
                        'state': 'failed', },
                },
                return_document=pymongo.collection.ReturnDocument.AFTER
            )

            if doc is None:
                break
            else:
                orphaned += 1
                j = Job.load(doc)
                Queue.retry(j)

        return orphaned
