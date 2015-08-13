# @author:  Kevin S Hahn

"""
API request handlers for process-job-handling.
"""

import logging
log = logging.getLogger('scitran.jobs')

import bson
import pymongo
import datetime

import base
import util

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
    "pending --> running",
    "running --> failed",
    "running --> complete",
]

def valid_transition(from_state, to_state):
    return (from_state + " --> " + to_state) in JOB_TRANSITIONS or from_state == to_state

ALGORITHMS = [
    "dcm2nii"
]


# TODO: json schema


def queue_job(db, algorithm_id, container_type, container_id, filename, filehash, attempt_n=1, previous_job_id=None):
    """
    Enqueues a job for execution.

    Parameters
    ----------
    db: pymongo.database.Database
        Reference to the database instance
    algorithm_id: string
        Human-friendly unique name of the algorithm
    container_type: string
        Type of container ('acquisition', 'session', etc)
    container_id: string
        ID of the container ('2', etc)
    filename: string
        Name of the file to download
    filehash: string
        Hash of the file to download
    attempt_n: integer (optional)
        If an equivalent job has tried & failed before, pass which attempt number we're at. Defaults to 1 (no previous attempts).
    """

    if algorithm_id not in ALGORITHMS:
        raise Exception('Usupported algorithm ' + algorithm_id)

    # TODO validate container exists

    now = datetime.datetime.utcnow()

    job = {
        'state': 'pending',

        'created':  now,
        'modified': now,

         # We need all these keys to re-run this job if it fails.
        'algorithm_id': algorithm_id,
        'container_id': container_id,
        'container_type': container_type,
        'container_type': algorithm_id,
        'filename': filename,
        'filehash': filehash,
        'attempt': attempt_n,

        'formula': {
            'inputs': [
                {
                    'type': 'scitran',
                    'location': '/',
                    'URI': 'TBD',
                },
                {
                    'type': 'scitran',
                    'location': '/script',
                    'URI': 'TBD',
                },

            ],

            'accents': {
                'cwd': "/script",
                'command': [ 'TBD' ],
                'environment': { },
            },

            'outputs': [
                {
                    'type': 'scitran',
                    'location': '/output',
                    'URI': 'TBD',
                },
            ],
        }

    }

    if previous_job_id is not None:
        job['previous_job_id'] = previous_job_id

    result = db.jobs.insert_one(job)
    _id = result.inserted_id

    log.info('Running %s as job %s to process %s %s' % (algorithm_id, str(_id), container_type, container_id))
    return _id

def serialize_job(job):
    if job:
        job['_id'] = str(job['_id'])
        job['created'] = util.format_timestamp(job['created'])
        job['modified'] = util.format_timestamp(job['modified'])

    return job

class Jobs(base.RequestHandler):

    """Provide /jobs API routes."""

    def get(self):
        """
        List all jobs. Not used by engine.
        """
        if not self.superuser_request:
            self.abort(401, 'Request requires superuser')

        results = list(self.app.db.jobs.find())
        for result in results:
            result = serialize_job(result)

        return results

    def count(self):
        """Return the total number of jobs. Not used by engine."""
        if not self.superuser_request:
            self.abort(401, 'Request requires superuser')

        return self.app.db.jobs.count()

    def addTestJob(self):
        """Adds a harmless job for testing purposes. Intentionally equivalent return to queue_job."""
        if not self.superuser_request:
            self.abort(401, 'Request requires superuser')

        return queue_job(self.app.db, 'dcm2nii', 'acquisition', '55bf861e6941f040cf8d6939')

    def next(self):
        """
        Atomically change a 'pending' job to 'running' and returns it. Updates timestamp.
        Will return empty if there are no jobs to offer.
        Engine will poll this endpoint whenever there are free processing slots.
        """
        if not self.superuser_request:
            self.abort(401, 'Request requires superuser')

        # REVIEW: is this atomic?
        # REVIEW: semantics are not documented as to this mutation's behaviour when filter matches no docs.
        result = self.app.db.jobs.find_one_and_update(
            {
                'state': 'pending'
            },
            { '$set': {
                'state': 'running',
                'modified': datetime.datetime.utcnow()}
            },
            sort=[('modified', -1)],
            return_document=pymongo.collection.ReturnDocument.AFTER
        )

        if result == None:
            self.abort(400, 'No jobs to process')

        return serialize_job(result)

class Job(base.RequestHandler):

    """Provides /Jobs/<jid> routes."""

    def get(self, _id):
        if not self.superuser_request:
            self.abort(401, 'Request requires superuser')

        result = self.app.db.jobs.find_one({'_id': bson.ObjectId(_id)})
        return serialize_job(result)

    def put(self, _id):
        """
        Update a job. Updates timestamp.
        Enforces a valid state machine transition, if any.
        Rejects any change to a job that is not currently in 'pending' or 'running' state.
        """
        if not self.superuser_request:
            self.abort(401, 'Request requires superuser')

        mutation = self.request.json
        job = self.app.db.jobs.find_one({'_id': bson.ObjectId(_id)})

        if job is None:
            self.abort(404, 'Job not found')

        if job['state'] not in JOB_STATES_ALLOWED_MUTATE:
            self.abort(404, 'Cannot mutate a job that is ' + job['state'] + '.')

        if 'state' in mutation and not valid_transition(job['state'], mutation['state']):
            self.abort(404, 'Mutating job from ' + job['state'] + ' to ' + mutation['state'] + ' not allowed.')

        # Any modification must be a timestamp update
        mutation['modified'] = datetime.datetime.utcnow()

        # REVIEW: is this atomic?
        # As far as I can tell, update_one vs find_one_and_update differ only in what they return.
        self.app.db.jobs.update_one(job, {'$set': mutation})
