"""
API request handlers for process-job-handling.
"""

import logging
log = logging.getLogger('scitran.api.jobs')

import bson
import pymongo
import datetime

from . import base

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


def spawn_jobs(db, containers, file):
    """
    Spawn some number of queued jobs to process a file.

    Parameters
    ----------
    db: pymongo.database.Database
        Reference to the database instance
    containers: [ tuple(string, scitran.Container) ]
        An array of tuples, each containing a container type name, and a container object.
        Contract is:
            1) The first container in the array will be the container which owns file passed in the file param.
            2) Following array indexes, if any, will be higher in the ownership heirarchy than the first container.
            3) Array is not guaranteed to be strictly hierarchically ordered.
    file: scitran.File
        File object that is used to spawn 0 or more jobs.
    """

    if file['filetype'] != 'dicom':
        return

    # File information
    filename = file['filename']
    filehash = file['filehash']

    # File container information
    last = len(containers) - 1
    container_type, container = containers[last]
    container_id = str(container['_id'])

    log.info('File ' + filename + 'is in a ' + container_type + ' with id ' + container_id + ' and hash ' + filehash)

    # Spawn rules currently do not look at container hierarchy, and only care about a single file.
    # Further, one algorithm is unconditionally triggered for each dirty file.

    queue_job(db, 'dcm2nii', container_type, container_id, filename, filehash)


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
        Type of container ('acquisitions', 'sessions', etc) that matches the URL route.
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

         # Everything required to generate a job formula.
        'intent': {
            'algorithm_id': algorithm_id,
            'container_type': container_type,
            'container_id': container_id,
            'filename': filename,
            'filehash': filehash,
        },

        'attempt': attempt_n,
    }

    if previous_job_id is not None:
        job['previous_job_id'] = previous_job_id

    result = db.jobs.insert_one(job)
    _id = result.inserted_id

    log.info('Running %s as job %s to process %s %s' % (algorithm_id, str(_id), container_type, container_id))
    return _id

def retry_job(db, j):
    """
    Given a failed job, either retry the job or fail it permanently, based on the attempt number.
    TODO: make max attempts configurable
    """

    i = j['intent']

    if j['attempt'] < 3:
        job_id = queue_job(db, i['algorithm_id'], i['container_type'], i['container_id'], i['filename'], i['filehash'], j['attempt']+1, j['_id'])
        log.info('respawned job %s as %s (attempt %d)' % (j['_id'], job_id, j['attempt']+1))
    else:
        log.info('permanently failed job %s (after %d attempts)' % (j['_id'], j['attempt']))


def generate_formula(i):
    """
    Given an intent, generates a formula to execute a job.

    Parameters
    ----------
    i: map
        A job's intent that holds everything needed to generate a formula.
    """

    if i['algorithm_id'] not in ALGORITHMS:
        raise Exception('Usupported algorithm ' + algorithm_id)

    # Currently hard-coded for a single algorithm: dcm2nii
    f = {
        'inputs': [
            {
                'type': 'file',
                'uri': '/opt/flywheel-temp/dcm_convert-0.1.1.tar',
                'location': '/',
            },
            {
                'type': 'scitran',
                'uri': '/' + i['container_type'] + '/' + i['container_id'] + '/file/' + i['filename'],
                'location': '/input/' + i['filename'],
            }
        ],
        'transform': {
            'command': ['bash', '-c', 'mkdir /output; /scripts/run /input/' + i['filename'] + ' /output/' + i['filename'].split('_')[0] ],
            'env': { },
            'dir': "/",
        },

        'outputs': [
            {
                'type': 'scitran',
                'uri': '/' + i['container_type'] + '/' + i['container_id'] + '/file/',
                'location': '/output',
            },
        ],
    }

    return f

class Jobs(base.RequestHandler):

    """Provide /jobs API routes."""

    def get(self):
        """
        List all jobs. Not used by engine.
        """
        if not self.superuser_request:
            self.abort(401, 'Request requires superuser')

        results = list(self.app.db.jobs.find())

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

        # First, atomically mark document as running.
        result = self.app.db.jobs.find_one_and_update(
            {
                'state': 'pending'
            },
            { '$set': {
                'state': 'running',
                'modified': datetime.datetime.utcnow()}
            },
            sort=[('modified', 1)],
            return_document=pymongo.collection.ReturnDocument.AFTER
        )

        if result is None:
            self.abort(400, 'No jobs to process')

        # Second, update document to store formula request.
        result = self.app.db.jobs.find_one_and_update(
            {
                '_id': result['_id']
            },
            { '$set': {
                'request': generate_formula(result['intent'])}
            },
            return_document=pymongo.collection.ReturnDocument.AFTER
        )

        if result is None:
            self.abort(500, 'Marked job as running but could not generate and save formula')

        return result

class Job(base.RequestHandler):

    """Provides /Jobs/<jid> routes."""

    def get(self, _id):
        if not self.superuser_request:
            self.abort(401, 'Request requires superuser')

        result = self.app.db.jobs.find_one({'_id': bson.ObjectId(_id)})
        return result

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

        # Create an object with all the fields that must not have changed concurrently.
        job_query =  {
            '_id': job['_id'],
            'state': job['state'],
        }

        result = self.app.db.jobs.update_one(job_query, {'$set': mutation})
        if result.modified_count != 1:
            self.abort(500, 'Job modification not saved')

        # If the job did not succeed, check to see if job should be retried.
        if 'state' in mutation and mutation['state'] == 'failed':
            job = self.app.db.jobs.find_one({'_id': bson.ObjectId(_id)})
            retry_job(self.app.db, job)
