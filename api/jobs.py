"""
API request handlers for process-job-handling.
"""

# We shadow the standard library; this is a workaround.
from __future__ import absolute_import

import logging
log = logging.getLogger('scitran.api.jobs')

import bson
import pymongo
import datetime
from collections import namedtuple

from . import base
from . import config


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

def valid_transition(from_state, to_state):
    return (from_state + ' --> ' + to_state) in JOB_TRANSITIONS or from_state == to_state

ALGORITHMS = [
    'dcm2nii',
    'qa'
]

# A FileInput tuple holds all the details of a scitran file that needed to use that as an input a formula.
FileInput = namedtuple('input', ['container_type', 'container_id', 'filename', 'filehash'])

# Convert a dictionary to a FileInput
def convert_to_fileinput(d):
    return FileInput(
        container_type= d['container_type'],
        container_id  = d['container_id'],
        filename      = d['filename'],
        filehash      = d['filehash']
    )

def create_fileinput_from_reference(container, container_type, file_):
    """
    Spawn a job to process a file.

    Parameters
    ----------
    container: scitran.Container
        A container object that the file is held by
    container_type: string
        The type of container (eg, 'session')
    file: scitran.File
        File object that is used to spawn 0 or more jobs.
    """

    # File information
    filename = file_['name']
    filehash = file_['hash']
    # File container information
    container_id = str(container['_id'])

    log.info('File ' + filename + 'is in a ' + container_type + ' with id ' + container_id + ' and hash ' + filehash)

    # Spawn rules currently do not look at container hierarchy, and only care about a single file.
    # Further, one algorithm is unconditionally triggered for each dirty file.

    return FileInput(container_type=container_type, container_id=container_id, filename=filename, filehash=filehash)


def queue_job(db, algorithm_id, input, attempt_n=1, previous_job_id=None):
    """
    Enqueues a job for execution.

    Parameters
    ----------
    db: pymongo.database.Database
        Reference to the database instance
    algorithm_id: string
        Human-friendly unique name of the algorithm
    input: FileInput
        The input to be used by this job
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

        'algorithm_id': algorithm_id,
        'input': input._asdict(),

        'attempt': attempt_n,
    }

    if previous_job_id is not None:
        job['previous_job_id'] = previous_job_id

    result = db.jobs.insert_one(job)
    _id = result.inserted_id

    log.info('Running %s as job %s to process %s %s' % (algorithm_id, str(_id), input.container_type, input.container_id))
    return _id

def retry_job(db, j, force=False):
    """
    Given a failed job, either retry the job or fail it permanently, based on the attempt number.
    Can override the attempt limit by passing force=True.
    """

    if j['attempt'] < 3 or Force:
        job_id = queue_job(db, j['algorithm_id'], convert_to_fileinput(j['input']), j['attempt']+1, j['_id'])
        log.info('respawned job %s as %s (attempt %d)' % (j['_id'], job_id, j['attempt']+1))
    else:
        log.info('permanently failed job %s (after %d attempts)' % (j['_id'], j['attempt']))


def generate_formula(algorithm_id, i):
    """
    Given an intent, generates a formula to execute a job.

    Parameters
    ----------
    algorithm_id: string
        Human-friendly unique name of the algorithm
    i: FileInput
        The input to be used by this job
    """

    if algorithm_id not in ALGORITHMS:
        raise Exception('Usupported algorithm ' + algorithm_id)

    f = {
        'inputs': [
            {
                'type': 'file',
                'uri': '/nope.txt',
                'location': '/',
            },
            {
                'type': 'scitran',
                'uri': '/' + i['container_type'] + '/' + i['container_id'] + '/files/' + i['filename'],
                'location': '/input/' + i['filename'],
            }
        ],
        'target': {
            'command': [ 'echo', 'No command specified for ' + algorithm_id],
            'env': { },
            'dir': "/",
        },

        'outputs': [
            {
                'type': 'scitran',
                'uri': '/' + i['container_type'] + '/' + i['container_id'] + '/files/',
                'location': '/output',
            },
        ],
    }

    if algorithm_id == 'dcm2nii':
        f['inputs'][0]['uri'] = '/opt/flywheel-temp/dcm_convert-0.1.1.tar'
        f['target']['command'] = ['bash', '-c', 'mkdir /output; /scripts/run /input/' + i['filename'] + ' /output/' + i['filename'].split('_')[0]]

    elif algorithm_id == 'qa':
        f['inputs'][0]['uri'] = '/opt/flywheel-temp/qa-report-fmri-0.0.2.tar'
        f['target']['command'] = ['bash', '-c', 'mkdir /output; /scripts/run; exit 0']
    else:
        raise Exception('Command for algorithm ' + algorithm_id + ' not specified')

    return f

class Jobs(base.RequestHandler):

    """Provide /jobs API routes."""

    def get(self):
        """
        List all jobs. Not used by engine.
        """
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        results = list(config.db.jobs.find())

        return results

    def count(self):
        """Return the total number of jobs. Not used by engine."""
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        return config.db.jobs.count()

    def next(self):
        """
        Atomically change a 'pending' job to 'running' and returns it. Updates timestamp.
        Will return empty if there are no jobs to offer.
        Engine will poll this endpoint whenever there are free processing slots.
        """
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        # First, atomically mark document as running.
        result = config.db.jobs.find_one_and_update(
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
        result = config.db.jobs.find_one_and_update(
            {
                '_id': result['_id']
            },
            { '$set': {
                'request': generate_formula(result['algorithm_id'], result['input'])}
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
            self.abort(403, 'Request requires superuser')

        result = config.db.jobs.find_one({'_id': bson.ObjectId(_id)})
        return result

    def put(self, _id):
        """
        Update a job. Updates timestamp.
        Enforces a valid state machine transition, if any.
        Rejects any change to a job that is not currently in 'pending' or 'running' state.
        """
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        mutation = self.request.json
        job = config.db.jobs.find_one({'_id': bson.ObjectId(_id)})

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

        result = config.db.jobs.update_one(job_query, {'$set': mutation})
        if result.modified_count != 1:
            self.abort(500, 'Job modification not saved')

        # If the job did not succeed, check to see if job should be retried.
        if 'state' in mutation and mutation['state'] == 'failed':
            job = config.db.jobs.find_one({'_id': bson.ObjectId(_id)})
            retry_job(config.db, job)
