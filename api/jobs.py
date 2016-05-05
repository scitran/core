"""
API request handlers for process-job-handling.
"""

# We shadow the standard library; this is a workaround.
from __future__ import absolute_import

import bson
import pymongo
import datetime

from collections import namedtuple
from .dao.containerutil import FileReference, create_filereference_from_dictionary, ContainerReference, create_containerreference_from_dictionary, create_containerreference_from_filereference

from . import base
from . import config
from . import util

log = config.log

# How many times a job should be retried
MAX_ATTEMPTS = 3

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

def get_gears():
    """
    Fetch the install-global gears from the database
    """

    gear_doc  = config.db.static.find_one({'_id': 'gears'})
    return gear_doc['gear_list']

def get_gear_by_name(name):

    # Find a gear from the list by name
    gear_doc = config.db.static.find_one(
        {'_id': 'gears'},
        {'gear_list': { '$elemMatch': {
            'name': 'dcm_convert'
        }}
    })

    if gear_doc is None:
        raise Exception('Unknown gear ' + name)

    # Mongo returns the full document: { '_id' : 'gears', 'gear_list' : [ { .. } ] }, so strip that out
    return gear_doc['gear_list'][0]

def queue_job_legacy(db, algorithm_id, input, tags=None, attempt_n=1, previous_job_id=None):
    """
    Tie together logic used from the no-manifest, single-file era.
    Takes a single FileReference instead of a map.
    """

    if tags is None:
        tags = []

    gear = get_gear_by_name(algorithm_id)

    if len(gear['manifest']['inputs']) != 1:
        raise Exception("Legacy gear enqueue attempt of " + algorithm_id + " failed: must have exactly 1 input in manifest")

    input_name = gear['manifest']['inputs'].keys()[0]

    inputs = {
        input_name: input
    }

    return queue_job(db, algorithm_id, inputs, tags=tags, attempt_n=attempt_n, previous_job_id=previous_job_id)

def queue_job(db, name, inputs, destination=None, tags=None, attempt_n=1, previous_job_id=None):
    """
    Enqueues a job for execution.

    Parameters
    ----------
    db: pymongo.database.Database
        Reference to the database instance
    name: string
        Unique name of the algorithm
    inputs: string -> FileReference map
        The inputs to be used by this job
    destination: ContainerReference (optional)
        Where to place the gear's output. Defaults to one of the input's containers.
    tags: string array (optional)
        Tags that this job should be marked with.
    attempt_n: integer (optional)
        If an equivalent job has tried & failed before, pass which attempt number we're at. Defaults to 1 (no previous attempts).
    previous_job_id: string (optional)
        If an equivalent job has tried & failed before, pass the last job attempt. Defaults to None (no previous attempts).
    """

    if tags is None:
        tags = []

    now = datetime.datetime.utcnow()
    gear = get_gear_by_name(name)

    # A job is always tagged with the name of the gear, and if present, any gear-configured tags
    tags.append(name)
    if gear.get('tags', None):
        tags.extend(gear['tags'])

    job = {
        'state': 'pending',

        'created':  now,
        'modified': now,

        'algorithm_id': name,
        'inputs': {},

        'attempt': attempt_n,
        'tags': tags
    }

    # Save input FileReferences
    for i in gear['manifest']['inputs'].keys():
        if inputs.get(i, None) is None:
            raise Exception('Gear ' + name + ' requires input ' + i + ' but was not provided')

        job['inputs'][i] = inputs[i]._asdict()

    if destination is not None:
        # Use configured destination container
        job['destination'] = destination._asdict()
    else:
        # Grab an arbitrary input's container
        key = inputs.keys()[0]
        cr = create_containerreference_from_filereference(inputs[key])
        job['destination'] = cr._asdict()

    if previous_job_id is not None:
        job['previous_job_id'] = previous_job_id

    result = db.jobs.insert_one(job)
    _id = result.inserted_id

    return _id

def retry_job(db, j, force=False):
    """
    Given a failed job, either retry the job or fail it permanently, based on the attempt number.
    Can override the attempt limit by passing force=True.
    """

    if j['attempt'] < MAX_ATTEMPTS or force:
        # Translate job out from db to type
        for x in j['inputs'].keys():
            j['inputs'][x] = create_filereference_from_dictionary(j['inputs'][x])

        destination = None
        if j.get('destination', None) is not None:
            destination = create_containerreference_from_dictionary(j['destination'])

        job_id = queue_job(db, j['algorithm_id'], j['inputs'], attempt_n=j['attempt']+1, previous_job_id=j['_id'], destination=destination)
        log.info('respawned job %s as %s (attempt %d)' % (j['_id'], job_id, j['attempt']+1))
    else:
        log.info('permanently failed job %s (after %d attempts)' % (j['_id'], j['attempt']))

def generate_formula(algorithm_id, inputs, destination, job_id=None):
    """
    Given an intent, generates a formula to execute a job.

    Parameters
    ----------
    algorithm_id: string
        Human-friendly unique name of the algorithm
    inputs: string -> FileReference map
        The inputs to be used by this job
    destination: ContainerReference
        Where to place the gear's output. Defaults to one of the input's containers.
    job_id: string (optional)
        The job ID this will be placed on. Enhances the file origin by adding the job ID to the upload URL.
    """

    f = {
        'inputs': [ ],
        'target': {
            'command': ['bash', '-c', 'rm -rf output; mkdir -p output; ./run; echo "Exit was $?"'],
            'env': {
                'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
            },
            'dir': "/flywheel/v0",
        },
        'outputs': [
            {
                'type': 'scitran',
                'uri': '',
                'location': '/flywheel/v0/output',
            },
        ],
    }

    gear = get_gear_by_name(algorithm_id)

    # Add the gear
    f['inputs'].append(gear['input'])

    # Map destination to upload URI
    f['outputs'][0]['uri'] = '/engine?level=' + destination['container_type'] + '&id=' + destination['container_id']

    # Add the files
    for input_name in inputs.keys():
        i = inputs[input_name]

        f['inputs'].append({
            'type': 'scitran',
            'uri': '/' + i['container_type'] + 's/' + i['container_id'] + '/files/' + i['filename'],
            'location': '/flywheel/v0/input/' + input_name,
        })

    # Log job origin if provided
    if job_id:
        f['outputs'][0]['uri'] += '&job=' + job_id

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

    def add(self):
        """
        Add a manifest-aware job to the queue.
        """

        # TODO: Check each input container for R, check dest container for RW
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        # TODO: json schema

        submit = self.request.json
        gear_name = submit['gear']

        # Translate maps to FileReferences
        inputs = {}
        for x in submit['inputs'].keys():
            input_map = submit['inputs'][x]
            inputs[x] = create_filereference_from_dictionary(input_map)

        # Add job tags, attempt number, and/or previous job ID, if present
        tags            = submit.get('tags', None)
        attempt_n       = submit.get('attempt_n', 1)
        previous_job_id = submit.get('previous_job_id', None)

        # Add destination container, if present
        destination = None
        if submit.get('destination', None) is not None:
            destination = create_containerreference_from_dictionary(submit['destination'])

        # Return enqueued job ID
        job_id = queue_job(config.db, gear_name, inputs, destination=destination, tags=tags, attempt_n=attempt_n, previous_job_id=previous_job_id)
        return { '_id': job_id }

    def add_raw(self):
        """
        Add a blob of JSON to the jobs table. Absolutely no validation.
        """
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        return { "_id": config.db.jobs.insert_one(self.request.json).inserted_id }

    def count(self):
        """Return the total number of jobs. Not used by engine."""
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        return config.db.jobs.count()

    def stats(self):
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

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
        permafailed = config.db.jobs.count({"attempt": {"$gte": MAX_ATTEMPTS}, "state":"failed"})

        return {
            'by-state': by_state,
            'by-tag': by_tag,
            'permafailed': permafailed
        }

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

        str_id = str(result['_id'])

        # Second, update document to store formula request.
        result = config.db.jobs.find_one_and_update(
            {
                '_id': result['_id']
            },
            { '$set': {
                'request': generate_formula(result['algorithm_id'], result['inputs'], result['destination'], job_id=str_id)}
            },
            return_document=pymongo.collection.ReturnDocument.AFTER
        )

        if result is None:
            self.abort(500, 'Marked job as running but could not generate and save formula')

        return result

    def reap_stale(self):
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

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
                retry_job(config.db, j)


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
