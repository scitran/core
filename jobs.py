# @author:  Kevin S Hahn

"""
API request handlers for process-job-handling.
"""

import logging
import datetime
log = logging.getLogger('scitran.jobs')
import pymongo

import base
import algorithms

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

# TODO: json schema

def validTransition(fromState, toState):
    return (fromState + " --> " + tosState) in JOB_TRANSITIONS



# Creating a Job From Uploads

# The current method of util.create.job() will be removed.
# New way: the upload API handler marks the file as dirty, for later processing.

def createJob(db, jobType, containerType, containerID):
    """
    @param db             Reference to the database instance
    @param jobType        Human-friendly name of the algorithm
    @param containerType  Type of container ('acquisition', 'session', etc)
    @param containerID    ID of the container ('2', etc)
    """

    if jobType != 'dcm2nii':
        raise Exception('Usupported algorithm ' + jobType)

    # TODO validate container exists

    job = {
        'state': 'pending',
        'attempt': 1,

        'created':  datetime.datetime.now(),
        'modified': datetime.datetime.now(),

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

    result = db.jobs.insert_one(job)
    id = result.inserted_id

    log.info('Running %s as job %s to process %s %s' % (jobType, str(id), containerType, containerID))
    return id


class Jobs(base.RequestHandler):

    """Provide /jobs API routes."""

    def get(self):
        """
        List all jobs. Not used by engine.
        """
        return list(self.app.db.jobs.find())

    def count(self):
        """Return the total number of jobs. Not used by engine."""
        return self.app.db.jobs.count()

    def next(self):
        """
        Atomically change a 'pending' job to 'running' and returns it. Updates timestamp.
        Will return empty if there are no jobs to offer.
        Engine will poll this endpoint whenever there are free processing slots.
        """

        # algorithms.createJob(self.app.db, 'dcm2nii', 'session', '55a58db95f22580812902b9e')

        # REVIEW: is this atomic?
        # REVIEW: semantics are not documented as to this mutation's behaviour when filter matches no docs.
        result = self.app.db.jobs.find_one_and_update(
            {
                'state': 'pending'
            },
            { '$set': {
                'state': 'running',
                'modified': datetime.datetime.now()}
            },
            sort=[('modified', -1)],
            return_document=pymongo.collection.ReturnDocument.AFTER
        )

        if result == None:
            self.abort(400, 'No jobs to process')

        return result

class Job(base.RequestHandler):

    """Provides /Jobs/<jid> routes."""

    def get(self, _id):
        return self.app.db.jobs.find_one({'_id': int(_id)})

    def put(self, _id):
        """
        Update a job. Updates timestamp.
        Enforces a valid state machine transition, if any.
        Rejects any change to a job that is not currently in 'pending' or 'running' state.
        """
        mutation = self.request.json
        job = self.app.db.jobs.find_one({'_id': int(_id)})

        print 'MUTATION HAS ' + len(mutation) + ' FIELDS'

        if job['state'] not in JOB_STATES_ALLOWED_MUTATE:
            self.abort(404, 'Cannot mutate a job that is ' + job['state'] + '.')

        if 'state' in mutation and not validTransition(job['state'], mutation['state']):
            self.abort(404, 'Mutating job from ' + job['state'] + ' to ' + mutation['state'] + ' not allowed.')

        # Any modification must be a timestamp update
        mutation['timestamp'] = datetime.datetime.now()

        # REVIEW: is this atomic?
        # As far as I can tell, update_one vs find_one_and_update differ only in what they return.
        self.app.db.jobs.update_one(job, mutation)
