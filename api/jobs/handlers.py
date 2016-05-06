"""
API request handlers for the jobs module
"""

# We shadow the standard library; this is a workaround.
from __future__ import absolute_import

import bson
import pymongo
import datetime

from collections import namedtuple
from ..dao.containerutil import FileReference, create_filereference_from_dictionary, ContainerReference, create_containerreference_from_dictionary, create_containerreference_from_filereference

from .. import base
from .. import config
from .. import util
from .jobs import Job
from .queue import Queue, JOB_STATES, MAX_ATTEMPTS

log = config.log


class JobsHandler(base.RequestHandler):

    """Provide /jobs API routes."""

    def get(self):
        """
        List all jobs.
        """
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        results = list(config.db.jobs.find())

        return results

    def add(self):
        """
        Add a job to the queue.
        """

        # TODO: Check each input container for R, check dest container for RW
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

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

        job = Job(gear_name, inputs, destination=destination, tags=tags, attempt=attempt_n, previous_job_id=previous_job_id)
        return job.insert()

    def stats(self):
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        return Queue.get_statistics()

    def next(self):
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        job = Queue.start_job()

        if job is None:
            self.abort(400, 'No jobs to process')
        else:
            return job

    def reap_stale(self):
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        while True:
            doc = config.db.jobs.find_one_and_update(
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
            if doc is None:
                break
            else:
                j = Job.load(doc)
                Queue.retry(j)


class JobHandler(base.RequestHandler):

    """Provides /Jobs/<jid> routes."""

    def get(self, _id):
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        return Job.get(_id)

    def put(self, _id):
        """
        Update a job. Updates timestamp.
        Enforces a valid state machine transition, if any.
        Rejects any change to a job that is not currently in 'pending' or 'running' state.
        """
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        j = Job.get(_id)
        Queue.mutate(j, self.request.json)
