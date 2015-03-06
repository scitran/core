# @author:  Kevin S Hahn

"""
API request handlers for process-job-handling.

represents the /nimsapi/jobs route
"""

import os
import bson
import json
import logging
import datetime
log = logging.getLogger('nimsapi.jobs')

import base

# TODO: what should this whitelist contain? protocol + FQDN?
# ex. https://coronal.stanford.edu
PROCESSOR_WHITELIST = [
    'dockerhost',
]

JOB_STATES = [
    'pending',      # created but not started
    'queued',       # job claimed by a processor
    'running',      # job running on a processor
    'done',         # job completed successfully
    'failed',       # some error occurred,
    'paused',       # job paused.  can't think when this would be useful...
]

# Jobs must now how they affect the various components of a file description
# some "special" case things will reset state from 'orig' to 'pending'
# but the usual case will be to append an item to the state list.

# TODO: create job function should live here
# where it can be editted with the route that consume and modify the jobs

# GET  /jobs full list of jobs, allow specifiers, status=
# POST /jobs creates a new job. this will be used by webapp to add new jobs
# GET  /jobs/<_id> get information about one job
# PUT  /jobs/<_id> update informabout about one job
# GET  /jobs/next, special route to get the 'next job'

class Jobs(base.RequestHandler):

    """Provide /jobs API routes."""

    def get(self):
        """
        Return one Job that needs processing.

        TODO: allow querying for group
        TODO: allow querying for project
        TODO: allow querying by other meta data. can this be generalized?

        """
        # TODO: auth
        return list(self.app.db.jobs.find())

    def count(self):
        """Return the total number of jobs."""
        # no auth?
        return self.app.db.jobs.count()

    def counts(self):
        """Return more information about the jobs."""
        counts = {
            'total': self.app.db.jobs.count(),
            'failed': self.app.db.jobs.find({'status': 'failed'}).count(),
            'pending': self.app.db.jobs.find({'status': 'pending'}).count(),
            'done': self.app.db.jobs.find({'status': 'done'}).count(),
        }
        return counts

    def next(self):
        """Return the next job in the queue that matches the query parameters."""
        # TODO: add ability to query on things like psd type or psd name
        try:
            query_params = self.request.json
        except ValueError as e:
            self.abort(400, str(e))

        query = {'status': 'pending'}
        try:
            query_params = self.request.json
        except ValueError as e:
            self.abort(400, str(e))

        project_query = query_params.get('project')
        group_query = query_params.get('group')
        query = {'status': 'pending'}
        if project_query:
            query.update({'project': project_query})
        if group_query:
            query.update({'group': group_query})

        # TODO: how to guarantee the 'oldest' jobs pending jobs are given out first
        job_spec = self.app.db.jobs.find_and_modify(
            query,
            {'$set': {'status': 'queued', 'modified': datetime.datetime.now()}},
            sort=[('modified', -1)],
            new=True
        )
        return job_spec


class Job(base.RequestHandler):

    """Provides /Jobs/<jid> routes."""

    # TODO flesh out the job schema
    json_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'User',
        'type': 'object',
        'properties': {
            '_id': {
                'title': 'Job ID',
                'type': 'string',
            },
        },
        'required': ['_id'],
        'additionalProperties': True,
    }

    def get(self, _id):
        return self.app.db.jobs.find_one({'_id': int(_id)})

    def put(self, _id):
        """Update a single job."""
        payload = self.request.json
        # TODO: validate the json before updating the db
        log.debug(payload)
        self.app.db.jobs.update({'_id': int(_id)}, {'$set': {'status': payload.get('status')}})
