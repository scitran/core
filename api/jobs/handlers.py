"""
API request handlers for the jobs module
"""

from ..dao.containerutil import create_filereference_from_dictionary, create_containerreference_from_dictionary

from .. import base
from .. import config

from .gears import get_gears, get_gear_by_name
from .jobs import Job
from .queue import Queue

log = config.log



class GearsHandler(base.RequestHandler):

    """Provide /gears API routes."""

    def get(self):
        """
        .. http:get:: /api/gears

            List all gears.

            :query fields: filter fields returned. Defaults to ['name']. Pass 'all' for everything.
            :type fields: string

            :statuscode 200: no error

            **Example request**:

            .. sourcecode:: http

                GET /api/gears HTTP/1.1
                Host: demo.flywheel.io
                Accept: */*


            **Example response**:

            .. sourcecode:: http

                HTTP/1.1 200 OK
                Vary: Accept-Encoding
                Content-Type: application/json; charset=utf-8
                [
                    {
                        "name": "dicom_mr_classifier"
                    },
                    {
                        "name": "dcm_convert"
                    },
                    {
                        "name": "qa-report-fmri"
                    }
                ]
        """

        if self.public_request:
            self.abort(403, 'Request requires login')

        fields = self.request.GET.getall('fields')
        if 'all' in fields:
            fields = None

        return get_gears(fields)


class GearHandler(base.RequestHandler):

    """Provide /gears/x API routes."""

    def get(self, _id):
        """
        .. http:get:: /api/gears/(gid)

            Detail a gear.

            :statuscode 200: no error

            **Example request**:

            .. sourcecode:: http

                GET /api/gears/dcm_convert HTTP/1.1
                Host: demo.flywheel.io
                Accept: */*


            **Example response**:

            .. sourcecode:: http

                HTTP/1.1 200 OK
                Vary: Accept-Encoding
                Content-Type: application/json; charset=utf-8
                {
                    "name": "dcm_convert"
                    "manifest": {
                        "config": {},
                        "inputs": {
                            "dicom": {
                                "base": "file",
                                "type": {
                                    "enum": [
                                        "dicom"
                                    ]
                                }
                            }
                        },
                    },
                }

        """

        if self.public_request:
            self.abort(403, 'Request requires login')

        return get_gear_by_name(_id)


class JobsHandler(base.RequestHandler):

    """Provide /jobs API routes."""

    def get(self):
        """
        List all jobs.
        """
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        return list(config.db.jobs.find())

    def add(self):
        """
        Add a job to the queue.
        """

        # TODO: Check each input container for R, check dest container for RW
        # if not self.superuser_request:
        # 	self.abort(403, 'Request requires superuser')

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

        tags = self.request.GET.getall('tags')
        if len(tags) <= 0:
            tags = None

        job = Queue.start_job(tags=tags)

        if job is None:
            self.abort(400, 'No jobs to process')
        else:
            return job

    def reap_stale(self):
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        count = Queue.scan_for_orphans()
        return { 'orphaned': count }


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
