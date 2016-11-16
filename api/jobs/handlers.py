"""
API request handlers for the jobs module
"""

import json
import StringIO
import gear_tools
from jsonschema import Draft4Validator, ValidationError

from ..dao.containerutil import create_filereference_from_dictionary, create_containerreference_from_dictionary, create_containerreference_from_filereference, ContainerReference
from .. import base
from .. import config

from .gears import get_gears, get_gear_by_name, get_invocation_schema, remove_gear, upsert_gear, suggest_container
from .jobs import Job
from .queue import Queue


class GearsHandler(base.RequestHandler):

    """Provide /gears API routes."""

    def get(self):
        """List all gears."""

        if self.public_request:
            self.abort(403, 'Request requires login')

        fields = self.request.GET.getall('fields')
        if 'all' in fields:
            fields = None

        return get_gears(fields)

class GearHandler(base.RequestHandler):
    """Provide /gears/x API routes."""

    def get(self, _id):
        """Detail a gear."""

        if self.public_request:
            self.abort(403, 'Request requires login')

        return get_gear_by_name(_id)

    def get_invocation(self, _id):

        if self.public_request:
            self.abort(403, 'Request requires login')

        gear = get_gear_by_name(_id)
        return get_invocation_schema(gear)

    def suggest(self, _id, cont_name, cid):

        if self.public_request:
            self.abort(403, 'Request requires login')

        cr = ContainerReference(cont_name, cid)
        if not self.superuser_request:
            cr.check_access(self.uid, 'ro')

        gear = get_gear_by_name(_id)
        return suggest_container(gear, cont_name+'s', cid)

    def post(self, _id):
        """Upsert an entire gear document."""

        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        doc = self.request.json

        if _id != doc.get('gear', {}).get('name', ''):
            self.abort(400, 'Name key must be present and match URL')

        try:
            upsert_gear(self.request.json)
        except ValidationError as err:
            key = None
            if len(err.relative_path) > 0:
                key = err.relative_path[0]

            self.response.set_status(400)
            return {
                'reason': 'Gear manifest does not match schema',
                'error': err.message.replace("u'", "'"),
                'key': key
            }

        return { 'name': _id }

    def delete(self, _id):
        """Delete a gear. Generally not recommended."""

        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        return remove_gear(_id)


class RulesHandler(base.RequestHandler):

    """Provide /rules API routes."""

    def get(self):
        """List rules"""
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        return config.db.singletons.find_one({"_id" : "rules"})['rule_list']

    def post(self):
        """Upsert all rules"""
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        doc = self.request.json
        config.db.singletons.replace_one({"_id" : "rules"}, {'rule_list': doc}, upsert=True)


class JobsHandler(base.RequestHandler):
    """Provide /jobs API routes."""
    def get(self):
        """List all jobs."""
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        return list(config.db.jobs.find())

    def add(self):
        """Add a job to the queue."""
        submit = self.request.json

        gear_name = submit['gear']

        # Translate maps to FileReferences
        inputs = {}
        for x in submit['inputs'].keys():
            input_map = submit['inputs'][x]
            inputs[x] = create_filereference_from_dictionary(input_map)

        # Add job tags, config, attempt number, and/or previous job ID, if present
        tags            = submit.get('tags', None)
        config_         = submit.get('config', {})
        attempt_n       = submit.get('attempt_n', 1)
        previous_job_id = submit.get('previous_job_id', None)
        now_flag        = submit.get('now', False) # A flag to increase job priority

        # Add destination container, or select one
        destination = None
        if submit.get('destination', None) is not None:
            destination = create_containerreference_from_dictionary(submit['destination'])
        else:
            key = inputs.keys()[0]
            destination = create_containerreference_from_filereference(inputs[key])

        # Permission check
        if not self.superuser_request:
            for x in inputs:
                inputs[x].check_access(self.uid, 'ro')
            destination.check_access(self.uid, 'rw')
            now_flag = False # Only superuser requests are allowed to set "now" flag

        # Config manifest check
        gear = get_gear_by_name(gear_name)
        if len(gear.get('manifest', {}).get('config', {})) > 0:

            invocation = gear_tools.derive_invocation_schema(gear['manifest'])
            ci = gear_tools.isolate_config_invocation(invocation)
            validator = Draft4Validator(ci)

            try:
                validator.validate(config_)
            except ValidationError as err:
                key = None
                if len(err.relative_path) > 0:
                    key = err.relative_path[0]

                self.response.set_status(422)
                return {
                    'reason': 'config did not match manifest',
                    'error': err.message.replace("u'", "'"),
                    'key': key
                }

        job = Job(gear_name, inputs, destination=destination, tags=tags, config_=config_, now=now_flag, attempt=attempt_n, previous_job_id=previous_job_id, origin=self.origin)
        result = job.insert()

        return { "_id": result }

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

    def get_config(self, _id):
        """Get a job's config"""
        if not self.superuser_request:
            self.abort(403, 'Request requires superuser')

        c = Job.get(_id).config
        if c is None:
            c = {}

        self.response.headers['Content-Type'] = 'application/octet-stream'
        self.response.headers['Content-Disposition'] = 'attachment; filename="config.json"'

        # Serve config as formatted json file
        encoded = json.dumps({"config": c}, sort_keys=True, indent=4, separators=(',', ': ')) + '\n'
        self.response.app_iter = StringIO.StringIO(encoded)

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

    def retry(self, _id):
        """ Retry a job.

        The job must have a state of 'failed', and must not have already been retried.
        Returns the id of the new, generated job.
        """
        j = Job.get(_id)

        # Permission check
        if not self.superuser_request:
            for x in j.inputs:
                j.inputs[x].check_access(self.uid, 'ro')
            j.destination.check_access(self.uid, 'rw')

        new_id = Queue.retry(j, force=True)
        return { "_id": new_id }
