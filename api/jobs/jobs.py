"""
Jobs
"""

import bson
import datetime

from ..dao.containerutil import create_filereference_from_dictionary, create_containerreference_from_dictionary, create_containerreference_from_filereference

from .. import config
from . import gears

log = config.log


class Job(object):
    def __init__(self, name, inputs, destination=None, tags=None, attempt=1, previous_job_id=None, created=None, modified=None, state='pending', request=None, _id=None):
        """
        Creates a job.

        Parameters
        ----------
        name: string
            Unique name of the algorithm
        inputs: string -> FileReference map
            The inputs to be used by this job
        destination: ContainerReference (optional)
            Where to place the gear's output. Defaults to one of the input's containers.
        tags: string array (optional)
            Tags that this job should be marked with.
        attempt: integer (optional)
            If an equivalent job has tried & failed before, pass which attempt number we're at. Defaults to 1 (no previous attempts).
        previous_job_id: string (optional)
            If an equivalent job has tried & failed before, pass the last job attempt. Defaults to None (no previous attempts).
        created: datetime (optional)
        modified: datetime (optional)
            Timestamps
        state: string (optional)
            The state of this job. Defaults to 'pending'.
        request: map (optional)
            The request that is used for the engine. Generated when job is started.
        _id: string (optional)
            The database identifier for this job.
        """

        # TODO: validate inputs against the manifest

        now = datetime.datetime.utcnow()

        if tags is None:
            tags = []
        if created is None:
            created = now
        if modified is None:
            modified = now

        if destination is None:
            # Grab an arbitrary input's container
            key = inputs.keys()[0]
            destination = create_containerreference_from_filereference(inputs[key])

        # A job is always tagged with the name of the gear
        tags.append(name)

        # Trim tags array to unique members...
        tags = list(set(tags))

        self.name    = name
        self.inputs          = inputs
        self.destination     = destination
        self.tags            = tags
        self.attempt         = attempt
        self.previous_job_id = previous_job_id
        self.created         = created
        self.modified        = modified
        self.state           = state
        self.request         = request
        self._id             = _id

    @classmethod
    def load(cls, d):
        # TODO: validate

        inputs = d['inputs']
        for x in inputs.keys():
            inputs[x] = create_filereference_from_dictionary(inputs[x])

        d['destination'] = create_containerreference_from_dictionary(d['destination'])

        d['_id'] = str(d['_id'])

        return cls(d['name'], d['inputs'], destination=d['destination'], tags=d['tags'], attempt=d['attempt'], previous_job_id=d.get('previous_job_id', None), created=d['created'], modified=d['modified'], state=d['state'], request=d.get('request', None), _id=d['_id'])

    @classmethod
    def get(cls, _id):
        doc = config.db.jobs.find_one({'_id': bson.ObjectId(_id)})
        if doc is None:
            raise Exception('Job not found')

        return cls.load(doc)

    def map(self):
        """
        Flatten struct to map
        """

        d = self.__dict__
        d['destination'] = d['destination'].__dict__

        for x in d['inputs'].keys():
            d['inputs'][x] = d['inputs'][x].__dict__

        if d['_id'] is None:
            d.pop('_id')
        if d['previous_job_id'] is None:
            d.pop('previous_job_id')
        if d['request'] is None:
            d.pop('request')

        return d

    def mongo(self):
        d = self.map()
        if d.get('_id', None):
            d['_id'] = bson.ObjectId(d['_id'])

        return d

    def insert(self):
        if self._id is not None:
            raise Exception('Cannot insert job that has already been inserted')

        result = config.db.jobs.insert_one(self.mongo())
        return result.inserted_id

    def generate_request(self, gear=None):
        """
        Generate the job's request, save it to the class, and return it

        Parameters
        ----------
        gear: map (optional)
            A gear_list map from the singletons.gears table. Will be loaded by the job's name otherwise.
        """

        r = {
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

        if gear is None:
            gear = gears.get_gear_by_name(self.name)

        # Add the gear
        r['inputs'].append(gear['input'])

        # Map destination to upload URI
        r['outputs'][0]['uri'] = '/engine?level=' + self.destination.type + '&id=' + self.destination.id

        # Add the files
        for input_name in self.inputs.keys():
            i = self.inputs[input_name]

            r['inputs'].append({
                'type': 'scitran',
                'uri': '/' + i.type + 's/' + i.id + '/files/' + i.name,
                'location': '/flywheel/v0/input/' + input_name,
            })

        # Log job origin if provided
        if self._id:
            r['outputs'][0]['uri'] += '&job=' + self._id

        self.request = r
        return self.request
