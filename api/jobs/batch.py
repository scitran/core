"""
Batch
"""
import bson

from .. import config
from ..dao import APINotFoundException
from ..dao.containerutil import create_filereference_from_dictionary, create_containerreference_from_filereference
from .jobs import Job
from .queue import Queue
from . import gears

log = config.log

BATCH_JOB_TRANSITIONS = {
    # To  <-------  #From
    'launched':     'pending',
    'cancelled':    'launched'
}


def get_all(query, projection=None):
    """
    Fetch batch objects from the database
    """
    return config.db.batch.find(query, projection)

def get(batch_id, projection=None, get_jobs=False):
    """
    Fetch batch object by id, include stats and job objects as requested
    """

    if isinstance(batch_id, str):
        batch_id = bson.ObjectId(batch_id)
    batch_job = config.db.batch.find_one({'_id': batch_id}, projection)

    if batch_job is None:
        raise APINotFoundException('Batch job {} not found.'.format(batch_id))

    if get_jobs:
        jobs = []
        for jid in batch_job.get('jobs', []):
            job = Job.get(jid)
            jobs.append(job)
        batch_job['jobs'] = jobs

    return batch_job

def find_matching_conts(gear, containers, container_type):
    """
    Give a gear and a list of containers, find files that:
      - have no solution to the gear's input schema (not matched)
      - have multiple solutions to the gear's input schema (ambiguous)
      - match the gear's input schema 1 to 1 (matched)
    Containers are placed in one of the three categories in order.
    A container with 2 possible files for one input and none for the other
    will be marked as 'not matched', not ambiguous.
    """

    matched_conts = []
    not_matched_conts = []
    ambiguous_conts = []

    for c in containers:
        files = c.get('files')
        if files:
            suggestions = gears.suggest_for_files(gear, files)

            # Determine if any of the inputs are ambiguous or not satisfied
            ambiguous = False # Are any of the inputs ambiguous?
            not_matched = False
            for files in suggestions.itervalues():
                if len(files) > 1:
                    ambiguous = True
                elif len(files) == 0:
                    not_matched = True
                    break

            # Based on results, add to proper list
            if not_matched:
                not_matched_conts.append(c)
            elif ambiguous:
                ambiguous_conts.append(c)
            else:
                # Create input map of file refs
                inputs = {}
                for input_name, files in suggestions.iteritems():
                    inputs[input_name] = {'type': container_type, 'id': str(c['_id']), 'name': files[0]}
                c['inputs'] = inputs
                matched_conts.append(c)
        else:
            not_matched_conts.append(c)
    return {
        'matched': matched_conts,
        'not_matched': not_matched_conts,
        'ambiguous': ambiguous_conts
    }

def insert(batch_proposal):
    """
    Simple database insert given a batch proposal.
    """
    return config.db.batch.insert(batch_proposal)

def update(batch_id, payload):
    """
    Updates a batch job, being mindful of state flow.
    """

    bid = bson.ObjectId(batch_id)
    query = {'_id': bid}
    if payload.get('state'):
        # Require that the batch job has the previous state
        query['state'] = BATCH_JOB_TRANSITIONS[payload.get('state')]
    result = config.db.batch.update_one({'_id': bid}, {'$set': payload})
    if result.modified_count != 1:
        raise Exception('Batch job not updated')

def run(batch_job):
    """
    Creates jobs from proposed inputs, returns jobs enqueued.
    """

    proposed_inputs = batch_job.get('proposed_inputs', [])
    gear_name = batch_job.get('gear')
    config_ = batch_job.get('config')
    origin = batch_job.get('origin')

    jobs = []
    job_ids = []
    for inputs in proposed_inputs:
        for input_name, fr in inputs.iteritems():
            inputs[input_name] = create_filereference_from_dictionary(fr)
        # TODO support analysis gears (will have to create analyses here)
        destination = create_containerreference_from_filereference(inputs[inputs.keys()[0]])
        job = Job(gear_name, inputs, destination=destination, tags=['batch'], config_=config_, origin=origin)
        job_id = job.insert()
        jobs.append(job)
        job_ids.append(job_id)

    update(batch_job['_id'], {'state': 'launched', 'jobs': job_ids})
    return jobs

def cancel(batch_job):
    """
    Cancels all pending jobs, returns number of jobs cancelled.
    """

    pending_jobs = config.db.jobs.find({'state': 'pending', '_id': {'$in': batch_job.get('jobs')}})
    cancelled_jobs = 0
    for j in pending_jobs:
        job = Job.load(j)
        try:
            Queue.mutate(job, {'state': 'cancelled'})
            cancelled_jobs += 1
        except Exception: # pylint: disable=broad-except
            # if the cancellation fails, move on to next job
            continue

    update(batch_job['_id'], {'state': 'cancelled'})
    return cancelled_jobs


def get_stats():
    """
    Return the number of jobs by state.
    """
    raise NotImplementedError()

def resume():
    """
    Move cancelled jobs back to pending.
    """
    raise NotImplementedError()

def delete():
    """
    Remove:
      - the batch job
      -  it's spawned jobs
      - all the files it's jobs produced.
    """
    raise NotImplementedError()
