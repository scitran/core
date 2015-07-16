
import logging
import datetime
log = logging.getLogger('scitran.jobs')

import base


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

    job = {
        'state': 'pending',
        'attempt': 1,

        'created':  datetime.datetime.now(),
        'modified': datetime.datetime.now(),

        'inputs': [
            {
                'type': 'scitran'
                'location': '/',
                'URI': 'TBD'
            },
            {
                'type': 'scitran',
                'location': '/script',
                'URI': 'TBD'
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
                'URI': 'TBD'
            },
        ],
    }

    result = db.jobs.insert_one(job)
    log.info('Running %s as job %d to process %s %d' % (jobType, result['_id'], containerType, containerID))
    return result.inserted_id
