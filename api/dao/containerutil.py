import bson.objectid
from collections import namedtuple

from .. import config

log = config.log


def add_id_to_subject(subject, pid):
    """
    Add a mongo id field to given subject object (dict)

    Use the same _id as other subjects in the session's project with the same code
    If no _id is found, generate a new _id
    """
    result = None
    if subject is None:
        subject = {}
    if subject.get('_id') is not None:
        # Ensure _id is bson ObjectId
        subject['_id'] = bson.ObjectId(str(subject['_id']))
        return subject

    # Attempt to match with another session in the project
    if subject.get('code') is not None and pid is not None:
        query = {'subject.code': subject['code'],
                 'project': pid,
                 'subject._id': {'$exists': True}}
        result = config.db.sessions.find_one(query)

    if result is not None:
        subject['_id'] = result['subject']['_id']
    else:
        subject['_id'] = bson.ObjectId()
    return subject


# A FileReference tuple holds all the details of a scitran file that needed to use that as an input a formula.
FileReference = namedtuple('input', ['container_type', 'container_id', 'filename'])

# Convert a dictionary to a FileReference
def create_filereference_from_dictionary(d):
    if d['container_type'].endswith('s'):
        raise Exception('Container type cannot be plural :|')

    return FileReference(
        container_type= d['container_type'],
        container_id  = d['container_id'],
        filename      = d['filename']
    )

def create_filereference_from_file_map(container, container_type, file_):
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

    if container_type.endswith('s'):
        raise Exception('Container type cannot be plural :|')

    # File information
    filename = file_['name']
    # File container information
    container_id = str(container['_id'])

    # Spawn rules currently do not look at container hierarchy, and only care about a single file.
    # Further, one algorithm is unconditionally triggered for each dirty file.

    return FileReference(container_type=container_type, container_id=container_id, filename=filename)
