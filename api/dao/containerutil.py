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


# A FileReference tuple holds all the details of a scitran file needed to uniquely identify it.
FileReference = namedtuple('FileReference', ['type', 'id', 'name'])

def create_filereference_from_dictionary(d):
    if d['type'].endswith('s'):
        raise Exception('Container type cannot be plural :|')

    return FileReference(
        type = d['type'],
        id   = d['id'],
        name = d['name']
    )

# A ContainerReference tuple holds all the details of a scitran container needed to uniquely identify it.
ContainerReference = namedtuple('ContainerReference', ['type', 'id'])

def create_containerreference_from_dictionary(d):
    if d['type'].endswith('s'):
        raise Exception('Container type cannot be plural :|')

    return ContainerReference(
        type = d['type'],
        id   = d['id']
    )

def create_containerreference_from_filereference(fr):
    return ContainerReference(
        type = fr.type,
        id   = fr.id
    )
