import bson.objectid
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
        return subject
    if subject.get('code') is not None:
        query = {'subject.code': subject['code'],
                 'project': pid,
                 'subject._id': {'$exists': True}}
        result = config.db.sessions.find_one(query)
    if result is not None:
        subject['_id'] = str(result['subject']['_id'])
    else:
        subject['_id'] = str(bson.ObjectId())
    return subject