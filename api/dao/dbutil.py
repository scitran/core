from pymongo.errors import DuplicateKeyError

from .. import config




def fault_tolerant_replace_one(coll_name, query, update, upsert=False):
    """
    Mongo does not see replace w/ upsert as an atomic action:
    https://jira.mongodb.org/browse/SERVER-14322

    Attempt a retry if first try produces DuplicateKeyError
    """

    try:
        result = config.db[coll_name].replace_one(query, update, upsert=upsert)
    except DuplicateKeyError:
        # Attempt again
        result = config.db[coll_name].replace_one(query, update, upsert=upsert)
    return result
