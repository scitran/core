"""
Gears
"""

from .. import config

log = config.log


def get_gears():
    """
    Fetch the install-global gears from the database
    """

    gear_doc  = config.db.singletons.find_one({'_id': 'gears'})
    return gear_doc['gear_list']

def get_gear_by_name(name):

    # Find a gear from the list by name
    gear_doc = config.db.singletons.find_one(
        {'_id': 'gears'},
        {'gear_list': { '$elemMatch': {
            'name': name
        }}
    })

    if gear_doc is None:
        raise Exception('Unknown gear ' + name)

    # Mongo returns the full document: { '_id' : 'gears', 'gear_list' : [ { .. } ] }, so strip that out
    return gear_doc['gear_list'][0]
