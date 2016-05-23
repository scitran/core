"""
Gears
"""

from .. import config

log = config.log

# For now, gears are in a singleton, prefixed by a key
SINGLETON_KEY = 'gear_list'

def get_gears(fields=None):
    """
    Fetch the install-global gears from the database
    """

    projection = { }

    if fields is None:
        fields = [ ]
        projection = { SINGLETON_KEY: 1 }
    else:
        fields.append('name')

    query = {'_id': 'gears'}

    for f in fields:
        projection[SINGLETON_KEY + '.' + f] = 1

    gear_doc = config.db.singletons.find_one(query, projection)

    # print gear_doc
    return gear_doc[SINGLETON_KEY]

def get_gear_by_name(name):

    # Find a gear from the list by name
    gear_doc = config.db.singletons.find_one(
        {'_id': 'gears'},
        {SINGLETON_KEY: { '$elemMatch': {
            'name': name
        }}
    })

    if gear_doc is None or gear_doc.get(SINGLETON_KEY) is None:
        raise Exception('Unknown gear ' + name)

    # Mongo returns the full document: { '_id' : 'gears', 'gear_list' : [ { .. } ] }, so strip that out
    return gear_doc[SINGLETON_KEY][0]

def insert_gear(doc):
    config.db.singletons.update(
        {"_id" : "gears"},
        {'$push': {'gear_list': doc} }
    )

def remove_gear(name):
    config.db.singletons.update(
        {"_id" : "gears"},
        {'$pull': {'gear_list':{ 'name': name }} }
    )

def upsert_gear(doc):
    remove_gear(doc['name'])
    insert_gear(doc)
