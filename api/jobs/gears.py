"""
Gears
"""

# import jsonschema
from jsonschema import Draft4Validator
import gear_tools

from .. import config
from .jobs import Job
from ..dao.containerstorage import ContainerStorage

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

def get_invocation_schema(gear):
    return gear_tools.derive_invocation_schema(gear['manifest'])

def suggest_container(gear, cont_name, cid):
    """
    Given a container reference, suggest files that would work well for each input on a gear.
    """

    root = ContainerStorage(cont_name, True).get_container(cid, projection={'permissions':0}, get_children=True)
    invocation_schema = get_invocation_schema(gear)

    schemas = {}
    for x in gear['manifest']['inputs']:
        schema = gear_tools.isolate_file_invocation(invocation_schema, x)
        schemas[x] = Draft4Validator(schema)

    # It would be nice to have use a visitor here instead of manual key loops.
    for acq in root.get('acquisitions', []):
        for f in acq.get('files', []):
            f['suggested'] = {}
            for x in schemas:
                f['suggested'][x] = schemas[x].is_valid(f)

    for analysis in root.get('analyses',{}):
        files = analysis.get('files', [])
        files[:] = [x for x in files if x.get('output')]
        for f in files:
            f['suggested'] = {}
            for x in schemas:
                f['suggested'][x] = schemas[x].is_valid(f)
        analysis['files'] = files

    return root

def insert_gear(doc):
    config.db.singletons.update(
        {"_id" : "gears"},
        {'$push': {'gear_list': doc} }
    )

    if config.get_item('queue', 'prefetch'):
        log.info('Queuing prefetch job for gear ' + doc['name'])

        job = Job(doc['name'], {}, destination={}, tags=['prefetch'], request={
            'inputs': [
                doc['input']
            ],
            'target': {
                'command': ['uname', '-a'],
                'env': {
                    'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
                },
            },
            'outputs': [ ],
        })
        return job.insert()

def remove_gear(name):
    config.db.singletons.update(
        {"_id" : "gears"},
        {'$pull': {'gear_list':{ 'name': name }} }
    )

def upsert_gear(doc):
    remove_gear(doc['name'])
    insert_gear(doc)
