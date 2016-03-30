import os
import copy
import glob
import logging
import pymongo
import datetime
import elasticsearch


logging.basicConfig(
    format='%(asctime)s %(name)16.16s %(filename)24.24s %(lineno)5d:%(levelname)4.4s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG,
)
log = logging.getLogger('scitran.api')

logging.getLogger('MARKDOWN').setLevel(logging.WARNING) # silence Markdown library
logging.getLogger('requests').setLevel(logging.WARNING) # silence Requests library
logging.getLogger('paste.httpserver').setLevel(logging.WARNING) # silence Paste library


# NOTE: Keep in sync with environment variables in sample.config file.
DEFAULT_CONFIG = {
    'core': {
        'log_level': 'info',
        'debug': False,
        'insecure': False,
        'newrelic': None,
        'drone_secret': None,
    },
    'site': {
        'id': 'local',
        'name': 'Local',
        'api_url': 'https://localhost/api',
        'central_url': 'https://sdmc.scitran.io/api',
        'registered': False,
        'ssl_cert': None,
    },
    'auth': {
        'client_id': '1052740023071-n20pk8h5uepdua3r8971pc6jrf25lvee.apps.googleusercontent.com',
        'id_endpoint': 'https://www.googleapis.com/plus/v1/people/me/openIdConnect',
        'auth_endpoint': 'https://accounts.google.com/o/oauth2/auth',
        'verify_endpoint': 'https://www.googleapis.com/oauth2/v1/tokeninfo',
    },
    'persistent': {
        'db_uri': 'mongodb://localhost:9001/scitran',
        'db_connect_timeout': '2000',
        'db_server_selection_timeout': '3000',
        'data_path': os.path.join(os.path.dirname(__file__), '../persistent/data'),
        'schema_path': 'api/schemas',
        'elasticsearch_host': 'localhost:9200',
    },
}

__config = copy.deepcopy(DEFAULT_CONFIG)
__config_persisted = False
__last_update = datetime.datetime.utcfromtimestamp(0)

#FIXME What is this?
#os.environ['PYTHON_EGG_CACHE'] = '/tmp/python_egg_cache'
#os.umask(0o022)

for outer_key, scoped_config in __config.iteritems():
    for inner_key in scoped_config:
        key = 'SCITRAN_' + outer_key.upper() + '_' + inner_key.upper()
        if key in os.environ:
            value = os.environ[key]
            if value.lower() == 'true':
                value = True
            elif value.lower() == 'false':
                value = False
            elif value.lower() == 'none':
                value = None
            __config[outer_key][inner_key] = value

if not os.path.exists(__config['persistent']['data_path']):
    os.makedirs(__config['persistent']['data_path'])

log.setLevel(getattr(logging, __config['core']['log_level'].upper()))

db = pymongo.MongoClient(
    __config['persistent']['db_uri'],
    j=True, # Requests only return once write has hit the DB journal
    connectTimeoutMS=__config['persistent']['db_connect_timeout'],
    serverSelectionTimeoutMS=__config['persistent']['db_server_selection_timeout'],
    connect=False, # Connect on first operation to avoid multi-threading related errors
).get_default_database()
log.debug(str(db))

es = elasticsearch.Elasticsearch([__config['persistent']['elasticsearch_host']])

# validate the lists of json schemas
schema_path = __config['persistent']['schema_path']

expected_mongo_schemas = set([
    'acquisition.json',
    'collection.json',
    'container.json',
    'file.json',
    'group.json',
    'note.json',
    'permission.json',
    'project.json',
    'session.json',
    'subject.json',
    'user.json',
    'avatars.json',
    'tag.json'
])
expected_input_schemas = set([
    'acquisition.json',
    'collection.json',
    'container.json',
    'file.json',
    'group.json',
    'note.json',
    'packfile.json',
    'permission.json',
    'project.json',
    'session.json',
    'subject.json',
    'user.json',
    'avatars.json',
    'download.json',
    'tag.json',
    'enginemetadata.json',
    'uploader.json',
    'reaper.json'
])
mongo_schemas = set()
input_schemas = set()

# check that the lists of schemas are correct
for schema_filepath in glob.glob(schema_path + '/mongo/*.json'):
    schema_file = os.path.basename(schema_filepath)
    mongo_schemas.add(schema_file)
    with open(schema_filepath, 'rU') as f:
        pass

assert mongo_schemas == expected_mongo_schemas, '{} is different from {}'.format(mongo_schemas, expected_mongo_schemas)

for schema_filepath in glob.glob(schema_path + '/input/*.json'):
    schema_file = os.path.basename(schema_filepath)
    input_schemas.add(schema_file)
    with open(schema_filepath, 'rU') as f:
        pass

assert input_schemas == expected_input_schemas, '{} is different from {}'.format(input_schemas, expected_input_schemas)

def initialize_db():
    log.info('Initializing database, creating indexes')
    # TODO jobs indexes
    # TODO review all indexes
    db.projects.create_index([('gid', 1), ('name', 1)])
    db.sessions.create_index('project')
    db.sessions.create_index('uid')
    db.acquisitions.create_index('session')
    db.acquisitions.create_index('uid')
    db.acquisitions.create_index('collections')
    db.authtokens.create_index('timestamp', expireAfterSeconds=604800)
    db.uploads.create_index('timestamp', expireAfterSeconds=60)
    db.downloads.create_index('timestamp', expireAfterSeconds=60)

    now = datetime.datetime.utcnow()
    db.groups.update_one({'_id': 'unknown'}, {'$setOnInsert': { 'created': now, 'modified': now, 'name': 'Unknown', 'roles': []}}, upsert=True)
    db.sites.replace_one({'_id': __config['site']['id']}, {'name': __config['site']['name'], 'site_url': __config['site']['api_url']}, upsert=True)


def get_config():
    global __last_update, __config, __config_persisted
    now = datetime.datetime.utcnow()
    if not __config_persisted:
        initialize_db()
        log.info('Persisting configuration')
        __config['created'] = __config['modified'] = now
        __config['latest'] = True
        r = db.config.replace_one({'latest': True}, __config, upsert=True)
        __config_persisted = bool(r.modified_count)
        __last_update = now
    elif now - __last_update > datetime.timedelta(seconds=120):
        log.debug('Refreshing configuration from database')
        __config = db.config.find_one({'latest': True})
        __last_update = now
        log.setLevel(getattr(logging, __config['core']['log_level'].upper()))
    return __config

def get_public_config():
    return {
        'created': __config.get('created'),
        'modified': __config.get('modified'),
        'site': __config.get('site'),
        'auth': __config.get('auth'),
    }

def get_version():
    return db.version.find_one({'_id': 'version'})

def get_item(outer, inner):
    return get_config()[outer][inner]
