#!/usr/bin/env python

import json
import bson
import sys
import logging
from api import config

CURRENT_DATABASE_VERSION = 3 # An int that is bumped when a new schema change is made

def get_db_version():

    version = config.get_version()
    if version is None or version.get('database', None) is None:
        return 0
    else:
        return version.get('database')


def confirm_schema_match():
    """
    Checks version of database schema

    Returns (0)  if DB schema version matches requirements.
    Returns (42) if DB schema version does not match
                 requirements and can be upgraded.
    Returns (43) if DB schema version does not match
                 requirements and cannot be upgraded,
                 perhaps because code is at lower version
                 than the DB schema version.
    """

    db_version = get_db_version()
    if not isinstance(db_version, int) or db_version > CURRENT_DATABASE_VERSION:
        logging.error('The stored db schema version of %s is incompatible with required version %s',
                       str(db_version), CURRENT_DATABASE_VERSION)
        sys.exit(43)
    elif db_version < CURRENT_DATABASE_VERSION:
        sys.exit(42)
    else:
        sys.exit(0)

def upgrade_to_1():
    """
    scitran/core issue #206

    Initialize db version to 1
    """
    config.db.version.insert_one({'_id': 'version', 'database': 1})

def upgrade_to_2():
    """
    scitran/core PR #236

    Set file.origin.name to id if does not exist
    Set file.origin.method to '' if does not exist
    """

    def update_file_origins(cont_list, cont_name):
        for container in cont_list:
            updated_files = []
            for file in container.get('files', []):
                origin = file.get('origin')
                if origin is not None:
                    if origin.get('name', None) is None:
                        file['origin']['name'] = origin['id']
                    if origin.get('method', None) is None:
                        file['origin']['method'] = ''
                updated_files.append(file)

            query = {'_id': container['_id']}
            update = {'$set': {'files': updated_files}}
            result = config.db[cont_name].update_one(query, update)

    query = {'$and':[{'files.origin.name': { '$exists': False}}, {'files.origin.id': { '$exists': True}}]}

    update_file_origins(config.db.collections.find(query), 'collections')
    update_file_origins(config.db.projects.find(query), 'projects')
    update_file_origins(config.db.sessions.find(query), 'sessions')
    update_file_origins(config.db.acquisitions.find(query), 'acquisitions')

def upgrade_to_3():
    """
    scitran/core PR #263

    Set User as curator of collection if:
      - collection has no curator
      - User is the only user with admin perms for the collection
    """

    pipeline = [
        {'$match': {'curator': {'$exists': False}, 'permissions.access': 'admin'}},
        {'$unwind': '$permissions'},
        {'$project': {'cid': '$_id', 'access': '$permissions.access', 'site': '$permissions.site',  'user': '$permissions._id'}},
        {'$group' : { '_id' : {'cid': '$cid', 'access': '$access'}, 'users': {'$push': '$user' }}},
        {'$match': {'_id.access': 'admin', 'users': {'$size': 1}}}
    ]

    collections = config.db.command('aggregate', 'collections', pipeline=pipeline)
    for collection in collections['result']:
        cid = collection['_id']['cid']
        uid = collection['users'][0]
        config.db.collections.update_one({'_id': cid}, {'$set': {'curator': uid}})
    logging.warn(collections)

def upgrade_schema():
    """
    Upgrades db to the current schema version

    Returns (0) if upgrade is successful
    """

    db_version = get_db_version()
    try:
        if db_version < 1:
            upgrade_to_1()
        if db_version < 2:
            upgrade_to_2()
        if db_version < 3:
            upgrade_to_3()
    except Exception as e:
        logging.exception('Incremental upgrade of db failed')
        sys.exit(1)
    else:
        config.db.version.update_one({'_id': 'version'}, {'$set': {'database': CURRENT_DATABASE_VERSION}})
        sys.exit(0)

try:
    if len(sys.argv) > 1:
        if sys.argv[1] == 'confirm_schema_match':
            confirm_schema_match()
        elif sys.argv[1] == 'upgrade_schema':
            upgrade_schema()
        else:
            logging.error('Unknown method name given as argv to database.py')
            sys.exit(1)
    else:
        logging.error('No method name given as argv to database.py')
        sys.exit(1)
except Exception as e:
    logging.exception('Unexpected error in database.py')
    sys.exit(1)
