#!/usr/bin/env python

import json
import sys
import logging
from api import config

CURRENT_DATABASE_VERSION = 1 # An int that is bumped when a new schema change is made

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
    # scitran/core issue #206
    config.db.version.insert_one({'_id': 'version', 'database': CURRENT_DATABASE_VERSION})

def upgrade_schema():
    """
    Upgrades db to the current schema version
    
    Returns (0) if upgrade is successful
    """

    db_version = get_db_version()
    try:
        if db_version < 1:
            upgrade_to_1()
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
    logging.exception()
    sys.exit(1)
