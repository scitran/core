#!/usr/bin/env python

import json
from api import config

CURRENT_DATABASE_VERSION = 1 # An int that is bumped when a new 

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

	version = config.db.version.find_one({"_id": "version"})
	if version is None or version.get('database', None) is None:
		return 42 # At version 0

	db_version = version.get('database', 0)
	if not isinstance(db_version, int) or db_version > CURRENT_DATABASE_VERSION:
		return 43 
	elif db_version < CURRENT_DATABASE_VERSION:
		return 42
	else:
		return 0

def upgrade_schema():
	"""
	Upgrades db to the current schema version
    
    Returns (0) if upgrade is successful
    """

	# In progress
	# db_version = version.get('database',0)
	
	# if db_version < 1:
	# 	# rename the metadata fields
	# 	config.db.container.update_many({}, {"$rename": {"metadata": "info"}})

	# config.db.version.update_one({"_id": "version"}, {"$set": {"database": CURRENT_DATABASE_VERSION}})
	return 0
