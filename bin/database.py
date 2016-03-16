#!/usr/bin/env python

"""
Suggested parameters:
    upgradeSchema       Upgrades the DB to the required schema version.
                        Returns (0) if upgrade is successful

    confirmSchemaMatch  Returns (0)  if DB schema version matches requirements.
                        Returns (42) if DB schema version does not match
                                     requirements and can be upgraded.
                        Returns (43) if DB schema version does not match
                                     requirements and cannot be upgraded,
                                     perhaps because code is at lower version
                                     than the DB schema version.
"""


import json

from api import config


print 'This is the stub database maintenance script.'

print 'DB uri: ' + str(config.get_item('persistent', 'db_uri'))

user = config.db.users.find_one({})

print 'Example user from db: {0}'.format(user)
