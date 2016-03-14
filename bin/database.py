#!/usr/bin/env python
import json

from api import config


print 'This is the stub database maintenance script.'

print 'DB uri: ' + str(config.get_item('persistent', 'db_uri'))

user = config.db.users.find_one({})

print 'Example user from db: {0}'.format(user)
