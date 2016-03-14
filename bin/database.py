import json

from .. import config


print 'This is the stub database maintenance script.'

print 'DB uri: ' + str(config.get_item('persistent', 'db_uri')))

user = config.db.users.find_one({})
user_j = json.dumps(user)

print 'Example user from db: ' + user_j
