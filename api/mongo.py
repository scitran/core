import pymongo
import datetime

db = None


def configure_db(db_uri, site_id, site_name, api_uri):
    global db
    db = pymongo.MongoClient(db_uri).get_default_database()
    # TODO jobs indexes
    # TODO review all indexes
    db.projects.create_index([('gid', 1), ('name', 1)])
    db.sessions.create_index('project')
    db.sessions.create_index('uid')
    db.acquisitions.create_index('session')
    db.acquisitions.create_index('uid')
    db.acquisitions.create_index('collections')
    db.authtokens.create_index('timestamp', expireAfterSeconds=600)
    db.uploads.create_index('timestamp', expireAfterSeconds=60)
    db.downloads.create_index('timestamp', expireAfterSeconds=60)

    now = datetime.datetime.utcnow()
    db.groups.update_one({'_id': 'unknown'}, {'$setOnInsert': { 'created': now, 'modified': now, 'name': 'Unknown', 'roles': []}}, upsert=True)
    db.sites.replace_one({'_id': site_id}, {'name': site_name, 'api_uri': api_uri}, upsert=True)


