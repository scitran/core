import time
import logging
import pymongo
import datetime

log = logging.getLogger('scitran.api')

db = None


def configure_db(db_uri, site_id, site_name, api_uri):
    global db
    for i in range(3):
        try:
            db = pymongo.MongoClient(db_uri, j=True).get_default_database()
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
            return
        except Exception as e:
            db = None
            log.warning('DB not available...trying again in {} seconds'.format((i + 1) * 2))
            time.sleep((i + 1) * 2)
    raise e
