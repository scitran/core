import pymongo

db = None

def configure_db(db_uri):
    global db
    db = pymongo.MongoClient(db_uri, j=True).get_default_database()
