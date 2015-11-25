import pymongo

db = None

def configure_db(db_uri):
    global db
    db = pymongo.MongoClient(db_uri, j=True, connectTimeoutMS=2000, serverSelectionTimeoutMS=3000).get_default_database()
