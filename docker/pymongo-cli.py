import os
import pymongo
from bson import ObjectId
db_uri = os.getenv('SCITRAN_PERSISTENT_DB_URI')
db = pymongo.MongoClient(db_uri).get_default_database()
