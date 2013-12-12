#!/usr/bin/env python
#
# @author:  Gunnar Schaefer, Kevin S. Hahn

import os
import sys
import site

site.addsitedir('/var/local/webapp2/lib/python2.7/site-packages')
sys.path.append('/var/local/nims')
os.environ['PYTHON_EGG_CACHE'] = '/tmp/python_egg_cache'


import pymongo
import webapp2
import nimsapi
import nimsutil

log_file = '/var/local/log/nimsapi.log'
pubkey_file = '/var/local/nims/internims/internims.pub'
db_uri = 'mongodb://nims:cnimr750@cnifs.stanford.edu,cnibk.stanford.edu/nims?replicaSet=cni'
stage_path = '/scratch/upload'

nimsutil.configure_log(log_file, False)
db_client = pymongo.MongoReplicaSetClient(db_uri) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri)

try:
    pubkey = open(pubkey_file).read() # FIXME: don't read too much
    # FIXME: verify that this is a valid public key
except IOError:
    pubkey = None

application = nimsapi.app
application.config = dict(stage_path=stage_path, site_id='stanford-cni', pubkey=pubkey)
application.db = db_client.get_default_database()
