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
import Crypto.PublicKey.RSA

log_file = '/var/local/log/nimsapi.log'
privkey_file = '/var/local/nims/nims.key'
db_uri = 'mongodb://nims:cnimr750@cnifs.stanford.edu,cnibk.stanford.edu/nims?replicaSet=cni'
stage_path = '/scratch/upload'

nimsutil.configure_log(log_file, False)
db_client = pymongo.MongoReplicaSetClient(db_uri) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri)

try:
    privkey = Crypto.PublicKey.RSA.importKey(open(privkey_file).read())
except:
    print privkey_file + ' is not a valid private SSL key file' # FIXME use logging
    privkey = None
else:
    print 'successfully loaded private SSL key from ' + privkey_file # FIXME use logging

application = nimsapi.app
application.config['stage_path'] = stage_path
application.config['site_id'] = 'stanford_cni'
application.config['privkey']  = privkey
application.db = db_client.get_default_database()
