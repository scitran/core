#!/usr/bin/env python
#
# @author:  Gunnar Schaefer, Kevin S. Hahn

import os
import sys
import site
import ConfigParser

configfile = '../production.ini'

config = ConfigParser.ConfigParser()
config.read(configfile)

site.addsitedir(os.path.join(config.get('nims', 'virtualenv'), 'lib', 'python2.7', 'site-packages'))
sys.path.append(config.get('nims', 'here'))
os.environ['PYTHON_EGG_CACHE'] = config.get('nims', 'python_egg_cache')

import pymongo
import webapp2
import nimsapi
import nimsutil
import ConfigParser
import Crypto.PublicKey.RSA

import logging
import logging.config

logging.config.fileConfig(configfile)
log = logging.getLogger('nimsapi')

privkey_file = config.get('nims', 'privkey_file')
db_uri = config.get('nims', 'db_uri')
stage_path = config.get('nims', 'stage_path')

db_client = pymongo.MongoReplicaSetClient(db_uri) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri)

try:
    privkey = Crypto.PublicKey.RSA.importKey(open(privkey_file).read())
except:
    log.warn(privkey_file + ' is not a valid private SSL key file') # FIXME use logging
    privkey = None
else:
    log.info('successfully loaded private SSL key from ' + privkey_file) # FIXME use logging

application = nimsapi.app
application.config['stage_path'] = config.get('nims', 'stage_path')
application.config['site_id'] = config.get('nims', 'site_id')
application.config['privkey']  = privkey
application.db = db_client.get_default_database()
