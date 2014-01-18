#!/usr/bin/env python
#
# @author:  Gunnar Schaefer, Kevin S. Hahn

import os
import sys
import site
import ConfigParser
import logging
import logging.config


def apply_config(configfile):
    """Return a ConfigParser object"""
    config = ConfigParser.ConfigParser()
    config.read(configfile)
    site.addsitedir(os.path.join(config.get('nims', 'virtualenv'), 'lib', 'python2.7', 'site-packages'))
    sys.path.append(config.get('nims', 'here'))
    os.environ['PYTHON_EGG_CACHE'] = config.get('nims', 'python_egg_cache')
    return config


def configure_logger(configfile):
    """return a nimsapi configured logger"""
    logging.config.fileConfig(configfile, disable_existing_loggers=False)
    return logging.getLogger('nimsapi')


def read_privkey(privkey_file):
    """reads SSL private key. returns key content as RSA.key object"""
    try:
        privkey = Crypto.PublicKey.RSA.importKey(open(privkey_file).read())
    except:
        log.warn(privkey_file + ' is not a valid private SSL key file')
        privkey = None
    else:
        log.info('successfully loaded private SSL key from ' + privkey_file)
    return privkey


def connect_db(db_uri):
    """return mongodb default database"""
    kwargs = dict(tz_aware=True)
    db_client = pymongo.MongoReplicaSetClient(db_uri, **kwargs) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri, **kwargs)
    db = db_client.get_default_database()
    db.remotes.ensure_index('UTC', expireAfterSeconds=120)
    return db


def internimsclient(db, hostname, site_id, privkey, internims_url):
    """sends is alive to internims central. no return"""
    # TODO: create a list of non-local users, who have permissions on experiments
    users = [users['_id'] for users in list(db.users.find({}, {'_id': True}))]

    payload = json.dumps({'iid': site_id, 'hostname': hostname, 'users': users})
    h = Crypto.Hash.SHA.new(payload)
    signature = Crypto.Signature.PKCS1_v1_5.new(privkey).sign(h)
    headers = {'Authorization': base64.b64encode(signature)}

    r = requests.post(url=internims_url, data=payload, headers=headers, verify=True)
    if r.status_code == 200:
        sites = json.loads(r.content)
        for site in sites:
            site['UTC'] = datetime.datetime.strptime(site['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
            db.remotes.find_and_modify(query={'_id': site['_id']}, update=site, upsert=True)
            log.debug('upserting remote site ' + site['_id'])
    else:
        log.info((r.status_code, r.reason))


if __name__ == '__main__':
    import argparse

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', '--configfile',  help='path to configuration file')
    arg_parser.add_argument('--internims_url', help='https://internims.appspot.com')
    arg_parser.add_argument('--db_uri', help='DB URI')
    arg_parser.add_argument('--hostname', help='fqdn, without protocol')
    arg_parser.add_argument('--site_id', help='instance ID')
    arg_parser.add_argument('--sleeptime', default=60, type=int, help='time to sleep between is alive signals')
    arg_parser.add_argument('-k', '--privkey', help='path to privkey file')
    args = arg_parser.parse_args()

    # read config
    config = apply_config(args.configfile)

    # import everything else
    import json
    import time
    import base64
    import pymongo
    import webapp2
    import nimsapi
    import nimsutil
    import requests
    import datetime
    import Crypto.Random
    import Crypto.Hash.SHA
    import Crypto.PublicKey.RSA
    import Crypto.Signature.PKCS1_v1_5
    import signal                   # not in uwsgi execution

    # configure logger
    log = configure_logger(args.configfile)

    # args override configfile
    db_uri = args.db_uri or config.get('nims', 'db_uri')
    hostname = args.hostname or config.get('nims', 'hostname')
    site_id = args.site_id or config.get('nims', 'site_id')
    internims_url = args.internims_url or config.get('nims', 'internims_url')
    privkey_file = args.privkey or config.get('nims', 'privkey_file')

    # load in the privkey
    privkey = read_privkey(privkey_file)

    # connect to db
    db = connect_db(db_uri)

    def term_handler(signum, stack):
        alive = False
        log.debug('Recieved SIGTERM - shuttin down')

    signal.signal(signal.SIGTERM, term_handler)

    alive = True
    while alive:
        internimsclient(db, hostname, site_id, privkey, internims_url)
        time.sleep(args.sleeptime)

else:
    # read config
    configfile = '../production.ini'
    config = apply_config(configfile)

    # import everything else
    import json
    import time
    import base64
    import pymongo
    import webapp2
    import nimsapi
    import argparse
    import datetime
    import nimsutil
    import requests
    import Crypto.Random
    import Crypto.Hash.SHA
    import Crypto.PublicKey.RSA
    import Crypto.Signature.PKCS1_v1_5
    import uwsgidecorators              # only in uwsgi execution

    # configure logger
    log = configure_logger(configfile)

    db_uri = config.get('nims', 'db_uri')
    stage_path = config.get('nims', 'stage_path')
    site_id = config.get('nims', 'site_id')

    # load in privkey
    privkey = read_privkey(config.get('nims', 'privkey_file'))

    # config uwsgi application
    application = nimsapi.app
    application.config['stage_path'] = stage_path
    application.config['site_id'] = site_id
    application.config['privkey']  = privkey

    # connect db
    application.db = connect_db(db_uri)

    @uwsgidecorators.postfork
    def random_atfork():
        Crypto.Random.atfork()

    @uwsgidecorators.timer(60)
    def internimsclient_timer(signum):
        internimsclient(application.db, config.get('nims', 'hostname'), config.get('nims', 'site_id'), privkey, config.get('nims', 'internims_url'))
