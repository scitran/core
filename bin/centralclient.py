#!/usr/bin/env python

import logging
import time
import pymongo
import argparse

from api.centralclient import log, update, clean_remotes

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--central_url', help='Scitran Central API URL', default='https://sdmc.scitran.io')
arg_parser.add_argument('--db_uri', help='DB URI', required=True)
arg_parser.add_argument('--api_uri', help='API URL, with https:// prefix', required=True)
arg_parser.add_argument('--site_id', help='instance hostname (used as unique ID)', required=True)
arg_parser.add_argument('--site_name', help='instance name', nargs='+', required=True)
arg_parser.add_argument('--ssl_cert', help='path to server ssl certificate file', required=True)
arg_parser.add_argument('--sleeptime', default=60, type=int, help='time to sleep between is alive signals')
arg_parser.add_argument('--debug', help='enable default mode', action='store_true', default=False)
arg_parser.add_argument('--log_level', help='log level [info]', default='info')
args = arg_parser.parse_args()
args.site_name = ' '.join(args.site_name) if args.site_name else None  # site_name as string

logging.basicConfig()
log.setLevel(getattr(logging, args.log_level.upper()))

db = (pymongo.MongoReplicaSetClient(args.db_uri)
      if 'replicaSet' in args.db_uri else
      pymongo.MongoClient(args.db_uri)).get_default_database()

fail_count = 0
while True:
    if not update(db, args.api_uri, args.site_name, args.site_id,
                  args.ssl_cert, args.central_url):
        fail_count += 1
    else:
        fail_count = 0
    if fail_count == 3:
        log.debug('scitran central unreachable, purging all remotes info')
        clean_remotes(db)
    time.sleep(args.sleeptime)
