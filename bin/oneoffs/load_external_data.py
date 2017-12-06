#!/usr/bin/env python

import bson
import copy
import datetime
import dateutil.parser
import json

from api import config

## DEFAULTS ##

USER_ID = "meganhenning@flywheel.io"
SAFE_FILE_HASH = "v0-sha384-a8d0d1bd9368e5385f31d3582db07f9bc257537d5e1f207d36a91fdd3d2f188fff56616c0874bb3535c37fdf761a446c"
PROJECT_ID = "5a26e049c6fa4a00161e4a1a"
GROUP_ID = 'scitran'

# Some day maybe this can use the SDK/API calls to get the proper test data
# For now, paste it in

SESSIONS = []

ACQUISITIONS = []

def handle_permissions(obj):
	obj['permissions'] = [{
		"access": "admin",
		"_id": USER_ID
	}]

def handle_dates(obj):
	if obj.get('timestamp'):
		obj['timestamp'] = dateutil.parser.parse(obj['timestamp'])
	if obj.get('created'):
		obj['created'] = dateutil.parser.parse(obj['created'])
	if obj.get('modified'):
		obj['modified'] = dateutil.parser.parse(obj['modified'])

def handle_file(f):
	handle_dates(f)
	f.pop('info_exists', None)
	f.pop('join_origin', None)
	f['hash'] = SAFE_FILE_HASH


for i, s in enumerate(SESSIONS):
	print "Processing session {} of {} sessions".format(i+1, len(SESSIONS))

	s.pop('join-origin', None)

	s['_id'] = bson.ObjectId(s['_id'])
	s['project'] = bson.ObjectId(str(PROJECT_ID))
	s['group'] = GROUP_ID
	handle_dates(s)
	handle_permissions(s)

	for f in s.get('files', []):
		handle_file(f)


	config.db.sessions.delete_many({'_id': s['_id']})
	config.db.sessions.insert(s)

for i, a in enumerate(ACQUISITIONS):
	print "Processing acquisition {} of {} acquisitions".format(i+1, len(ACQUISITIONS))

	a['_id'] = bson.ObjectId(a['_id'])
	a['session'] = bson.ObjectId(a['session'])

	a.pop('join-origin', None)

	handle_dates(a)
	handle_permissions(a)

	for f in a.get('files', []):
		handle_file(f)

	config.db.acquisitions.delete_many({'_id': a['_id']})
	config.db.acquisitions.insert(a)
