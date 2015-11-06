import requests
import json
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)
log.setLevel(logging.INFO)

import warnings

warnings.filterwarnings('ignore')

from nose.tools import with_setup
import pymongo
from bson.objectid import ObjectId

db = pymongo.MongoClient('mongodb://localhost:9001/scitran').get_default_database()

base_url = 'https://localhost:8443/api'
test_data = type('',(object,),{})()

def setup_db():
    payload = {
        'group': 'unknown',
        'label': 'SciTran/Testing',
        'public': False
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/projects?user=admin@user.com&root=true', data=payload, verify=False)
    test_data.pid = json.loads(r.content)['_id']
    assert r.ok
    log.debug('pid = \'{}\''.format(test_data.pid))

    payload = {
        'project': test_data.pid,
        'label': 'session_testing',
        'public': False
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/sessions?user=admin@user.com&root=true', data=payload, verify=False)
    assert r.ok
    test_data.sid = json.loads(r.content)['_id']
    log.debug('sid = \'{}\''.format(test_data.sid))

    payload = {
        'session': test_data.sid,
        'label': 'acq_testing',
        'public': False
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/acquisitions?user=admin@user.com&root=true', data=payload, verify=False)
    assert r.ok
    test_data.aid = json.loads(r.content)['_id']
    log.debug('aid = \'{}\''.format(test_data.aid))

def teardown_db():
    r = requests.delete(base_url + '/acquisitions/' + test_data.aid + '?user=admin@user.com&root=true', verify=False)
    assert r.ok
    r = requests.delete(base_url + '/sessions/' + test_data.sid + '?user=admin@user.com&root=true', verify=False)
    assert r.ok
    r = requests.delete(base_url + '/projects/' + test_data.pid + '?user=admin@user.com&root=true', verify=False)
    assert r.ok


@with_setup(setup_db, teardown_db)
def test_sequence():
    payload = {
        'curator': 'admin@user.com',
        'label': 'SciTran/Testing',
        'public': True
    }
    r = requests.post(base_url + '/collections?user=admin@user.com', data=json.dumps(payload), verify=False)
    assert r.ok
    _id = json.loads(r.content)['_id']
    log.debug('_id = \'{}\''.format(_id))
    r = requests.get(base_url + '/collections/' + _id + '?user=admin@user.com', verify=False)
    assert r.ok
    payload = {
        'contents':{
            'nodes':
            [{
                'level': 'session',
                '_id': test_data.sid
            }],
            'operation': 'add'
        }
    }
    r = requests.put(base_url + '/collections/' + _id + '?user=admin@user.com', data=json.dumps(payload), verify=False)
    assert r.ok
    r = requests.get(base_url + '/collections/' + _id + '/acquisitions?session=' + test_data.sid + '&user=admin@user.com', verify=False)
    assert r.ok
    coll_acq_id= json.loads(r.content)[0]['_id']
    assert coll_acq_id  == test_data.aid
    acq_ids = [ObjectId(test_data.aid)]
    acs = db.acquisitions.find({'_id': {'$in': acq_ids}})
    for ac in acs:
        assert len(ac['collections']) == 1
        assert ac['collections'][0] == ObjectId(_id)
    r = requests.delete(base_url + '/collections/' + _id + '?user=admin@user.com', verify=False)
    assert r.ok
    r = requests.get(base_url + '/collections/' + _id + '?user=admin@user.com', verify=False)
    assert r.status_code == 404
    acs = db.acquisitions.find({'_id': {'$in': acq_ids}})
    for ac in acs:
        assert len(ac['collections']) == 0
