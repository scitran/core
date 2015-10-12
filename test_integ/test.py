import requests
import json
import logging
log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)

import warnings
warnings.filterwarnings('ignore')

base_url = 'https://localhost:8443/api2'

def test_projects():
    payload = {
        'files': [],
        'group': 'unknown',
        'label': 'SciTran/Testing',
        'public': False,
        'permissions': []
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/projects?user=rfrigato@stanford.edu', data=payload, verify=False)
    assert r.ok
    _id = json.loads(r.content)['_id']
    r = requests.get(base_url + '/projects/' + _id + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok
    payload = {
        'group': 'scitran',
    }
    payload = json.dumps(payload)
    r = requests.put(base_url + '/projects/' + _id + '?user=rfrigato@stanford.edu', data=payload, verify=False)
    assert r.ok
    r = requests.delete(base_url + '/projects/' + _id + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok

def test_sessions():
    payload = {
        'files': [],
        'group': 'unknown',
        'label': 'SciTran/Testing',
        'public': False,
        'permissions': []
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/projects?user=rfrigato@stanford.edu', data=payload, verify=False)
    pid = json.loads(r.content)['_id']
    assert r.ok
    payload = {
        'files': [],
        'project': pid,
        'label': 'session_testing',
        'public': False,
        'permissions': []
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/sessions?user=rfrigato@stanford.edu', data=payload, verify=False)
    assert r.ok
    _id = json.loads(r.content)['_id']
    r = requests.get(base_url + '/sessions/' + _id + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok
    payload = {
        'files': [],
        'group': 'unknown',
        'label': 'SciTran/Testing 2',
        'public': False,
        'permissions': []
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/projects?user=rfrigato@stanford.edu', data=payload, verify=False)
    new_pid = json.loads(r.content)['_id']
    assert r.ok
    payload = {
        'project': new_pid,
    }
    payload = json.dumps(payload)
    r = requests.put(base_url + '/sessions/' + _id + '?user=rfrigato@stanford.edu', data=payload, verify=False)
    assert r.ok
    r = requests.delete(base_url + '/sessions/' + _id + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok
    r = requests.get(base_url + '/sessions/' + _id + '?user=rfrigato@stanford.edu', verify=False)
    assert r.status_code == 404
    r = requests.delete(base_url + '/projects/' + pid + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok
    r = requests.delete(base_url + '/projects/' + new_pid + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok

def test_acquisitions():
    payload = {
        'files': [],
        'group': 'unknown',
        'label': 'SciTran/Testing',
        'public': False,
        'permissions': []
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/projects?user=rfrigato@stanford.edu', data=payload, verify=False)
    pid = json.loads(r.content)['_id']
    assert r.ok

    payload = {
        'files': [],
        'project': pid,
        'label': 'session_testing',
        'public': False,
        'permissions': []
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/sessions?user=rfrigato@stanford.edu', data=payload, verify=False)
    assert r.ok
    sid = json.loads(r.content)['_id']

    payload = {
        'files': [],
        'project': pid,
        'label': 'session_testing_1',
        'public': False,
        'permissions': []
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/sessions?user=rfrigato@stanford.edu', data=payload, verify=False)
    assert r.ok
    new_sid = json.loads(r.content)['_id']

    payload = {
        'files': [],
        'session': sid,
        'label': 'acq_testing',
        'public': False,
        'permissions': []
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/acquisitions?user=rfrigato@stanford.edu', data=payload, verify=False)
    assert r.ok
    aid = json.loads(r.content)['_id']

    r = requests.get(base_url + '/acquisitions/' + aid + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok

    payload = {
        'session': new_sid
    }
    payload = json.dumps(payload)
    r = requests.put(base_url + '/acquisitions/' + aid + '?user=rfrigato@stanford.edu', data=payload, verify=False)
    assert r.ok

    r = requests.delete(base_url + '/acquisitions/' + aid + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok
    r = requests.get(base_url + '/acquisitions/' + aid + '?user=rfrigato@stanford.edu', verify=False)
    assert r.status_code == 404
    r = requests.delete(base_url + '/sessions/' + sid + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok
    r = requests.delete(base_url + '/sessions/' + new_sid + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok
    r = requests.delete(base_url + '/projects/' + pid + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok
