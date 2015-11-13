import requests
import json
import logging
log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)

requests.packages.urllib3.disable_warnings()

base_url = 'https://localhost:8443/api'

def test_groups():
    _id = 'test'
    r = requests.get(base_url + '/groups/' + _id + '?user=admin@user.com&root=true', verify=False)
    assert r.status_code == 404
    payload = {
        '_id': _id
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/groups?user=admin@user.com&root=true', data=payload, verify=False)
    assert r.ok
    r = requests.get(base_url + '/groups/' + _id + '?user=admin@user.com&root=true', verify=False)
    assert r.ok
    payload = {
        'name': 'Test group',
    }
    payload = json.dumps(payload)
    r = requests.put(base_url + '/groups/' + _id + '?user=admin@user.com&root=true', data=payload, verify=False)
    assert r.ok
    r = requests.delete(base_url + '/groups/' + _id + '?user=admin@user.com&root=true', verify=False)
    assert r.ok
