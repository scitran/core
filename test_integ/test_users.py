import requests
import json
import logging
log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)

requests.packages.urllib3.disable_warnings()

base_url = 'https://localhost:8443/api'

def test_users():
    _id = 'new@user.com'
    r = requests.get(base_url + '/users/self?user=admin@user.com', verify=False)
    assert r.ok
    r = requests.get(base_url + '/users/' + _id + '?user=admin@user.com&root=true', verify=False)
    assert r.status_code == 404
    payload = {
        '_id': _id,
        'firstname': 'New',
        'lastname': 'User',
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/users?user=admin@user.com&root=true', data=payload, verify=False)
    assert r.ok
    r = requests.get(base_url + '/users/' + _id + '?user=admin@user.com&root=true', verify=False)
    assert r.ok
    payload = {
        'firstname': 'Realname'
    }
    payload = json.dumps(payload)
    r = requests.put(base_url + '/users/' + _id + '?user=admin@user.com&root=true', data=payload, verify=False)
    assert r.ok
    r = requests.delete(base_url + '/users/' + _id + '?user=admin@user.com&root=true', verify=False)
    assert r.ok
