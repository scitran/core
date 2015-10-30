import requests
import json
import logging
log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)

requests.packages.urllib3.disable_warnings()

base_url = 'https://localhost:8443/api'

def test_users():
    _id = 'test@user.com'
    r = requests.get(base_url + '/users/self?user=rfrigato@stanford.edu', verify=False)
    assert r.ok
    r = requests.get(base_url + '/users/' + _id + '?user=rfrigato@stanford.edu&root=true', verify=False)
    assert r.status_code == 404
    payload = {
        '_id': _id,
        'firstname': 'Test',
        'lastname': 'User',
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/users?user=rfrigato@stanford.edu&root=true', data=payload, verify=False)
    assert r.ok
    r = requests.get(base_url + '/users/' + _id + '?user=rfrigato@stanford.edu&root=true', verify=False)
    assert r.ok
    payload = {
        'firstname': 'New'
    }
    payload = json.dumps(payload)
    r = requests.put(base_url + '/users/' + _id + '?user=rfrigato@stanford.edu&root=true', data=payload, verify=False)
    assert r.ok
    r = requests.delete(base_url + '/users/' + _id + '?user=rfrigato@stanford.edu&root=true', verify=False)
    assert r.ok