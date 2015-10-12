import requests
import json
from pprint import pprint

import warnings
warnings.filterwarnings('ignore')

base_url = 'https://localhost:8443/api/groups/scitran/roles'

def _build_url_and_payload(method, user, access, requestor, site='local'):
    if method == 'POST':
        url = base_url + '?user=' + requestor
        payload = {
            '_id': user,
            'site': site,
            'access': access
        }
        return url, json.dumps(payload)
    else:
        url = base_url + '/' + site + '/' + user + '?user=' + requestor
        return url, None

adm_user = 'rfrigato@stanford.edu'
user = 'renzo.frigato@gmail.com'

def test_roles():
    url_get, _ = _build_url_and_payload('GET', user, None, adm_user)
    r = requests.get(url_get, verify=False)
    assert r.status_code == 404

    url_post, payload = _build_url_and_payload('POST', user, 'rw', adm_user)
    r = requests.post(url_post, data=payload, verify=False)
    assert r.ok
    r = requests.get(url_get, verify=False)
    assert r.ok
    content = json.loads(r.content)
    assert content['access'] == 'rw'
    assert content['_id'] == user

    url_get_not_auth, _ = _build_url_and_payload('GET', adm_user, None, user)
    r = requests.get(url_get_not_auth, verify=False)
    assert r.status_code == 403

    payload = json.dumps({'access':'admin'})
    r = requests.put(url_get, data=payload, verify=False)
    assert r.ok

    r = requests.get(url_get_not_auth, verify=False)
    assert r.ok

    payload = json.dumps({'access':'rw'})
    r = requests.put(url_get, data=payload, verify=False)
    assert r.ok

    r = requests.get(url_get_not_auth, verify=False)
    assert r.status_code == 403

    r = requests.delete(url_get, verify=False)
    assert r.ok
    r = requests.get(url_get, verify=False)
    assert r.status_code == 404