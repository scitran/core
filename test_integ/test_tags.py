import requests
import json
import warnings
from nose.tools import with_setup
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)
log.setLevel(logging.INFO)
warnings.filterwarnings('ignore')

adm_user = 'rfrigato@stanford.edu'
test_data = type('',(object,),{})()
base_url = 'https://localhost:8443/api'

def _build_url_and_payload(method, tag, newtag=None, requestor=adm_user):
    if method == 'POST':
        url = test_data.proj_url + '?user=' + requestor
        payload = json.dumps({'value': tag})
    else:
        url = test_data.proj_url + '/' + tag + '?user=' + requestor
        payload = json.dumps({'value': newtag})
    return url, payload


def setup_db():
    payload = {
        'files': [],
        'group': 'unknown',
        'label': 'SciTran/Testing',
        'public': False,
        'permissions': []
    }
    payload = json.dumps(payload)
    r = requests.post(base_url + '/projects?user=rfrigato@stanford.edu', data=payload, verify=False)
    test_data.pid = json.loads(r.content)['_id']
    assert r.ok
    log.debug('pid = \'{}\''.format(test_data.pid))
    test_data.proj_url = base_url + '/projects/{}/tags'.format(test_data.pid)

def teardown_db():
    r = requests.delete(base_url + '/projects/' + test_data.pid + '?user=rfrigato@stanford.edu', verify=False)
    assert r.ok

@with_setup(setup_db, teardown_db)
def test_tags():
    tag = 'test_tag'
    new_tag = 'new_test_tag'
    other_tag = 'other_test_tag'
    url_get_tag, _ = _build_url_and_payload('GET', tag)
    url_get_new, _ = _build_url_and_payload('GET', new_tag)
    url_get_other, _ = _build_url_and_payload('GET', other_tag)

    r = requests.get(url_get_tag, verify=False)
    assert r.status_code == 404
    url_post, payload = _build_url_and_payload('POST', tag)
    r = requests.post(url_post, data=payload, verify=False)
    assert r.ok
    r = requests.get(url_get_tag, verify=False)
    assert r.ok
    assert json.loads(r.content) == tag

    url_post, payload = _build_url_and_payload('POST', new_tag)
    r = requests.post(url_post, data=payload, verify=False)
    assert r.ok
    url_post, payload = _build_url_and_payload('POST', new_tag)
    r = requests.post(url_post, data=payload, verify=False)
    assert r.status_code == 404
    r = requests.get(url_get_new, verify=False)
    assert r.ok
    assert json.loads(r.content) == new_tag

    url_put, payload = _build_url_and_payload('PUT', tag, new_tag)
    r = requests.put(url_put, data=payload, verify=False)
    assert r.status_code == 404

    r = requests.get(url_get_other, verify=False)
    assert r.status_code == 404
    url_put, payload = _build_url_and_payload('PUT', tag, other_tag)
    r = requests.put(url_put, data=payload, verify=False)
    assert r.ok
    r = requests.get(url_get_other, verify=False)
    assert r.ok
    assert json.loads(r.content) == other_tag

    r = requests.get(url_get_tag, verify=False)
    assert r.status_code == 404
    r = requests.delete(url_get_other, verify=False) # url for 'DELETE' is the same as the one for 'GET'
    assert r.ok
    r = requests.get(url_get_other, verify=False)
    assert r.status_code == 404
    r = requests.delete(url_get_new, verify=False) # url for 'DELETE' is the same as the one for 'GET'
    assert r.ok
    r = requests.get(url_get_new, verify=False)
    assert r.status_code == 404