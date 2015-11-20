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

adm_user = 'admin@user.com'
user = 'test@user.com'
test_data = type('',(object,),{})()
base_url = 'https://localhost:8443/api'

def _build_url(_id=None, requestor=adm_user):
    if _id is None:
        url = test_data.proj_url + '?user=' + requestor
    else:
        url = test_data.proj_url + '/' + _id + '?user=' + requestor
    return url


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
    test_data.proj_url = base_url + '/projects/{}/notes'.format(test_data.pid)
    data = {
        '_id': user,
        'site': 'local',
        'access': 'rw'
    }
    url = base_url + '/projects/' + test_data.pid + '/permissions?user=admin@user.com&root=true'
    r = requests.post(url, data=json.dumps(data), verify=False)
    assert r.ok

def teardown_db():
    r = requests.delete(base_url + '/projects/' + test_data.pid + '?user=admin@user.com&root=true', verify=False)
    assert r.ok

@with_setup(setup_db, teardown_db)
def test_notes():
    url_post = _build_url(requestor=user)
    data = {'user': user, 'text':'test note'}
    r = requests.post(url_post, data=json.dumps(data), verify=False)
    assert r.ok
    r = requests.get(base_url + '/projects/{}?user={}'.format(test_data.pid, adm_user), verify=False)
    assert r.ok
    p = json.loads(r.content)
    assert len(p['notes']) == 1
    assert p['notes'][0]['user'] == user
    note_id = p['notes'][0]['_id']
    url_get = _build_url(note_id, user)
    r = requests.get(url_get, verify=False)
    assert r.ok
    assert json.loads(r.content)['_id'] == note_id
    data = {'text':'modified test note'}
    r = requests.put(url_get, data=json.dumps(data), verify=False)
    assert r.ok
    r = requests.get(url_get, verify=False)
    assert r.ok
    assert json.loads(r.content)['text'] == 'modified test note'
    r = requests.delete(url_get, verify=False)
    assert r.ok
    r = requests.get(url_get, verify=False)
    assert r.status_code == 404

