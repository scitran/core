import requests
import json

import warnings
warnings.filterwarnings('ignore')

base_url = 'https://localhost:8443/api2'

def test_sequence():
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
