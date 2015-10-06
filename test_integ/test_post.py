import requests
import json
import logging
log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)
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
    log.warning(_id)
