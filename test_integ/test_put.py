import requests
import json
import logging
log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)
import warnings
warnings.filterwarnings('ignore')
base_url = 'https://localhost:8443/api2'
_id = '5615cace675bee22a7882d01'

def test_put():
    payload = {
        'group': 'scitran',
    }
    payload = json.dumps(payload)
    r = requests.put(base_url + '/projects/' + _id + '?user=rfrigato@stanford.edu', data=payload, verify=False)
    assert r.ok
    log.warning(r.content)
