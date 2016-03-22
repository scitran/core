import requests
import json
import time
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_groups(api_as_admin, data_builder):
    group_id = 'test_group_' + str(int(time.time() * 1000))

    # Cannot find a non-existant group
    r = api_as_admin.get('/groups/' + group_id)
    assert r.status_code == 404

    data_builder.create_group(group_id)

    # Able to find new group
    r = api_as_admin.get('/groups/' + group_id)
    assert r.ok

    # Able to change group name
    payload = json.dumps({'name': 'Test group'})
    r = api_as_admin.put('/groups/' + group_id, data=payload)
    assert r.ok

    data_builder.delete_group(group_id)
