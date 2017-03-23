import json
import time
import pytest
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_permissions(with_a_group_and_a_project, as_admin):
    data = with_a_group_and_a_project
    permissions_path = '/projects/' + data.project_id + '/permissions'
    user_1_local_path = permissions_path + '/local/' + data.user_1
    user_2_local_path = permissions_path + '/local/' + data.user_2
    user_2_another_path = permissions_path + '/another/' + data.user_2

    # GET is not allowed for general permissions path
    r = as_admin.get(permissions_path)
    assert r.status_code == 405

    # Add permissions for user 1
    payload = json.dumps({
        '_id': data.user_1,
        'site': 'local',
        'access': 'ro'
    })
    r = as_admin.post(permissions_path, data=payload)
    assert r.ok

    # Verify permissions for user 1
    r = as_admin.get(user_1_local_path)
    assert r.ok
    content = json.loads(r.content)
    assert content['_id'] == data.user_1
    assert content['site'] == 'local'
    assert content['access'] == 'ro'

    # Update user 1 to have admin access
    payload = json.dumps({
        'access': 'admin'
    })
    r = as_admin.put(user_1_local_path, data=payload)
    assert r.ok

    # Add user 2 to have ro access
    payload = json.dumps({
        '_id': data.user_2,
        'site': 'local',
        'access': 'ro'
    })
    r = as_admin.post(permissions_path, data=payload)
    assert r.ok

    # Attempt to change user 2's id to user 1
    payload = json.dumps({
        '_id': data.user_1
    })
    r = as_admin.put(user_2_local_path, data=payload)
    assert r.status_code == 404

    # Change user 2's site
    payload = json.dumps({
        'site': 'another'
    })
    r = as_admin.put(user_2_local_path, data=payload)
    assert r.ok

    # Verify user 2's site changed
    r = as_admin.get(user_2_another_path)
    assert r.ok
    content = json.loads(r.content)
    assert content['_id'] == data.user_2
    assert content['site'] == 'another'
    assert content['access'] == 'ro'

    # Delete user 2
    r = as_admin.delete(user_2_another_path)
    assert r.ok

    # Ensure user 2 is gone
    r = as_admin.get(user_2_another_path)
    assert r.status_code == 404
