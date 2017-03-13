import json
import logging


log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_batch(with_hierarchy, with_gear, with_invalid_gear, as_user, as_admin):
    data = with_hierarchy
    gear = with_gear
    invalid_gear = with_invalid_gear

    # get all
    r = as_user.get('/batch')
    assert r.ok

    # get all w/o enforcing permissions
    r = as_admin.get('/batch')
    assert r.ok

    # try to create batch without gear_id/targets
    r = as_admin.post('/batch', json={})
    assert r.status_code == 400

    # try to create batch with different target container types
    r = as_admin.post('/batch', json={
        'gear_id': gear,
        'targets': [
            {'type': 'session', 'id': 'test-session-id'},
            {'type': 'acquisition', 'id': 'test-acquisition-id'},
        ],
    })
    assert r.status_code == 400

    # try to create batch using an invalid gear
    r = as_admin.post('/batch', json={
        'gear_id': invalid_gear,
        'targets': [{'type': 'session', 'id': 'test-session-id'}],
    })
    assert r.status_code == 400

    # create a batch
    r = as_admin.post('/acquisitions/' + data.acquisition + '/files', files={
        'file': ('test.txt', 'test\ncontent\n')
    })
    assert r.ok

    r = as_admin.post('/batch', json={
        'gear_id': gear,
        'targets': [
            {'type': 'acquisition', 'id': data.acquisition},
        ],
        'target_context': {'type': 'session', 'id': data.session}
    })
    assert r.ok
    batch_id = r.json()['_id']

    # get batch
    r = as_admin.get('/batch/' + batch_id)
    assert r.ok
    assert r.json()['state'] == 'pending'

    # try to cancel non-launched batch
    r = as_admin.post('/batch/' + batch_id + '/cancel')
    assert r.status_code == 400

    # run batch
    r = as_admin.post('/batch/' + batch_id + '/run')
    assert r.ok
    r = as_admin.get('/batch/' + batch_id)
    assert r.json()['state'] == 'launched'

    # try to run non-pending batch
    r = as_admin.post('/batch/' + batch_id + '/run')
    assert r.status_code == 400

    # cancel batch
    r = as_admin.post('/batch/' + batch_id + '/cancel')
    assert r.ok
    r = as_admin.get('/batch/' + batch_id)
    assert r.json()['state'] == 'cancelled'
