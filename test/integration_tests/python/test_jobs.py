import json
import logging
from copy import deepcopy


log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_jobs_access(as_user):
    r = as_user.get('/jobs')
    assert r.status_code == 403

    r = as_user.get('/jobs/next')
    assert r.status_code == 403

    r = as_user.get('/jobs/stats')
    assert r.status_code == 403

    r = as_user.post('/jobs/reap')
    assert r.status_code == 403

    r = as_user.get('/jobs/test-job')
    assert r.status_code == 403

    r = as_user.get('/jobs/test-job/config.json')
    assert r.status_code == 403


def test_jobs(with_gear, with_hierarchy, as_user, as_admin):
    gear = with_gear
    data = with_hierarchy

    job1 = {
        'gear_id': gear,
        'inputs': {
            'dicom': {
                'type': 'acquisition',
                'id': data.acquisition,
                'name': 'test.zip'
            }
        },
        'config': { 'two-digit multiple of ten': 20 },
        'destination': {
            'type': 'acquisition',
            'id': data.acquisition
        },
        'tags': [ 'test-tag' ]
    }

    # add job with explicit destination
    r = as_user.post('/jobs/add', json=job1)
    assert r.ok
    job1_id = r.json()['_id']

    # get job
    r = as_admin.get('/jobs/' + job1_id)
    assert r.ok

    # get job config
    r = as_admin.get('/jobs/' + job1_id + '/config.json')
    assert r.ok

    # try to update job (user may only cancel)
    r = as_user.put('/jobs/' + job1_id, json={'test': 'invalid'})
    assert r.status_code == 403

    # add job with implicit destination
    job2 = deepcopy(job1)
    del job2['destination']
    r = as_user.post('/jobs/add', json=job2)
    assert r.ok

    # add job with invalid gear
    r = as_admin.get('/gears/' + gear)
    assert r.ok
    invalid_gear = r.json()
    del invalid_gear['_id']
    invalid_gear['gear']['name'] = 'invalid'
    invalid_gear['gear']['custom'] = {'flywheel': {'invalid': True}}
    invalid_gear_id = as_admin.post('/gears/invalid', json=invalid_gear).json()['_id']

    job3 = deepcopy(job2)
    job3['gear_id'] = invalid_gear_id

    r = as_user.post('/jobs/add', json=job3)
    assert r.status_code == 400

    as_admin.delete('/gears/' + invalid_gear_id)

    # get next job - with nonexistent tag
    r = as_admin.get('/jobs/next?tags=fake-tag')
    assert r.status_code == 400

    # get next job - without tags
    r = as_admin.get('/jobs/next')
    assert r.status_code == 200

    # retry job
    r = as_admin.put('/jobs/' + job1_id, json={'state': 'failed'})
    assert r.ok

    r = as_user.post('/jobs/' + job1_id + '/retry')
    assert r.ok
