
def test_queue_search(data_builder, default_payload, as_admin, file_form):

    # Dupe of test_jobs.py
    gear_doc = default_payload['gear']['gear']
    gear_doc['inputs'] = {
        'dicom': {
            'base': 'file'
        }
    }
    gear = data_builder.create_gear(gear=gear_doc, category='utility')
    project = data_builder.create_project()
    session = data_builder.create_session()
    acquisition = data_builder.create_acquisition()
    assert as_admin.post('/acquisitions/' + acquisition + '/files', files=file_form('test.zip')).ok

    job_data = {
        'gear_id': gear,
        'inputs': {
            'dicom': {
                'type': 'acquisition',
                'id': acquisition,
                'name': 'test.zip'
            }
        },
        'config': { 'two-digit multiple of ten': 20 },
        'destination': {
            'type': 'acquisition',
            'id': acquisition
        },
        'tags': [ 'test-tag' ]
    }

    r = as_admin.post('/jobs/add', json=job_data)
    assert r.ok
    utility_id = r.json()['_id']

    r = as_admin.get('/sessions/' + session + '/jobs?join=gears')
    assert r.ok
    assert(any(x['id'] == utility_id for x in r.json()['jobs']))

    ana_gear_id = data_builder.create_gear(gear=gear_doc, category='analysis')
    job_data['gear_id'] = ana_gear_id

    r = as_admin.post('/jobs/add', json=job_data)
    assert r.ok
    ana_id = r.json()['_id']


    r = as_admin.get('/sessions/' + session + '/jobs?join=gears')
    assert r.ok
    assert(any(x['id'] == utility_id for x in r.json()['jobs']))
    assert(any(x['id'] == ana_id     for x in r.json()['jobs']))
