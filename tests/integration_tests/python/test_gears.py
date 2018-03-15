def test_gear_add_versioning(default_payload, randstr, data_builder, as_root):
    gear_name = randstr()
    gear_version_1 = '0.0.1'
    gear_version_2 = '0.0.2'

    gear_payload = default_payload['gear']
    gear_payload['gear']['name'] = gear_name

    # create new gear w/ gear_version_1
    gear_payload['gear']['version'] = gear_version_1
    r = as_root.post('/gears/' + gear_name, json=gear_payload)
    assert r.ok
    gear_id_1 = r.json()['_id']

    # get gear by id, test name and version
    r = as_root.get('/gears/' + gear_id_1)
    assert r.ok
    assert r.json()['gear']['name'] == gear_name
    assert r.json()['gear']['version'] == gear_version_1

    # list gears, test gear name occurs only once
    r = as_root.get('/gears', params={'fields': 'all'})
    assert r.ok
    assert sum(gear['gear']['name'] == gear_name for gear in r.json()) == 1

    # create new gear w/ gear_version_2
    gear_payload['gear']['version'] = gear_version_2
    r = as_root.post('/gears/' + gear_name, json=gear_payload)
    assert r.ok
    gear_id_2 = r.json()['_id']

    # get gear by id, test name and version
    r = as_root.get('/gears/' + gear_id_2)
    assert r.ok
    assert r.json()['gear']['name'] == gear_name
    assert r.json()['gear']['version'] == gear_version_2

    # list gears, test gear name occurs only once
    r = as_root.get('/gears', params={'fields': 'all'})
    assert r.ok
    assert sum(gear['gear']['name'] == gear_name for gear in r.json()) == 1

    # try to create gear w/ same name and version (gear_version_2)
    r = as_root.post('/gears/' + gear_name, json=gear_payload)
    assert not r.ok

    # delete gears
    r = as_root.delete('/gears/' + gear_id_1)
    assert r.ok

    r = as_root.delete('/gears/' + gear_id_2)
    assert r.ok


def test_gear_add_invalid(default_payload, randstr, as_root):
    gear_name = 'test-gear-add-invalid-' + randstr()

    # try to add invalid gear - missing name
    r = as_root.post('/gears/' + gear_name, json={})
    assert r.status_code == 400

    # try to add invalid gear - manifest validation error
    r = as_root.post('/gears/' + gear_name, json={'gear': {'name': gear_name}})
    assert r.status_code == 400

    # try to add invalid gear - manifest validation error of non-root-level key
    gear_payload = default_payload['gear']
    gear_payload['gear']['inputs'] = {'invalid': 'inputs'}
    r = as_root.post('/gears/' + gear_name, json=gear_payload)
    assert r.status_code == 400


def test_gear_access(data_builder, as_public, as_admin, as_user):
    gear = data_builder.create_gear()

    # test login required
    r = as_public.get('/gears')
    assert r.status_code == 403

    r = as_public.get('/gears/' + gear)
    assert r.status_code == 403

    r = as_public.get('/gears/' + gear + '/invocation')
    assert r.status_code == 403

    r = as_public.get('/gears/' + gear + '/suggest/test-container/test-id')
    assert r.status_code == 403

    # test superuser required with user
    r = as_user.post('/gears/' + gear, json={'test': 'payload'})
    assert r.status_code == 403

    r = as_user.delete('/gears/' + gear)
    assert r.status_code == 403

    # as_admin has root set to True so it's the same as as_root
    # As far as I can tell this is because the update to set root to True in as_root doesn't work
    # # test superuser required
    # r = as_admin.post('/gears/' + gear, json={'test': 'payload'})
    # assert r.status_code == 403
    #
    # r = as_admin.delete('/gears/' + gear)
    # assert r.status_code == 403


def test_gear_invocation_and_suggest(data_builder, file_form, as_admin):
    gear = data_builder.create_gear()
    group = data_builder.create_group(label='test-group')
    project = data_builder.create_project(label='test-project')
    session = data_builder.create_session(label='test-session', subject={'code': 'test-subject'})
    session2 = data_builder.create_session(label='test-session-2', subject={'code': 'test-subject-2'})
    subject = as_admin.get('/sessions/' + session).json()['subject']['_id']
    subject2 = as_admin.get('/sessions/' + session2).json()['subject']['_id']
    acquisition = data_builder.create_acquisition(label='test-acquisition')
    acquisition2 = data_builder.create_acquisition(label='test-acquisition', session=session2)
    acquisition3 = data_builder.create_acquisition(label='test-acquisition', session=session2)


    # Add collection with only the 3rd acquisition
    collection = as_admin.post('/collections', json={'label': 'test-collection'}).json()['_id']
    assert as_admin.put('/collections/' + collection, json={
        'contents': {
            'operation': 'add',
            'nodes': [
                {'level': 'acquisition', '_id': acquisition3}
            ],
        }
    }).ok


    # Add files to collection/project/sessions/acquisition
    as_admin.post('/collections/' + collection + '/files', files=file_form(
        'one.csv', meta={'name': 'one.csv'}))
    as_admin.post('/projects/' + project + '/files', files=file_form(
        'one.csv', meta={'name': 'one.csv'}))
    as_admin.post('/sessions/' + session + '/files', files=file_form(
        'one.csv', meta={'name': 'one.csv'}))
    as_admin.post('/sessions/' + session2 + '/files', files=file_form(
        'one.csv', meta={'name': 'one.csv'}))
    as_admin.post('/acquisitions/' + acquisition + '/files', files=file_form(
        'one.csv', meta={'name': 'one.csv'}))
    as_admin.post('/acquisitions/' + acquisition2 + '/files', files=file_form(
        'one.csv', meta={'name': 'one.csv'}))
    as_admin.post('/acquisitions/' + acquisition3 + '/files', files=file_form(
        'one.csv', meta={'name': 'one.csv'}))


    # Add analysis
    analysis = as_admin.post('/sessions/' + session + '/analyses', files=file_form(
        'one.csv', meta={'label': 'test', 'outputs': [{'name': 'one.csv'}]})).json()['_id']
    analysis2 = as_admin.post('/sessions/' + session2 + '/analyses', files=file_form(
        'one.csv', meta={'label': 'test', 'outputs': [{'name': 'one.csv'}]})).json()['_id']

    # test invocation
    r = as_admin.get('/gears/' + gear + '/invocation')
    assert r.ok


    # test suggest project
    r = as_admin.get('/gears/' + gear + '/suggest/project/' + project)
    assert r.ok

    assert len(r.json()['children']['subjects']) == 2
    assert len(r.json()['children']['analyses']) == 0
    assert len(r.json()['files']) == 1
    assert len(r.json()['parents']) == 1


    # test suggest subject
    r = as_admin.get('/gears/' + gear + '/suggest/subject/' + subject)
    assert r.ok

    assert len(r.json()['children']['sessions']) == 1
    assert len(r.json()['children']['analyses']) == 0
    assert len(r.json()['files']) == 0
    assert len(r.json()['parents']) == 2


    # test suggest session
    r = as_admin.get('/gears/' + gear + '/suggest/session/' + session)
    assert r.ok

    assert len(r.json()['children']['acquisitions']) == 1
    assert len(r.json()['children']['analyses']) == 1
    assert len(r.json()['files']) == 1
    assert len(r.json()['parents']) == 3


    # test suggest acquisition
    r = as_admin.get('/gears/' + gear + '/suggest/acquisition/' + acquisition)
    assert r.ok

    assert len(r.json()['children']['analyses']) == 0
    assert len(r.json()['files']) == 1
    assert len(r.json()['parents']) == 4


    # test suggest analysis
    r = as_admin.get('/gears/' + gear + '/suggest/analysis/' + analysis)
    assert r.ok

    assert len(r.json()['files']) == 1
    assert len(r.json()['parents']) == 4


    ### Test with collection context

    # test suggest project
    r = as_admin.get('/gears/' + gear + '/suggest/collection/' + collection, params={'collection': collection})
    assert r.ok

    assert len(r.json()['children']['subjects']) == 1
    assert len(r.json()['children']['analyses']) == 0
    assert len(r.json()['files']) == 1
    assert len(r.json()['parents']) == 0


    # test suggest subject
    r = as_admin.get('/gears/' + gear + '/suggest/subject/' + subject2, params={'collection': collection})
    assert r.ok

    assert len(r.json()['children']['sessions']) == 1
    assert len(r.json()['children']['analyses']) == 0
    assert len(r.json()['files']) == 0
    assert len(r.json()['parents']) == 1


    # test suggest session
    r = as_admin.get('/gears/' + gear + '/suggest/session/' + session2, params={'collection': collection})
    assert r.ok

    assert len(r.json()['children']['acquisitions']) == 1
    assert len(r.json()['children']['analyses']) == 1
    assert len(r.json()['files']) == 1
    assert len(r.json()['parents']) == 2


    # test suggest acquisition
    r = as_admin.get('/gears/' + gear + '/suggest/acquisition/' + acquisition3, params={'collection': collection})
    assert r.ok

    assert len(r.json()['children']['analyses']) == 0
    assert len(r.json()['files']) == 1
    assert len(r.json()['parents']) == 3


    # test suggest analysis
    r = as_admin.get('/gears/' + gear + '/suggest/analysis/' + analysis2, params={'collection': collection})
    assert r.ok

    assert len(r.json()['files']) == 1
    assert len(r.json()['parents']) == 3
