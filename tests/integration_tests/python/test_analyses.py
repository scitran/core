import cStringIO
import os
import tarfile


def test_online_analysis(data_builder, as_admin, as_drone, file_form):
    gear = data_builder.create_gear(gear={'inputs': {'csv': {'base': 'file'}}})
    session = data_builder.create_session()
    acquisition = data_builder.create_acquisition()
    assert as_admin.post('/acquisitions/' + acquisition + '/files', files=file_form('input.csv')).ok

    # Create job-based analysis
    r = as_admin.post('/sessions/' + session + '/analyses', json={
        'label': 'online',
        'job': {'gear_id': gear,
                'inputs': {'csv': {'type': 'acquisition', 'id': acquisition, 'name': 'input.csv'}}}
    })
    assert r.ok
    analysis = r.json()['_id']

    # Verify job was created with it
    r = as_admin.get('/analyses/' + analysis)
    assert r.ok
    job = r.json().get('job')
    assert job

    check_files(as_admin, analysis, 'inputs', 'input.csv')

    # Try manual upload - not allowed for job-based analysis
    r = as_admin.post('/analyses/' + analysis + '/files', files=file_form('output.csv'))
    assert r.status_code == 400

    # Engine upload
    r = as_drone.post('/engine',
        params={'level': 'analysis', 'id': analysis, 'job': job},
        files=file_form('output.csv', meta={'type': 'tabular data'}))
    assert r.ok

    check_files(as_admin, analysis, 'files', 'output.csv')


def test_offline_analysis(data_builder, as_admin, file_form):
    session = data_builder.create_session()
    acquisition = data_builder.create_acquisition()
    assert as_admin.post('/acquisitions/' + acquisition + '/files', files=file_form('input.csv')).ok

    # Create ad-hoc analysis
    r = as_admin.post('/sessions/' + session + '/analyses', json={
        'label': 'offline',
        'inputs': [{'type': 'acquisition', 'id': acquisition, 'name': 'input.csv'}]
    })
    assert r.ok
    analysis = r.json()['_id']

    check_files(as_admin, analysis, 'inputs', 'input.csv')

    # Manual upload
    r = as_admin.post('/analyses/' + analysis + '/files', files=file_form('output.csv'))
    assert r.ok

    check_files(as_admin, analysis, 'files', 'output.csv')


def test_legacy_analysis(data_builder, as_admin, file_form):
    session = data_builder.create_session()

    r = as_admin.post('/sessions/' + session + '/analyses', files=file_form('input.csv', 'output.csv', meta={
        'label': 'legacy',
        'inputs': [{'name': 'input.csv'}],
        'outputs': [{'name': 'output.csv'}],
    }))
    assert r.ok
    analysis = r.json()['_id']

    check_files(as_admin, analysis, 'inputs', 'input.csv')
    check_files(as_admin, analysis, 'files', 'output.csv')


def check_files(as_admin, analysis_id, filegroup, *filenames):
    # Verify that filegroup has all files, inflated
    r = as_admin.get('/analyses/' + analysis_id)
    assert r.ok
    analysis = r.json()
    assert set(f['name'] for f in analysis.get(filegroup, [])) == set(filenames)
    assert all('size' in f for f in analysis.get(filegroup, []))

    # Verify that filegroup download works
    r = as_admin.get('/analyses/' + analysis_id + '/' + filegroup, params={'ticket': ''})
    assert r.ok
    ticket = r.json()['ticket']
    r = as_admin.get('/download', params={'ticket': ticket})
    assert r.ok
    dirname = 'input' if filegroup == 'inputs' else 'output'
    with tarfile.open(mode='r', fileobj=cStringIO.StringIO(r.content)) as tar:
        assert set(m.name for m in tar.getmembers()) == set('/'.join([analysis['label'], dirname, fn]) for fn in filenames)
