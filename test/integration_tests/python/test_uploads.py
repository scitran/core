import datetime
import dateutil.parser
import os
import json
import time
import pytest
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)

@pytest.fixture()
def with_group_and_file_data(as_admin, data_builder, bunch, request):
    group_id = 'test_group_' + str(int(time.time() * 1000))
    data_builder.create_group(group_id)
    files = {}
    for i, cont in enumerate(['project', 'subject', 'session', 'acquisition', 'unused']):
        files['file' + str(i+1)] = (cont + '.csv', 'some,data,to,send\nanother,row,to,send\n')
    files['metadata'] = {
        'group': {'_id': group_id},
        'project': {
            'label': 'test_project',
            'files': [{'name': 'project.csv'}]
        },
        'session': {
            'subject': {
                'code': 'test_subject_code',
                'files': [{'name': 'subject.csv'}]
            },
            'files': [{'name': 'session.csv'}]
        },
        'acquisition': {
            'files': [{'name': 'acquisition.csv'}]
        }
    }

    def teardown_db():
        r = as_admin.get('/groups/{}/projects'.format(group_id))
        content = json.loads(r.content)
        if content:
            project_id = content[0]['_id']
            r = as_admin.get('/projects/{}/sessions'.format(project_id))
            content = json.loads(r.content)
            if content:
                session_id = content[0]['_id']
                r = as_admin.get('/sessions/{}/acquisitions'.format(session_id))
                content = json.loads(r.content)
                if content:
                    acquisition_id = content[0]['_id']
                    as_admin.delete('/acquisitions/' + acquisition_id)
                as_admin.delete('/sessions/' + session_id)
            as_admin.delete('/projects/' + project_id)
        as_admin.delete('/groups/' + group_id)

    request.addfinalizer(teardown_db)

    fixture_data = bunch.create()
    fixture_data.files = files
    return fixture_data


def test_uid_upload(with_group_and_file_data, as_admin):
    data = with_group_and_file_data
    metadata = data.files['metadata']
    metadata['session']['uid'] = 'test_session_uid'
    metadata['acquisition']['uid'] = 'test_acquisition_uid'

    # try to uid-upload w/o metadata
    del data.files['metadata']
    r = as_admin.post('/upload/uid', files=data.files)
    assert r.status_code == 500

    # uid-upload files
    data.files['metadata'] = ('', json.dumps(metadata))
    r = as_admin.post('/upload/uid', files=data.files)
    assert r.ok


def test_label_upload(with_group_and_file_data, as_admin):
    data = with_group_and_file_data
    metadata = data.files['metadata']
    metadata['session']['label'] = 'test_session_label'
    metadata['acquisition']['label'] = 'test_acquisition_label'

    # label-upload files
    data.files['metadata'] = ('', json.dumps(metadata))
    r = as_admin.post('/upload/label', files=data.files)
    assert r.ok


def find_file_in_array(filename, files):
    for f in files:
        if f.get('name') == filename:
            return f

def test_acquisition_engine_upload(with_hierarchy_and_file_data, as_admin):

    data = with_hierarchy_and_file_data
    metadata = {
        'project':{
            'label': 'engine project',
            'info': {'test': 'p'}
        },
        'session':{
            'label': 'engine session',
            'subject': {'code': 'engine subject'},
            'info': {'test': 's'}
        },
        'acquisition':{
            'label': 'engine acquisition',
            'timestamp': '2016-06-20T21:57:36+00:00',
            'info': {'test': 'a'},
            'files':[
                {
                    'name': 'one.csv',
                    'type': 'engine type 0',
                    'info': {'test': 'f0'}
                },
                {
                    'name': 'two.csv',
                    'type': 'engine type 1',
                    'info': {'test': 'f1'}
                }
            ]
        }
    }
    data.files['metadata'] = ('', json.dumps(metadata))

    r = as_admin.post('/engine?level=acquisition&id='+data.acquisition, files=data.files)
    assert r.ok

    r = as_admin.get('/projects/' + data.project)
    assert r.ok
    p = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert p['label'] != metadata['project']['label']
    assert cmp(p['info'], metadata['project']['info']) == 0

    r = as_admin.get('/sessions/' + data.session)
    assert r.ok
    s = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert s['label'] != metadata['session']['label']
    assert cmp(s['info'], metadata['session']['info']) == 0
    assert s['subject']['code'] == metadata['session']['subject']['code']

    r = as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok
    a = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert a['label'] != metadata['acquisition']['label']
    a_timestamp = dateutil.parser.parse(a['timestamp'])
    m_timestamp = dateutil.parser.parse(metadata['acquisition']['timestamp'])
    assert a_timestamp == m_timestamp
    assert cmp(a['info'], metadata['acquisition']['info']) == 0

    for f in a['files']:
        mf = find_file_in_array(f['name'], metadata['acquisition']['files'])
        assert mf is not None
        assert f['type'] == mf['type']
        assert cmp(f['info'], mf['info']) == 0

def test_session_engine_upload(with_hierarchy_and_file_data, as_admin):

    data = with_hierarchy_and_file_data
    metadata = {
        'project':{
            'label': 'engine project',
            'info': {'test': 'p'}
        },
        'session':{
            'label': 'engine session',
            'subject': {'code': 'engine subject'},
            'timestamp': '2016-06-20T21:57:36+00:00',
            'info': {'test': 's'},
            'files': [
                {
                    'name': 'one.csv',
                    'type': 'engine type 0',
                    'info': {'test': 'f0'}
                },
                {
                    'name': 'two.csv',
                    'type': 'engine type 1',
                    'info': {'test': 'f1'}
                }
            ]
        }
    }
    data.files['metadata'] = ('', json.dumps(metadata))

    r = as_admin.post('/engine?level=session&id='+data.session, files=data.files)
    assert r.ok

    r = as_admin.get('/projects/' + data.project)
    assert r.ok
    p = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert p['label'] != metadata['project']['label']
    assert cmp(p['info'], metadata['project']['info']) == 0

    r = as_admin.get('/sessions/' + data.session)
    assert r.ok
    s = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert s['label'] != metadata['session']['label']
    assert cmp(s['info'], metadata['session']['info']) == 0

    assert s['subject']['code'] == metadata['session']['subject']['code']
    s_timestamp = dateutil.parser.parse(s['timestamp'])
    m_timestamp = dateutil.parser.parse(metadata['session']['timestamp'])
    assert s_timestamp == m_timestamp

    for f in s['files']:
        mf = find_file_in_array(f['name'], metadata['session']['files'])
        assert mf is not None
        assert f['type'] == mf['type']
        assert cmp(f['info'], mf['info']) == 0

def test_project_engine_upload(with_hierarchy_and_file_data, as_admin):

    data = with_hierarchy_and_file_data
    metadata = {
        'project':{
            'label': 'engine project',
            'info': {'test': 'p'},
            'files': [
                {
                    'name': 'one.csv',
                    'type': 'engine type 0',
                    'info': {'test': 'f0'}
                },
                {
                    'name': 'two.csv',
                    'type': 'engine type 1',
                    'info': {'test': 'f1'}
                }
            ]
        }
    }
    data.files['metadata'] = ('', json.dumps(metadata))

    r = as_admin.post('/engine?level=project&id='+data.project, files=data.files)
    assert r.ok

    r = as_admin.get('/projects/' + data.project)
    assert r.ok
    p = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert p['label'] != metadata['project']['label']
    assert cmp(p['info'], metadata['project']['info']) == 0

    for f in p['files']:
        mf = find_file_in_array(f['name'], metadata['project']['files'])
        assert mf is not None
        assert f['type'] == mf['type']
        assert cmp(f['info'], mf['info']) == 0

def test_acquisition_file_only_engine_upload(with_hierarchy_and_file_data, as_admin):

    data = with_hierarchy_and_file_data

    r = as_admin.post('/engine?level=acquisition&id='+data.acquisition, files=data.files)
    assert r.ok

    r = as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok
    a = json.loads(r.content)

    for k,v in data.files.items():
        mf = find_file_in_array(v[0], a['files'])
        assert mf is not None

def test_acquisition_subsequent_file_engine_upload(with_hierarchy_and_file_data, as_admin):

    data = with_hierarchy_and_file_data

    filedata_1 = {}
    filedata_1['file1'] = ('file-one.csv', 'some,data,to,send\nanother,row,to,send\n')
    filedata_1['metadata'] = ('', json.dumps({
        'acquisition':{
            'files':[
                {
                    'name': 'file-one.csv',
                    'type': 'engine type 1',
                    'info': {'test': 'f1'}
                }
            ]
        }
    }))

    r = as_admin.post('/engine?level=acquisition&id='+data.acquisition, files=filedata_1)
    assert r.ok

    r = as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok
    a = json.loads(r.content)

    mf = find_file_in_array('file-one.csv', a['files'])
    assert mf is not None

    filedata_2 = {}
    filedata_2['file1'] = ('file-two.csv', 'some,data,to,send\nanother,row,to,send\n')
    filedata_2['metadata'] = ('', json.dumps({
        'acquisition':{
            'files':[
                {
                    'name': 'file-two.csv',
                    'type': 'engine type 1',
                    'info': {'test': 'f1'}
                }
            ]
        }
    }))

    r = as_admin.post('/engine?level=acquisition&id='+data.acquisition, files=filedata_2)
    assert r.ok

    r = as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok
    a = json.loads(r.content)

    # Assert both files are still present after upload
    mf = find_file_in_array('file-one.csv', a['files'])
    assert mf is not None
    mf = find_file_in_array('file-two.csv', a['files'])
    assert mf is not None

def test_acquisition_metadata_only_engine_upload(with_hierarchy_and_file_data, as_admin):

    data = with_hierarchy_and_file_data
    metadata = {
        'project':{
            'label': 'engine project',
            'info': {'test': 'p'}
        },
        'session':{
            'label': 'engine session',
            'subject': {'code': 'engine subject'},
            'info': {'test': 's'}
        },
        'acquisition':{
            'label': 'engine acquisition',
            'timestamp': '2016-06-20T21:57:36+00:00',
            'info': {'test': 'a'}
        }
    }
    data.files = {}
    data.files['metadata'] = ('', json.dumps(metadata))

    r = as_admin.post('/engine?level=acquisition&id='+data.acquisition, files=data.files)
    assert r.ok

    r = as_admin.get('/projects/' + data.project)
    assert r.ok
    p = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert p['label'] != metadata['project']['label']
    assert cmp(p['info'], metadata['project']['info']) == 0

    r = as_admin.get('/sessions/' + data.session)
    assert r.ok
    s = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert s['label'] != metadata['session']['label']
    assert cmp(s['info'], metadata['session']['info']) == 0
    assert s['subject']['code'] == metadata['session']['subject']['code']

    r = as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok
    a = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert a['label'] != metadata['acquisition']['label']
    a_timestamp = dateutil.parser.parse(a['timestamp'])
    m_timestamp = dateutil.parser.parse(metadata['acquisition']['timestamp'])
    assert a_timestamp == m_timestamp
    assert cmp(a['info'], metadata['acquisition']['info']) == 0


def test_analysis_upload(with_gear, with_hierarchy, as_user):
    gear = with_gear
    data = with_hierarchy
    file_data = {
        'file': ('test-1.dcm', open('test/integration_tests/python/test_files/test-1.dcm', 'rb').read()),
        'metadata': ('', json.dumps({
            'label': 'test analysis',
            'inputs': [ { 'name': 'test-1.dcm' } ]
        }))
    }

    # create session analysis
    r = as_user.post('/sessions/' + data.session + '/analyses', files=file_data)
    assert r.ok
    session_analysis_upload = r.json()['_id']

    # delete session analysis
    r = as_user.delete('/sessions/' + data.session + '/analyses/' + session_analysis_upload)
    assert r.ok

    # create acquisition analysis
    r = as_user.post('/acquisitions/' + data.acquisition + '/analyses', files=file_data)
    assert r.ok
    acquisition_analysis_upload = r.json()['_id']

    # delete acquisition analysis
    r = as_user.delete('/acquisitions/' + data.acquisition + '/analyses/' + acquisition_analysis_upload)
    assert r.ok

    # create acquisition file (for the fixture acquisition)
    r = as_user.post('/acquisitions/' + data.acquisition + '/files', files={
        'file': file_data['file']
    })
    assert r.ok

    # try to create analysis+job w/ missing analysis/job info
    r = as_user.post('/sessions/' + data.session + '/analyses', params={'job': 'true'}, json={})
    assert r.status_code == 400

    # create session analysis (job) using acquisition's file as input
    r = as_user.post('/sessions/' + data.session + '/analyses', params={'job': 'true'}, json={
        'analysis': { 'label': 'test analysis job' },
        'job': {
            'gear_id': gear,
            'inputs': {
                'dicom': {
                    'type': 'acquisition',
                    'id': data.acquisition,
                    'name': 'test-1.dcm'
                }
            },
            'tags': ['example']
        }
    })
    assert r.ok
    session_analysis_job = r.json()['_id']

    # delete session analysis (job)
    r = as_user.delete('/sessions/' + data.session + '/analyses/' + session_analysis_job)
    assert r.ok


def test_analysis_engine_upload(with_hierarchy_and_file_data, as_admin):
    data = with_hierarchy_and_file_data
    data.files['metadata'] = ('', json.dumps({
        'label': 'test analysis',
        'inputs': [{'name': 'one.csv'}, {'name': 'two.csv'}]
    }))

    # create acquisition analysis
    r = as_admin.post('/acquisitions/' + data.acquisition + '/analyses', files=data.files)
    assert r.ok
    acquisition_analysis_upload = r.json()['_id']

    r = as_admin.post('/engine?level=analysis&id=' + data.acquisition, files={
        'file': ('engine-analysis.txt', 'test analysis output content\n'),
        'metadata': ('', json.dumps({
            'value': {'label': 'test'},
            'type': 'text',
            'enabled': True
        }))
    })
    assert r.ok

    # delete acquisition analysis
    r = as_admin.delete('/acquisitions/' + data.acquisition + '/analyses/' + acquisition_analysis_upload)
    assert r.ok


def test_packfile(with_hierarchy_and_file_data, as_user):
    data = with_hierarchy_and_file_data

    # try to start packfile-upload to non-project target
    r = as_user.post('/sessions/' + data.session + '/packfile-start')
    assert r.status_code == 500

    # try to start packfile-upload to non-existent project (using session id)
    r = as_user.post('/projects/' + data.session + '/packfile-start')
    assert r.status_code == 500

    # start packfile-upload
    r = as_user.post('/projects/' + data.project + '/packfile-start')
    assert r.ok
    token = r.json()['token']

    # try to upload to packfile w/o token
    r = as_user.post('/projects/' + data.project + '/packfile')
    assert r.status_code == 500

    # upload to packfile
    r = as_user.post('/projects/' + data.project + '/packfile', params={'token': token}, files=data.files)
    assert r.ok

    metadata = {
        'project': {'_id': data.project},
        'session': {'label': 'test-packfile-label'},
        'acquisition': {
            'label': 'test-packfile-label',
            'timestamp': '1979-01-01T00:00:00+00:00'
        },
        'packfile': {'type': 'test'}
    }

    # try to finish packfile-upload w/o token
    r = as_user.post('/projects/' + data.project + '/packfile-end', params={'metadata': json.dumps(metadata)})
    assert r.status_code == 500

    # try to finish packfile-upload with files in the request
    r = as_user.post('/projects/' + data.project + '/packfile-end',
        params={'token': token, 'metadata': json.dumps(metadata)},
        files={'file': ('packfile-end.txt', 'sending files to packfile-end is not allowed\n')}
    )
    assert r.status_code == 500

    # finish packfile-upload (creates new session/acquisition)
    r = as_user.post('/projects/' + data.project + '/packfile-end', params={
        'token': token,
        'metadata': json.dumps(metadata)
    })
    assert r.ok

    # clean up added session/acquisition
    event_data_start_str = 'event: result\ndata: '
    event_data_start_pos = r.text.find(event_data_start_str)
    event_data = json.loads(r.text[event_data_start_pos + len(event_data_start_str):])
    r = as_user.delete('/acquisitions/' + event_data['acquisition_id'])
    assert r.ok
    r = as_user.delete('/sessions/' + event_data['session_id'])
    assert r.ok
