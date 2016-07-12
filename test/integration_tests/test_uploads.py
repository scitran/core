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
def with_group_and_file_data(api_as_admin, data_builder, bunch, request):
    group_id = 'test_group_' + str(int(time.time() * 1000))
    data_builder.create_group(group_id)
    file_names = ['proj.csv', 'ses.csv', 'acq.csv']
    files = {}
    for i, name in enumerate(file_names):
        files['file' + str(i+1)] = (name, 'some,data,to,send\nanother,row,to,send\n')

    def teardown_db():
        r = api_as_admin.get('/groups/{}/projects'.format(group_id))
        content = json.loads(r.content)
        if content:
            project_id = content[0]['_id']
            r = api_as_admin.get('/projects/{}/sessions'.format(project_id))
            content = json.loads(r.content)
            if content:
                session_id = content[0]['_id']
                r = api_as_admin.get('/sessions/{}/acquisitions'.format(session_id))
                content = json.loads(r.content)
                if content:
                    acquisition_id = content[0]['_id']
                    api_as_admin.delete('/acquisitions/' + acquisition_id)
                api_as_admin.delete('/sessions/' + session_id)
            api_as_admin.delete('/projects/' + project_id)
        api_as_admin.delete('/groups/' + group_id)

    request.addfinalizer(teardown_db)

    fixture_data = bunch.create()
    fixture_data.group_id = group_id
    fixture_data.files = files
    return fixture_data

@pytest.fixture()
def with_hierarchy_and_file_data(api_as_admin, bunch, request, data_builder):
    group =         data_builder.create_group('test_upload_' + str(int(time.time() * 1000)))
    project =       data_builder.create_project(group)
    session =       data_builder.create_session(project)
    acquisition =   data_builder.create_acquisition(session)

    file_names = ['one.csv', 'two.csv']
    files = {}
    for i, name in enumerate(file_names):
        files['file' + str(i+1)] = (name, 'some,data,to,send\nanother,row,to,send\n')

    def teardown_db():
        data_builder.delete_acquisition(acquisition)
        data_builder.delete_session(session)
        data_builder.delete_project(project)
        data_builder.delete_group(group)

    request.addfinalizer(teardown_db)

    fixture_data = bunch.create()
    fixture_data.group = group
    fixture_data.project = project
    fixture_data.session = session
    fixture_data.acquisition = acquisition
    fixture_data.files = files
    return fixture_data


def test_uid_upload(with_group_and_file_data, api_as_admin):
    data = with_group_and_file_data
    metadata = {
        'group':{
            '_id': data.group_id
        },
        'project':{
            'label':'test_project',
            'files':[
                {
                    'name':data.files.keys()[0]
                }
            ]
        },
        'session':{
            'uid':'test_session_uid',
            'files':[
                {
                    'name':data.files.keys()[1]
                }
            ],
            'subject': {'code': 'test_subject'}
        },
        'acquisition':{
            'uid':'test_acquisition_uid',
            'files':[
                {
                    'name':data.files.keys()[2]
                }
            ]
        }
    }
    metadata = json.dumps(metadata)
    data.files['metadata'] = ('', metadata)

    r = api_as_admin.post('/upload/uid', files=data.files)
    assert r.ok


def test_label_upload(with_group_and_file_data, api_as_admin):
    data = with_group_and_file_data
    metadata = {
        'group':{
            '_id': data.group_id
        },
        'project':{
            'label':'test_project',
            'files':[
                {
                    'name':data.files.keys()[0]
                }
            ]
        },
        'session':{
            'label':'test_session',
            'files':[
                {
                    'name':data.files.keys()[1]
                }
            ],
            'subject': {'code': 'test_subject'}
        },
        'acquisition':{
            'label':'test_acquisition',
            'files':[
                {
                    'name':data.files.keys()[2]
                }
            ]
        }
    }
    metadata = json.dumps(metadata)
    data.files['metadata'] = ('', metadata)

    r = api_as_admin.post('/upload/label', files=data.files)
    assert r.ok

    data.files['metadata'] = metadata
    r = api_as_admin.post('/upload/label', files=data.files)
    assert r.status_code == 400

def find_file_in_array(filename, files):
    for f in files:
        if f.get('name') == filename:
            return f

def test_acquisition_engine_upload(with_hierarchy_and_file_data, api_as_admin):

    data = with_hierarchy_and_file_data
    metadata = {
        'project':{
            'label': 'engine project',
            'metadata': {'test': 'p'}
        },
        'session':{
            'label': 'engine session',
            'subject': {'code': 'engine subject'},
            'metadata': {'test': 's'}
        },
        'acquisition':{
            'label': 'engine acquisition',
            'timestamp': '2016-06-20T21:57:36+00:00',
            'metadata': {'test': 'a'},
            'files':[
                {
                    'name': 'one.csv',
                    'type': 'engine type 0',
                    'metadata': {'test': 'f0'}
                },
                {
                    'name': 'two.csv',
                    'type': 'engine type 1',
                    'metadata': {'test': 'f1'}
                }
            ]
        }
    }
    data.files['metadata'] = ('', json.dumps(metadata))

    r = api_as_admin.post('/engine?level=acquisition&id='+data.acquisition, files=data.files)
    assert r.ok

    r = api_as_admin.get('/projects/' + data.project)
    assert r.ok
    p = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert p['label'] != metadata['project']['label']
    assert cmp(p['metadata'], metadata['project']['metadata']) == 0

    r = api_as_admin.get('/sessions/' + data.session)
    assert r.ok
    s = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert s['label'] != metadata['session']['label']
    assert cmp(s['metadata'], metadata['session']['metadata']) == 0
    assert s['subject']['code'] == metadata['session']['subject']['code']

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok
    a = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert a['label'] != metadata['acquisition']['label']
    a_timestamp = dateutil.parser.parse(a['timestamp'])
    m_timestamp = dateutil.parser.parse(metadata['acquisition']['timestamp'])
    assert a_timestamp == m_timestamp
    assert cmp(a['metadata'], metadata['acquisition']['metadata']) == 0

    for f in a['files']:
        mf = find_file_in_array(f['name'], metadata['acquisition']['files'])
        assert mf is not None
        assert f['type'] == mf['type']
        assert cmp(f['metadata'], mf['metadata']) == 0

def test_session_engine_upload(with_hierarchy_and_file_data, api_as_admin):

    data = with_hierarchy_and_file_data
    metadata = {
        'project':{
            'label': 'engine project',
            'metadata': {'test': 'p'}
        },
        'session':{
            'label': 'engine session',
            'subject': {'code': 'engine subject'},
            'timestamp': '2016-06-20T21:57:36+00:00',
            'metadata': {'test': 's'},
            'files': [
                {
                    'name': 'one.csv',
                    'type': 'engine type 0',
                    'metadata': {'test': 'f0'}
                },
                {
                    'name': 'two.csv',
                    'type': 'engine type 1',
                    'metadata': {'test': 'f1'}
                }
            ]
        }
    }
    data.files['metadata'] = ('', json.dumps(metadata))

    r = api_as_admin.post('/engine?level=session&id='+data.session, files=data.files)
    assert r.ok

    r = api_as_admin.get('/projects/' + data.project)
    assert r.ok
    p = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert p['label'] != metadata['project']['label']
    assert cmp(p['metadata'], metadata['project']['metadata']) == 0

    r = api_as_admin.get('/sessions/' + data.session)
    assert r.ok
    s = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert s['label'] != metadata['session']['label']
    assert cmp(s['metadata'], metadata['session']['metadata']) == 0
    assert s['subject']['code'] == metadata['session']['subject']['code']
    s_timestamp = dateutil.parser.parse(s['timestamp'])
    m_timestamp = dateutil.parser.parse(metadata['session']['timestamp'])
    assert s_timestamp == m_timestamp

    for f in s['files']:
        mf = find_file_in_array(f['name'], metadata['session']['files'])
        assert mf is not None
        assert f['type'] == mf['type']
        assert cmp(f['metadata'], mf['metadata']) == 0

def test_project_engine_upload(with_hierarchy_and_file_data, api_as_admin):

    data = with_hierarchy_and_file_data
    metadata = {
        'project':{
            'label': 'engine project',
            'metadata': {'test': 'p'},
            'files': [
                {
                    'name': 'one.csv',
                    'type': 'engine type 0',
                    'metadata': {'test': 'f0'}
                },
                {
                    'name': 'two.csv',
                    'type': 'engine type 1',
                    'metadata': {'test': 'f1'}
                }
            ]
        }
    }
    data.files['metadata'] = ('', json.dumps(metadata))

    r = api_as_admin.post('/engine?level=project&id='+data.project, files=data.files)
    assert r.ok

    r = api_as_admin.get('/projects/' + data.project)
    assert r.ok
    p = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert p['label'] != metadata['project']['label']
    assert cmp(p['metadata'], metadata['project']['metadata']) == 0

    for f in p['files']:
        mf = find_file_in_array(f['name'], metadata['project']['files'])
        assert mf is not None
        assert f['type'] == mf['type']
        assert cmp(f['metadata'], mf['metadata']) == 0

def test_acquisition_file_only_engine_upload(with_hierarchy_and_file_data, api_as_admin):

    data = with_hierarchy_and_file_data

    r = api_as_admin.post('/engine?level=acquisition&id='+data.acquisition, files=data.files)
    assert r.ok

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok
    a = json.loads(r.content)

    for k,v in data.files.items():
        mf = find_file_in_array(v[0], a['files'])
        assert mf is not None

def test_acquisition_subsequent_file_engine_upload(with_hierarchy_and_file_data, api_as_admin):

    data = with_hierarchy_and_file_data

    filedata_1 = {}
    filedata_1['file1'] = ('file-one.csv', 'some,data,to,send\nanother,row,to,send\n')
    filedata_1['metadata'] = ('', json.dumps({
        'acquisition':{
            'files':[
                {
                    'name': 'file-one.csv',
                    'type': 'engine type 1',
                    'metadata': {'test': 'f1'}
                }
            ]
        }
    }))

    r = api_as_admin.post('/engine?level=acquisition&id='+data.acquisition, files=filedata_1)
    assert r.ok

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
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
                    'metadata': {'test': 'f1'}
                }
            ]
        }
    }))

    r = api_as_admin.post('/engine?level=acquisition&id='+data.acquisition, files=filedata_2)
    assert r.ok

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok
    a = json.loads(r.content)

    # Assert both files are still present after upload
    mf = find_file_in_array('file-one.csv', a['files'])
    assert mf is not None
    mf = find_file_in_array('file-two.csv', a['files'])
    assert mf is not None

def test_acquisition_metadata_only_engine_upload(with_hierarchy_and_file_data, api_as_admin):

    data = with_hierarchy_and_file_data
    metadata = {
        'project':{
            'label': 'engine project',
            'metadata': {'test': 'p'}
        },
        'session':{
            'label': 'engine session',
            'subject': {'code': 'engine subject'},
            'metadata': {'test': 's'}
        },
        'acquisition':{
            'label': 'engine acquisition',
            'timestamp': '2016-06-20T21:57:36+00:00',
            'metadata': {'test': 'a'}
        }
    }
    data.files = {}
    data.files['metadata'] = ('', json.dumps(metadata))

    r = api_as_admin.post('/engine?level=acquisition&id='+data.acquisition, files=data.files)
    assert r.ok

    r = api_as_admin.get('/projects/' + data.project)
    assert r.ok
    p = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert p['label'] != metadata['project']['label']
    assert cmp(p['metadata'], metadata['project']['metadata']) == 0

    r = api_as_admin.get('/sessions/' + data.session)
    assert r.ok
    s = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert s['label'] != metadata['session']['label']
    assert cmp(s['metadata'], metadata['session']['metadata']) == 0
    assert s['subject']['code'] == metadata['session']['subject']['code']

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok
    a = json.loads(r.content)
    # Engine metadata should not replace existing fields
    assert a['label'] != metadata['acquisition']['label']
    a_timestamp = dateutil.parser.parse(a['timestamp'])
    m_timestamp = dateutil.parser.parse(metadata['acquisition']['timestamp'])
    assert a_timestamp == m_timestamp
    assert cmp(a['metadata'], metadata['acquisition']['metadata']) == 0
