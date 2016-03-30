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


