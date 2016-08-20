import os
import json
import time
import pytest
import logging
import tarfile
import cStringIO

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


@pytest.fixture()
def with_a_download_available(api_as_admin, data_builder, bunch, request):
    file_name = "test.csv"
    group_id = 'test_group_' + str(int(time.time() * 1000))

    data_builder.create_group(group_id)
    project_id = data_builder.create_project(group_id)
    session_id = data_builder.create_session(project_id)
    acquisition_id = data_builder.create_acquisition(session_id)

    # multiform fields for the file upload
    files = {'file1': (file_name, 'some,data,to,send\nanother,row,to,send\n')}
    metadata = {
        'name': file_name,
        'type': 'csv',
        'instrument': 'MRI'
    }
    metadata = json.dumps(metadata)
    files['metadata'] = ('', metadata)

    # upload the same file to each container created in the test
    api_as_admin.post('/acquisitions/' + acquisition_id + '/files', files=files)
    api_as_admin.post('/sessions/' + session_id + '/files', files=files)
    api_as_admin.post('/projects/' + project_id + '/files', files=files)

    def teardown_download():
        api_as_admin.delete('/acquisitions/' + acquisition_id)
        api_as_admin.delete('/sessions/' + session_id)
        api_as_admin.delete('/projects/' + project_id)
        api_as_admin.delete('/groups/' + group_id)

    request.addfinalizer(teardown_download)

    fixture_data = bunch.create()
    fixture_data.project_id = project_id
    fixture_data.file_name = file_name
    return fixture_data


def test_download(with_a_download_available, api_as_admin):
    data = with_a_download_available

    # Retrieve a ticket for a batch download
    payload = json.dumps({
        'optional': False,
        'nodes': [
            {
                'level': 'project',
                '_id': data.project_id
            }
        ]
    })
    r = api_as_admin.post('/download', data=payload)
    assert r.ok

    # Perform the download
    ticket = json.loads(r.content)['ticket']
    r = api_as_admin.get('/download', params={'ticket': ticket})
    assert r.ok

    tar_file = cStringIO.StringIO(r.content)
    tar = tarfile.open(mode="r", fileobj=tar_file)

    # Verify a single file in tar with correct file name
    for tarinfo in tar:
        assert os.path.basename(tarinfo.name) == data.file_name
    tar.close()
