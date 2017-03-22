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
        'modality': 'MRI'
    }

    # upload the same file to each container created and use different tags to
    # facilitate download filter tests:
    # acquisition: [], session: ['plus'], project: ['plus', 'minus']
    files['metadata'] = ('', json.dumps(metadata))
    api_as_admin.post('/acquisitions/' + acquisition_id + '/files', files=files)
    files['metadata'] = ('', json.dumps(dict(tags=['plus'], **metadata)))
    api_as_admin.post('/sessions/' + session_id + '/files', files=files)
    files['metadata'] = ('', json.dumps(dict(tags=['plus', 'minus'], **metadata)))
    api_as_admin.post('/projects/' + project_id + '/files', files=files)

    def teardown_download():
        api_as_admin.delete('/acquisitions/' + acquisition_id)
        api_as_admin.delete('/sessions/' + session_id)
        api_as_admin.delete('/projects/' + project_id)
        api_as_admin.delete('/groups/' + group_id)

    request.addfinalizer(teardown_download)

    fixture_data = bunch.create()
    fixture_data.project_id = project_id
    fixture_data.session_id = session_id
    fixture_data.acquisition_id = acquisition_id
    fixture_data.file_name = file_name
    return fixture_data


def test_download(with_a_download_available, api_as_user, db):
    data = with_a_download_available
    missing_object_id = '000000000000000000000000'

    # Try to download w/ nonexistent ticket
    r = api_as_user.get('/download', params={'ticket': missing_object_id})
    assert r.status_code == 404

    # Retrieve a ticket for a batch download
    r = api_as_user.post('/download', json={
        'optional': False,
        'filters': [{'tags': {
            '-': ['minus'],
            '+': ['plus']
        }}],
        'nodes': [
            {'level': 'project', '_id': data.project_id},
            {'level': 'session', '_id': data.session_id},
            {'level': 'acquisition', '_id': data.acquisition_id},
        ]
    })
    assert r.ok
    ticket = r.json()['ticket']

    # Perform the download
    r = api_as_user.get('/download', params={'ticket': ticket})
    assert r.ok

    tar_file = cStringIO.StringIO(r.content)
    tar = tarfile.open(mode="r", fileobj=tar_file)

    # Verify a single file in tar with correct file name
    for tarinfo in tar:
        assert os.path.basename(tarinfo.name) == data.file_name
    tar.close()

    # Try to perform the download from a different IP
    update_result = db.downloads.update_one(
        {'_id': ticket},
        {'$set': {'ip': '0.0.0.0'}})
    assert update_result.modified_count == 1

    r = api_as_user.get('/download', params={'ticket': ticket})
    assert r.status_code == 400

    # Try to retrieve a ticket referencing nonexistent containers
    r = api_as_user.post('/download', json={
        'optional': False,
        'nodes': [
            {'level': 'project', '_id': missing_object_id},
            {'level': 'session', '_id': missing_object_id},
            {'level': 'acquisition', '_id': missing_object_id},
        ]
    })
    assert r.status_code == 404

    # Try to retrieve ticket for bulk download w/ invalid container name
    # (not project|session|acquisition)
    r = api_as_user.post('/download', params={'bulk': 'true'}, json={
        'files': [{'container_name': 'subject', 'container_id': missing_object_id, 'filename': 'nosuch.csv'}]
    })
    assert r.status_code == 400

    # Try to retrieve ticket for bulk download referencing nonexistent file
    r = api_as_user.post('/download', params={'bulk': 'true'}, json={
        'files': [{'container_name': 'project', 'container_id': data.project_id, 'filename': 'nosuch.csv'}]
    })
    assert r.status_code == 404

    # Retrieve ticket for bulk download
    r = api_as_user.post('/download', params={'bulk': 'true'}, json={
        'files': [{'container_name': 'project', 'container_id': data.project_id, 'filename': data.file_name}]
    })
    assert r.ok
    ticket = r.json()['ticket']

    # Perform the download using symlinks
    r = api_as_user.get('/download', params={'ticket': ticket, 'symlinks': 'true'})
    assert r.ok
