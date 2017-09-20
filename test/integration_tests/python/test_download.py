import cStringIO
import os
import tarfile
import zipfile


def test_download(data_builder, file_form, as_admin, api_db):
    project = data_builder.create_project(label='project1')
    session = data_builder.create_session(label='session1')
    session2 = data_builder.create_session(label='session1')
    acquisition = data_builder.create_acquisition(session=session)
    acquisition2 = data_builder.create_acquisition(session=session2)

    # upload the same file to each container created and use different tags to
    # facilitate download filter tests:
    # acquisition: [], session: ['plus'], project: ['plus', 'minus']
    file_name = 'test.csv'
    as_admin.post('/acquisitions/' + acquisition + '/files', files=file_form(
        file_name, meta={'name': file_name, 'type': 'csv'}))

    as_admin.post('/acquisitions/' + acquisition2 + '/files', files=file_form(
        file_name, meta={'name': file_name, 'type': 'csv'}))

    as_admin.post('/sessions/' + session + '/files', files=file_form(
        file_name, meta={'name': file_name, 'type': 'csv', 'tags': ['plus']}))

    as_admin.post('/projects/' + project + '/files', files=file_form(
        file_name, meta={'name': file_name, 'type': 'csv', 'tags': ['plus', 'minus']}))

    missing_object_id = '000000000000000000000000'

    # Try to download w/ nonexistent ticket
    r = as_admin.get('/download', params={'ticket': missing_object_id})
    assert r.status_code == 404

    # Retrieve a ticket for a batch download
    r = as_admin.post('/download', json={
        'optional': False,
        'filters': [{'tags': {
            '-': ['minus']
        }}],
        'nodes': [
            {'level': 'project', '_id': project},        
        ]
    })
    assert r.ok
    ticket = r.json()['ticket']

    # Perform the download
    r = as_admin.get('/download', params={'ticket': ticket})
    assert r.ok

    tar_file = cStringIO.StringIO(r.content)
    tar = tarfile.open(mode="r", fileobj=tar_file)

    # Verify a single file in tar with correct file name
    found_second_session = False 
    for tarinfo in tar:
        assert os.path.basename(tarinfo.name) == file_name
        if 'session1_0' in str(tarinfo.name):
            found_second_session = True
    assert found_second_session

    tar.close()

    # Download one session with many acquisitions and make sure they are in the same subject folder

    acquisition3 = data_builder.create_acquisition(session=session)
    r = as_admin.post('/acquisitions/' + acquisition3 + '/files', files=file_form(
        file_name, meta={'name': file_name, 'type': 'csv'}))
    assert r.ok

    r = as_admin.post('/download', json={
        'optional': False,
        'nodes': [
            {'level': 'acquisition', '_id': acquisition},
            {'level': 'acquisition', '_id': acquisition3},
        ]
    })
    assert r.ok
    ticket = r.json()['ticket']

    # Perform the download
    r = as_admin.get('/download', params={'ticket': ticket})
    assert r.ok

    tar_file = cStringIO.StringIO(r.content)
    tar = tarfile.open(mode="r", fileobj=tar_file)

    # Verify a single file in tar with correct file name
    found_second_session = False 
    for tarinfo in tar:
        assert os.path.basename(tarinfo.name) == file_name
        if 'session1_0' in str(tarinfo.name):
            found_second_session = True
    assert not found_second_session

    tar.close()


    # Try to perform the download from a different IP
    update_result = api_db.downloads.update_one(
        {'_id': ticket},
        {'$set': {'ip': '0.0.0.0'}})
    assert update_result.modified_count == 1

    r = as_admin.get('/download', params={'ticket': ticket})
    assert r.status_code == 400

    # Try to retrieve a ticket referencing nonexistent containers
    r = as_admin.post('/download', json={
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
    r = as_admin.post('/download', params={'bulk': 'true'}, json={
        'files': [{'container_name': 'subject', 'container_id': missing_object_id, 'filename': 'nosuch.csv'}]
    })
    assert r.status_code == 400

    # Try to retrieve ticket for bulk download referencing nonexistent file
    r = as_admin.post('/download', params={'bulk': 'true'}, json={
        'files': [{'container_name': 'project', 'container_id': project, 'filename': 'nosuch.csv'}]
    })
    assert r.status_code == 404

    # Retrieve ticket for bulk download
    r = as_admin.post('/download', params={'bulk': 'true'}, json={
        'files': [{'container_name': 'project', 'container_id': project, 'filename': file_name}]
    })
    assert r.ok
    ticket = r.json()['ticket']

    # Perform the download using symlinks
    r = as_admin.get('/download', params={'ticket': ticket, 'symlinks': 'true'})
    assert r.ok


def test_filelist_download(data_builder, file_form, as_admin):
    session = data_builder.create_session()
    zip_cont = cStringIO.StringIO()
    with zipfile.ZipFile(zip_cont, 'w') as zip_file:
        zip_file.writestr('two.csv', 'sample\ndata\n')
    zip_cont.seek(0)
    session_files = '/sessions/' + session + '/files'
    as_admin.post(session_files, files=file_form('one.csv'))
    as_admin.post(session_files, files=file_form(('two.zip', zip_cont)))

    # try to get non-existent file
    r = as_admin.get(session_files + '/non-existent.csv')
    assert r.status_code == 404

    # try to get file w/ non-matching hash
    r = as_admin.get(session_files + '/one.csv', params={'hash': 'match me if you can'})
    assert r.status_code == 409

    # get download ticket for single file
    r = as_admin.get(session_files + '/one.csv', params={'ticket': ''})
    assert r.ok
    ticket = r.json()['ticket']

    # download single file w/ ticket
    r = as_admin.get(session_files + '/one.csv', params={'ticket': ticket})
    assert r.ok

    # try to get zip info for non-zip file
    r = as_admin.get(session_files + '/one.csv', params={'ticket': ticket, 'info': 'true'})
    assert r.status_code == 400

    # try to get zip member of non-zip file
    r = as_admin.get(session_files + '/one.csv', params={'ticket': ticket, 'member': 'hardly'})
    assert r.status_code == 400

    # try to download a different file w/ ticket
    r = as_admin.get(session_files + '/two.zip', params={'ticket': ticket})
    assert r.status_code == 400

    # get download ticket for zip file
    r = as_admin.get(session_files + '/two.zip', params={'ticket': ''})
    assert r.ok
    ticket = r.json()['ticket']

    # get zip info
    r = as_admin.get(session_files + '/two.zip', params={'ticket': ticket, 'info': 'true'})
    assert r.ok

    # try to get non-existent zip member
    r = as_admin.get(session_files + '/two.zip', params={'ticket': ticket, 'member': 'hardly'})
    assert r.status_code == 400

    # get zip member
    r = as_admin.get(session_files + '/two.zip', params={'ticket': ticket, 'member': 'two.csv'})
    assert r.ok


def test_analysis_download(data_builder, file_form, as_admin):
    session = data_builder.create_session()
    zip_cont = cStringIO.StringIO()
    with zipfile.ZipFile(zip_cont, 'w') as zip_file:
        zip_file.writestr('two.csv', 'sample\ndata\n')
    zip_cont.seek(0)
    analysis = as_admin.post('/sessions/' + session + '/analyses', files=file_form(
        'one.csv', ('two.zip', zip_cont),
        meta={'label': 'test', 'inputs': [{'name': 'one.csv'}, {'name': 'two.csv'}]}
    )).json()['_id']
    analysis_files = '/sessions/' + session + '/analyses/' + analysis + '/files'
    new_analysis_files = '/analyses/' + analysis + '/files'


    # try to download analysis files w/ non-existent ticket
    r = as_admin.get(analysis_files, params={'ticket': '000000000000000000000000'})
    assert r.status_code == 404

    # get analysis batch download ticket for all files
    r = as_admin.get(analysis_files, params={'ticket': ''}, json={"optional":True,"nodes":[{"level":"analysis","_id":analysis}]})
    assert r.ok
    ticket = r.json()['ticket']

    # filename is analysis_<label> not analysis_<_id>
    assert r.json()['filename'] == 'analysis_test.tar'

    # batch download analysis files w/ ticket from wrong endpoint
    r = as_admin.get(analysis_files, params={'ticket': ticket})
    assert r.status_code == 400

    # batch download analysis files w/ ticket from correct endpoint
    r = as_admin.get('/download', params={'ticket': ticket})
    assert r.ok

    ### Using '/download' endpoint ###
    # try to download analysis files w/ non-existent ticket
    r = as_admin.get('/download', params={'ticket': '000000000000000000000000'})
    assert r.status_code == 404

    # get analysis batch download ticket for all files
    r = as_admin.get('/download', params={'ticket': ''}, json={"optional":True,"nodes":[{"level":"analysis","_id":analysis}]})
    assert r.ok
    ticket = r.json()['ticket']

    # filename is analysis_<label> not analysis_<_id>
    assert r.json()['filename'] == 'analysis_test.tar'

    # batch download analysis files w/ ticket
    r = as_admin.get('/download', params={'ticket': ticket})
    assert r.ok

    # try to get download ticket for non-existent analysis file
    r = as_admin.get(analysis_files + '/non-existent.csv')
    assert r.status_code == 404

    # get analysis download ticket for single file
    r = as_admin.get(analysis_files + '/one.csv', params={'ticket': ''})
    assert r.ok
    ticket = r.json()['ticket']

    # download single analysis file w/ ticket
    r = as_admin.get(analysis_files + '/one.csv', params={'ticket': ticket})
    assert r.ok

    # try to get zip info for non-zip file
    r = as_admin.get(analysis_files + '/one.csv', params={'ticket': ticket, 'info': 'true'})
    assert r.status_code == 400

    # try to get zip member of non-zip file
    r = as_admin.get(analysis_files + '/one.csv', params={'ticket': ticket, 'member': 'nosuch'})
    assert r.status_code == 400

    # try to download a different file w/ ticket
    r = as_admin.get(analysis_files + '/two.zip', params={'ticket': ticket})
    assert r.status_code == 400

    # get analysis download ticket for zip file
    r = as_admin.get(analysis_files + '/two.zip', params={'ticket': ''})
    assert r.ok
    ticket = r.json()['ticket']

    # get zip info
    r = as_admin.get(analysis_files + '/two.zip', params={'ticket': ticket, 'info': 'true'})
    assert r.ok

    # try to get non-existent zip member
    r = as_admin.get(analysis_files + '/two.zip', params={'ticket': ticket, 'member': 'nosuch'})
    assert r.status_code == 400

    # get zip member
    r = as_admin.get(analysis_files + '/two.zip', params={'ticket': ticket, 'member': 'two.csv'})
    assert r.ok

    ### single file analysis download using FileListHandler ###
    # try to get download ticket for non-existent analysis file
    r = as_admin.get(new_analysis_files + '/non-existent.csv')
    assert r.status_code == 404

    # get analysis download ticket for single file
    r = as_admin.get(new_analysis_files + '/one.csv', params={'ticket': ''})
    assert r.ok
    ticket = r.json()['ticket']

    # download single analysis file w/ ticket
    r = as_admin.get(new_analysis_files + '/one.csv', params={'ticket': ticket})
    assert r.ok

    # try to get zip info for non-zip file
    r = as_admin.get(new_analysis_files + '/one.csv', params={'ticket': ticket, 'info': 'true'})
    assert r.status_code == 400

    # try to get zip member of non-zip file
    r = as_admin.get(new_analysis_files + '/one.csv', params={'ticket': ticket, 'member': 'nosuch'})
    assert r.status_code == 400

    # try to download a different file w/ ticket
    r = as_admin.get(new_analysis_files + '/two.zip', params={'ticket': ticket})
    assert r.status_code == 400

    # get analysis download ticket for zip file
    r = as_admin.get(new_analysis_files + '/two.zip', params={'ticket': ''})
    assert r.ok
    ticket = r.json()['ticket']

    # get zip info
    r = as_admin.get(new_analysis_files + '/two.zip', params={'ticket': ticket, 'info': 'true'})
    assert r.ok

    # try to get non-existent zip member
    r = as_admin.get(new_analysis_files + '/two.zip', params={'ticket': ticket, 'member': 'nosuch'})
    assert r.status_code == 400

    # get zip member
    r = as_admin.get(new_analysis_files + '/two.zip', params={'ticket': ticket, 'member': 'two.csv'})
    assert r.ok

def test_filters(data_builder, file_form, as_admin):

    project = data_builder.create_project()
    session = data_builder.create_session()
    acquisition = data_builder.create_acquisition()
    acquisition2 = data_builder.create_acquisition()

    as_admin.post('/acquisitions/' + acquisition + '/files', files=file_form(
        "test.csv", meta={'name': "test.csv", 'type': 'csv', 'tags': ['red', 'blue']}))
    as_admin.post('/acquisitions/' + acquisition + '/files', files=file_form(
        'test.dicom', meta={'name': 'test.dicom', 'type': 'dicom', 'tags': ['red']}))
    as_admin.post('/acquisitions/' + acquisition2 + '/files', files=file_form(
        'test.nifti', meta={'name': 'test.nifti', 'type': 'nifti'}))
    r = as_admin.get('/acquisitions/' + acquisition)
    assert r.ok

    # Malformed filters
    r = as_admin.post('/download', json={
        'optional': False,
        'filters': [
            {'tags': 'red'}
        ],
        'nodes': [
            {'level': 'session', '_id': session},
        ]
    })
    assert r.status_code == 400

    # No filters
    r = as_admin.post('/download', json={
        'optional': False,
        'nodes': [
            {'level': 'session', '_id': session},
        ]
    })
    assert r.ok
    assert r.json()['file_cnt'] == 3

    # Filter by tags
    r = as_admin.post('/download', json={
        'optional': False,
        'filters': [
            {'tags': {'+':['red']}}
        ],
        'nodes': [
            {'level': 'session', '_id': session},
        ]
    })
    assert r.ok
    assert r.json()['file_cnt'] == 2

    # Filter by type
    r = as_admin.post('/download', json={
        'optional': False,
        'filters': [
            {'types': {'+':['nifti']}}
        ],
        'nodes': [
            {'level': 'session', '_id': session},
        ]
    })
    assert r.ok
    assert r.json()['file_cnt'] == 1

def test_summary(data_builder, as_admin, file_form):
    project = data_builder.create_project(label='project1')
    session = data_builder.create_session(label='session1')
    session2 = data_builder.create_session(label='session1')
    acquisition = data_builder.create_acquisition(session=session)
    acquisition2 = data_builder.create_acquisition(session=session2)

    # upload the same file to each container created and use different tags to
    # facilitate download filter tests:
    # acquisition: [], session: ['plus'], project: ['plus', 'minus']
    file_name = 'test.csv'
    as_admin.post('/acquisitions/' + acquisition + '/files', files=file_form(
        file_name, meta={'name': file_name, 'type': 'csv'}))

    as_admin.post('/acquisitions/' + acquisition2 + '/files', files=file_form(
        file_name, meta={'name': file_name, 'type': 'csv'}))

    as_admin.post('/sessions/' + session + '/files', files=file_form(
        file_name, meta={'name': file_name, 'type': 'csv', 'tags': ['plus']}))

    as_admin.post('/projects/' + project + '/files', files=file_form(
        file_name, meta={'name': file_name, 'type': 'csv', 'tags': ['plus', 'minus']}))

    missing_object_id = '000000000000000000000000'

    r = as_admin.post('/download/summary', json={"level":"projects", "_id":project})
    assert r.ok
    assert len(r.json()) == 1
    assert r.json().get("csv", {}).get("count",0) == 4 

    r = as_admin.post('/download/summary', json={"level":"sessions", "_id":session})
    assert r.ok
    assert len(r.json()) == 1
    assert r.json().get("csv", {}).get("count",0) == 2 
