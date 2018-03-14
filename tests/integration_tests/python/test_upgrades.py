import os
import sys

import bson
import pytest


@pytest.fixture(scope='function')
def database(mocker):
    bin_path = os.path.join(os.getcwd(), 'bin')
    mocker.patch('sys.path', [bin_path] + sys.path)
    import database
    return database


def test_42(data_builder, api_db, as_admin, database):
    # Mimic old-style archived flag
    session = data_builder.create_session()
    session2 = data_builder.create_session()
    api_db.sessions.update_one({'_id': bson.ObjectId(session)}, {'$set': {'archived': True}})
    api_db.sessions.update_one({'_id': bson.ObjectId(session2)}, {'$set': {'archived': False}})

    # Verfiy archived session is not hidden anymore
    assert session  in [s['_id'] for s in as_admin.get('/sessions').json()]

    # Verify upgrade creates new-style hidden tag
    database.upgrade_to_42()
    session_data = as_admin.get('/sessions/' + session).json()
    assert 'archived' not in session_data
    assert 'hidden' in session_data['tags']

    # Verify archived was removed when false as well
    session_data = as_admin.get('/sessions/' + session2).json()
    assert 'archived' not in session_data


def test_43(data_builder, api_db, as_admin, file_form, database):
    # Create session and upload file for later use as analysis input
    session = data_builder.create_session()
    r = as_admin.post('/sessions/' + session + '/files', files=file_form('input.txt'))
    assert r.ok

    # Create ad-hoc analysis with input ref, then upload output
    r = as_admin.post('/sessions/' + session + '/analyses', json={
        'label': 'offline',
        'inputs': [{'type': 'session', 'id': session, 'name': 'input.txt'}]
    })
    assert r.ok
    analysis_id = r.json()['_id']
    r = as_admin.post('/analyses/' + analysis_id + '/files', files=file_form('output.txt', meta=[{'name': 'output.txt'}]))
    assert r.ok

    # Mimic old-style analysis input/output tags
    analysis = api_db.analyses.find_one({'_id': bson.ObjectId(analysis_id)}, ['inputs', 'files'])
    for f in analysis['inputs']:
        f['input'] = True
    for f in analysis['files']:
        f['output'] = True
    api_db.analyses.update_one({'_id': bson.ObjectId(analysis_id)},
                               {'$set': {'files': analysis['inputs'] + analysis['files']},
                                '$unset': {'inputs': ''}})

    # Verify upgrade gets rid of tags and separates inputs/files
    database.upgrade_to_43()
    analysis = as_admin.get('/analyses/' + analysis_id).json()
    assert 'inputs' in analysis
    assert len(analysis['inputs']) == 1
    assert 'input' not in analysis['inputs'][0]

    assert 'files' in analysis
    assert len(analysis['files']) == 1
    assert 'output' not in analysis['files'][0]
