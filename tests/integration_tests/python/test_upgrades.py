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
    api_db.sessions.update_one({'_id': bson.ObjectId(session)}, {'$set': {'archived': True}})

    # Verfiy archived session is not hidden anymore
    assert session in [s['_id'] for s in as_admin.get('/sessions').json()]

    # Verify upgrade creates new-style hidden tag
    database.upgrade_to_42()
    session_data = as_admin.get('/sessions/' + session).json()
    assert 'archived' not in session_data
    assert 'hidden' in session_data['tags']
