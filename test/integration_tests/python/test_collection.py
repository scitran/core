import json
import logging
import pytest

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_collections(api_as_user, single_project_session_acquisition_tree):
    data = single_project_session_acquisition_tree

    my_collection_id = create_collection(api_as_user)
    get_collection(api_as_user, my_collection_id)
    add_session_to_collection(api_as_user, data.sid, my_collection_id)

    r = api_as_user.get('/acquisitions/' + data.aid)
    collections = json.loads(r.content)['collections']
    assert my_collection_id in collections

    delete_collection(api_as_user, my_collection_id)

    r = api_as_user.get('/collections/' + my_collection_id)
    assert r.status_code == 404

    r = api_as_user.get('/acquisitions/' + data.aid)
    collections = json.loads(r.content)['collections']
    assert my_collection_id not in collections


# This fixture sets up a single project->session->acquisition hierarchy in the db
@pytest.fixture(scope="module")
def single_project_session_acquisition_tree(api_as_admin, request, bunch, data_builder):

    pid = data_builder.create_project('scitran')
    sid = data_builder.create_session(pid)
    aid = data_builder.create_acquisition(sid)

    def teardown_db():
        data_builder.delete_acquisition(aid)
        data_builder.delete_session(sid)
        data_builder.delete_project(pid)

    # Setup teardown handler
    request.addfinalizer(teardown_db)

    # This sets up a poor man's dot-notation dict
    fixture_data = bunch.create()
    fixture_data.sid = sid
    fixture_data.aid = aid
    return fixture_data


# Return collection id created
def create_collection(api):
    # POST - Create a collection
    NEW_COLLECTION_JSON = json.dumps({
        'label': 'SciTran/Testing',
        'public': True
    })
    r = api.post('/collections', data=NEW_COLLECTION_JSON)
    assert r.ok
    return json.loads(r.content)['_id']


def get_collection(api, collection_id):
    # GET - Retrieve a collection
    r = api.get('/collections/' + collection_id)
    assert r.ok


def add_session_to_collection(api, session_id, collection_id):
    # PUT - Add session to collection
    ADD_SESSION_TO_COLLECTION_JSON = json.dumps({
        'contents': {
            'nodes':
            [{
                'level': 'session',
                '_id': session_id,
            }],
            'operation': 'add'
        }
    })
    r = api.put('/collections/' + collection_id, data=ADD_SESSION_TO_COLLECTION_JSON)
    assert r.ok


def delete_collection(api, collection_id):
    # DELETE - Delete the collection
    r = api.delete('/collections/' + collection_id)
    assert r.ok
