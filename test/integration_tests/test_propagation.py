import json
import time
import logging
import pytest

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


@pytest.fixture(scope="module")
def with_hierarchy(api_as_admin, bunch, request, data_builder):
    group =         data_builder.create_group('test_prop_group')
    project =       data_builder.create_project(group)
    session =       data_builder.create_session(project)
    acquisition =   data_builder.create_acquisition(session)

    def teardown_db():
        data_builder.delete_group(group)
        data_builder.delete_project(project)
        data_builder.delete_session(session)
        data_builder.delete_acquisition(acquisition)

    request.addfinalizer(teardown_db)

    fixture_data = bunch.create()
    fixture_data.group = group
    fixture_data.project = project
    fixture_data.session = session
    fixture_data.acquisition = acquisition
    return fixture_data

# Test changing propagated properties
def test_archived_propagation_from_project(with_hierarchy, data_builder, api_as_admin):
    """
    Tests:
      - 'archived' is a propagated property
      - propagation works from a project level
      - setting a propagated property triggers propagation
      - set logic for setting 1 of the propagated properties
    """
    data = with_hierarchy

    payload = json.dumps({'archived': True})
    r = api_as_admin.put('/projects/' + data.project, data=payload)
    assert r.ok

    r = api_as_admin.get('/projects/' + data.project)
    assert r.ok and json.loads(r.content)['archived']

    r = api_as_admin.get('/sessions/' + data.session)
    assert r.ok and json.loads(r.content)['archived']

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok and json.loads(r.content)['archived']

def test_public_propagation_from_project(with_hierarchy, data_builder, api_as_admin):
    """
    Tests:
      - 'public' is a propagated property
    """
    data = with_hierarchy

    payload = json.dumps({'public': True})
    r = api_as_admin.put('/projects/' + data.project, data=payload)
    assert r.ok

    r = api_as_admin.get('/projects/' + data.project)
    assert r.ok and json.loads(r.content)['public']

    r = api_as_admin.get('/sessions/' + data.session)
    assert r.ok and json.loads(r.content)['public']

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok and json.loads(r.content)['public']

def test_public_and_archived_propagation_from_project(with_hierarchy, data_builder, api_as_admin):
    """
    Tests:
      - set logic for setting all of the propagated properties
    """
    data = with_hierarchy

    payload = json.dumps({'public': False, 'archived': False})
    r = api_as_admin.put('/projects/' + data.project, data=payload)
    assert r.ok

    r = api_as_admin.get('/projects/' + data.project)
    content = json.loads(r.content)
    assert r.ok and content['public'] == False and content['archived'] == False

    r = api_as_admin.get('/sessions/' + data.session)
    content = json.loads(r.content)
    assert r.ok and content['public'] == False and content['archived'] == False

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    content = json.loads(r.content)
    assert r.ok and content['public'] == False and content['archived'] == False

def test_public_propagation_from_session(with_hierarchy, data_builder, api_as_admin):
    """
    Tests:
      - propagation works from a session level
    """
    data = with_hierarchy

    payload = json.dumps({'public': True})
    r = api_as_admin.put('/sessions/' + data.session, data=payload)
    assert r.ok

    r = api_as_admin.get('/sessions/' + data.session)
    assert r.ok and json.loads(r.content)['public']

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok and json.loads(r.content)['public']

def test_set_public_acquisition(with_hierarchy, data_builder, api_as_admin):
    """
    Tests:
      - setting a propagated property on an acquisition does not attempt to propagate (would hit Exception)
    """
    data = with_hierarchy

    payload = json.dumps({'archived': True})
    r = api_as_admin.put('/acquisitions/' + data.acquisition, data=payload)
    assert r.ok


# Test propagation of project permission changes
def test_add_and_remove_user_for_project_permissions(with_hierarchy, data_builder, api_as_admin):
    """
    Tests:
      - changing permissions at a project level triggers propagation
      - additive change to list propagates properly
      - change to list propagates properly
      - removal from list propagates properly
    """
    def get_user_in_perms(perms, uid):
        for perm in perms:
            if perm['_id'] == uid:
                return perm
        return None

    data = with_hierarchy
    user_id = 'new@user.com'

    # Add user
    payload = json.dumps({'_id': user_id, 'access': 'admin', 'site': 'local'})
    r = api_as_admin.post('/projects/' + data.project + '/permissions', data=payload)
    assert r.ok

    r = api_as_admin.get('/projects/' + data.project)
    perms = json.loads(r.content)['permissions']
    assert r.ok and get_user_in_perms(perms, user_id)

    r = api_as_admin.get('/sessions/' + data.session)
    perms = json.loads(r.content)['permissions']
    assert r.ok and get_user_in_perms(perms, user_id)

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    perms = json.loads(r.content)['permissions']
    assert r.ok and get_user_in_perms(perms, user_id)

    # Modify user perms
    payload = json.dumps({'access': 'rw'})
    r = api_as_admin.put('/projects/' + data.project + '/permissions/' + user_id, data=payload)
    assert r.ok

    r = api_as_admin.get('/projects/' + data.project)
    perms = json.loads(r.content)['permissions']
    user = get_user_in_perms(perms, user_id)
    assert r.ok and user is not None and user['access'] == 'rw'

    r = api_as_admin.get('/sessions/' + data.session)
    perms = json.loads(r.content)['permissions']
    user = get_user_in_perms(perms, user_id)
    assert r.ok and user is not None and user['access'] == 'rw'

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    perms = json.loads(r.content)['permissions']
    user = get_user_in_perms(perms, user_id)
    assert r.ok and user is not None and user['access'] == 'rw'

    # Add user
    r = api_as_admin.delete('/projects/' + data.project + '/permissions/' + user_id, data=payload)
    assert r.ok

    r = api_as_admin.get('/projects/' + data.project)
    perms = json.loads(r.content)['permissions']
    assert r.ok and get_user_in_perms(perms, user_id) is None

    r = api_as_admin.get('/sessions/' + data.session)
    perms = json.loads(r.content)['permissions']
    assert r.ok and get_user_in_perms(perms, user_id) is None

    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    perms = json.loads(r.content)['permissions']
    assert r.ok and get_user_in_perms(perms, user_id) is None


# Test tag pool renaming and deletion
def test_add_and_remove_user_for_project_permissions(with_hierarchy, data_builder, api_as_admin):
    """
    Tests:
      - propagation from the group level
      - renaming tag at group level renames tags in hierarchy
      - deleting tag at group level renames tags in hierarchy
    """

    data = with_hierarchy
    tag = 'test tag'
    tag_renamed = 'test tag please ignore'


    # Add tag to hierarchy
    payload = json.dumps({'value': tag})
    r = api_as_admin.post('/groups/' + data.group + '/tags', data=payload)
    assert r.ok
    r = api_as_admin.post('/projects/' + data.project + '/tags', data=payload)
    assert r.ok
    r = api_as_admin.post('/sessions/' + data.session + '/tags', data=payload)
    assert r.ok
    r = api_as_admin.post('/acquisitions/' + data.acquisition + '/tags', data=payload)
    assert r.ok

    r = api_as_admin.get('/groups/' + data.group)
    assert r.ok and tag in json.loads(r.content)['tags']
    r = api_as_admin.get('/projects/' + data.project)
    assert r.ok and tag in json.loads(r.content)['tags']
    r = api_as_admin.get('/sessions/' + data.session)
    assert r.ok and tag in json.loads(r.content)['tags']
    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok and tag in json.loads(r.content)['tags']

    # Rename tag
    payload = json.dumps({'value': tag_renamed})
    r = api_as_admin.put('/groups/' + data.group + '/tags/' + tag, data=payload)
    assert r.ok

    r = api_as_admin.get('/groups/' + data.group)
    assert r.ok and tag_renamed in json.loads(r.content)['tags']
    r = api_as_admin.get('/projects/' + data.project)
    assert r.ok and tag_renamed in json.loads(r.content)['tags']
    r = api_as_admin.get('/sessions/' + data.session)
    assert r.ok and tag_renamed in json.loads(r.content)['tags']
    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok and tag_renamed in json.loads(r.content)['tags']

    # Delete tag
    r = api_as_admin.put('/groups/' + data.group + '/tags/' + tag_renamed, data=payload)
    assert r.ok

    r = api_as_admin.get('/groups/' + data.group)
    assert r.ok and tag_renamed not in json.loads(r.content)['tags']
    r = api_as_admin.get('/projects/' + data.project)
    assert r.ok and tag_renamed not in json.loads(r.content)['tags']
    r = api_as_admin.get('/sessions/' + data.session)
    assert r.ok and tag_renamed not in json.loads(r.content)['tags']
    r = api_as_admin.get('/acquisitions/' + data.acquisition)
    assert r.ok and tag_renamed not in json.loads(r.content)['tags']
