import json
import time
import logging
import pytest

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


@pytest.fixture(scope="module")
def with_two_groups(api_as_admin, bunch, request, data_builder):
    group_1 = data_builder.create_group('test_group_' + str(int(time.time() * 1000)))
    group_2 = data_builder.create_group('test_group_' + str(int(time.time() * 1000)))

    def teardown_db():
        data_builder.delete_group(group_1)
        data_builder.delete_group(group_2)

    request.addfinalizer(teardown_db)

    fixture_data = bunch.create()
    fixture_data.group_1 = group_1
    fixture_data.group_2 = group_2
    return fixture_data


# Create project
# Add it to a group
# Switch the project to the second group
# Delete the project
def test_switching_project_between_groups(with_two_groups, data_builder, api_as_user):
    data = with_two_groups

    pid = data_builder.create_project(data.group_1)
    assert api_as_user.get('/projects/' + pid).ok
    r = api_as_user.get('/groups/' + data.group_1 + '/projects')
    print json.loads(r.content)

    payload = json.dumps({'group': with_two_groups.group_2})
    r = api_as_user.put('/projects/' + pid, data=payload)
    assert r.ok

    r = api_as_user.get('/projects/' + pid)
    assert r.ok and json.loads(r.content)['group'] == data.group_2

    data_builder.delete_project(pid)


def test_switching_session_between_projects(with_two_groups, data_builder, api_as_user):
    data = with_two_groups

    project_1_id = data_builder.create_project(data.group_1)
    project_2_id = data_builder.create_project(data.group_1)
    session_id = data_builder.create_session(project_1_id)

    payload = json.dumps({'project': project_2_id})
    r = api_as_user.put('/sessions/' + session_id, data=payload)
    assert r.ok

    r = api_as_user.get('/sessions/' + session_id)
    assert r.ok and json.loads(r.content)['project'] == project_2_id

    data_builder.delete_session(session_id)
    data_builder.delete_project(project_1_id)
    data_builder.delete_project(project_2_id)


def test_switching_acquisition_between_projects(with_two_groups, data_builder, api_as_user):
    data = with_two_groups

    project_id = data_builder.create_project(data.group_1)
    session_1_id = data_builder.create_session(project_id)
    session_2_id = data_builder.create_session(project_id)
    acquisition_id = data_builder.create_acquisition(session_1_id)

    payload = json.dumps({'session': session_2_id})
    r = api_as_user.put('/acquisitions/' + acquisition_id, data=payload)
    assert r.ok

    r = api_as_user.get('/acquisitions/' + acquisition_id)
    assert r.ok and json.loads(r.content)['session'] == session_2_id

    data_builder.delete_acquisition(acquisition_id)
    data_builder.delete_session(session_1_id)
    data_builder.delete_session(session_2_id)
    data_builder.delete_project(project_id)
