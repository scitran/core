# Various wholesale app states that might be useful in your tests

import time
import pytest


@pytest.fixture
def with_hierarchy(api_as_admin, bunch, request, data_builder):
    group =         data_builder.create_group('test_prop_' + str(int(time.time() * 1000)))
    project =       data_builder.create_project(group)
    session =       data_builder.create_session(project)
    acquisition =   data_builder.create_acquisition(session)

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
    return fixture_data


# NOTE removing scope='module' form with_hierarchy_and_file_data fixed the need
# for keeping the duplicate in test_uploads. Now the fixture runs per test,
# eliminating any data dependency between tests within a module at the price
# of more load/teardown requests.

@pytest.fixture
def with_hierarchy_and_file_data(with_hierarchy):
    file_names = ['one.csv', 'two.csv']
    files = {}
    for i, name in enumerate(file_names):
        files['file' + str(i+1)] = (name, 'some,data,to,send\nanother,row,to,send\n')

    fixture_data = with_hierarchy
    fixture_data.files = files
    return fixture_data


@pytest.fixture
def with_gear(request, as_admin):
    gear_name = 'test-gear'
    r = as_admin.post('/gears/' + gear_name, json={
        'category': 'converter',
        'gear': {
            'inputs': {
                'wat': {
                    'base': 'file',
                    'type': { 'enum': [ 'wat' ] }
                }
            },
            'maintainer': 'Example',
            'description': 'Example',
            'license': 'BSD-2-Clause',
            'author': 'Example',
            'url': 'https://example.example',
            'label': 'wat',
            'flywheel': '0',
            'source': 'https://example.example',
            'version': '0.0.1',
            'config': {},
            'name': gear_name
        },
        'exchange': {
            'git-commit': 'aex',
            'rootfs-hash': 'sha384:oy',
            'rootfs-url': 'https://example.example'
        }
    })
    assert r.ok
    gear_id = r.json()['_id']

    def teardown_db():
        r = as_admin.delete('/gears/' + gear_id)
        assert r.ok

    request.addfinalizer(teardown_db)

    return gear_id
