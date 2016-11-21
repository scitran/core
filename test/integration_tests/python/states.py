# Various wholesale app states that might be useful in your tests

import time
import pytest

# Currently a dupe from test_uploads.py.
# Could not understand why this doesn't work if I remove the original; future work needed here.
@pytest.fixture(scope="module")
def with_hierarchy_and_file_data(api_as_admin, bunch, request, data_builder):
    group =         data_builder.create_group('test_upload_' + str(int(time.time() * 1000)))
    project =       data_builder.create_project(group)
    session =       data_builder.create_session(project)
    acquisition =   data_builder.create_acquisition(session)

    file_names = ['one.csv', 'two.csv']
    files = {}
    for i, name in enumerate(file_names):
        files['file' + str(i+1)] = (name, 'some,data,to,send\nanother,row,to,send\n')

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
    fixture_data.files = files
    return fixture_data
