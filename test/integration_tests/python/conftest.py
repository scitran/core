import json
import os
import time

import pytest
import pymongo
import requests


# Pytest considers fixtures to be provided by "plugins", which are generally provided by
# files called conftest.py. This prevents us from placing module-level fixture logic in
# well-organized files. To fix this, we simply star-import from files that we need.
#
# Ref: http://pytest.org/2.2.4/plugins.html
from basics import *
from states import *

@pytest.fixture(scope="session")
def bunch():
    class BunchFactory:
        def create():
            return type('Bunch', (), {})
        create = staticmethod(create)

    return BunchFactory()


@pytest.fixture(scope="module")
def db():
    mongo_path = os.environ.get('MONGO_PATH', 'mongodb://localhost:9001/scitran')
    return pymongo.MongoClient(mongo_path).get_default_database()

@pytest.fixture(scope="module")
def access_log_db():
    mongo_path = os.environ.get('ACCESS_LOG_MONGO_PATH', 'mongodb://localhost:9001/logs')
    return pymongo.MongoClient(mongo_path).get_default_database()


@pytest.fixture()
def with_a_group_and_a_project(as_admin, data_builder, request, bunch):

    user_1 = 'user1@user.com'
    user_2 = 'user2@user.com'

    group_id = 'test_group_' + str(int(time.time() * 1000))
    data_builder.create_group(group_id)
    project_id = data_builder.create_project(group_id)

    def teardown_db():
        data_builder.delete_project(project_id)
        data_builder.delete_group(group_id)

    request.addfinalizer(teardown_db)

    fixture_data = bunch.create()
    fixture_data.project_id = project_id
    fixture_data.user_1 = user_1
    fixture_data.user_2 = user_2
    return fixture_data


@pytest.fixture(scope="module")
def data_builder(as_admin):
    class DataBuilder:

        # This function is called whenever DataBuilder.create_X() or
        # DataBuilder.delete_X() is called (those functions don't
        # actually exist). It returns a callable object that does the right
        # thing based on what X is.
        # This madness allows significantly reduced copy/paste code and more
        # readable tests.
        def __getattr__(self, name):
            parent_attribute = {
                'group': '_id',
                'project': 'group',
                'session': 'project',
                'acquisition': 'session'
            }

            # create_<container>( parent_id )
            #
            # Call the functions create_group(), create_project(), create_session(),
            # or create_acquisition(), passing in the "parent" container id as the singular
            # parameter.
            if name.startswith('create_'):
                def create_(parent_id):
                    container = name.split('create_')[1]
                    parent = parent_attribute[container]
                    api_path = '/' + container + 's'  # API paths are pluralized

                    payload = {parent: parent_id}
                    if (container != 'group'):
                        payload.update({'public': True, 'label': container + '_testing'})
                    payload = json.dumps(payload)

                    print api_path, payload
                    r = as_admin.post(api_path, data=payload)
                    print r.content

                    assert r.ok

                    return json.loads(r.content)['_id']
                return create_

            # delete_<container>( id )
            #
            # Call the functions delete_group(), delete_project(), delete_session(),
            # or delete_acquisition(), passing in the id as the only parameter.
            if name.startswith('delete_'):
                def delete_(id):
                    container = name.split('delete_')[1]
                    api_path = '/' + container + 's'  # API paths are pluralized
                    r = as_admin.delete(api_path + '/' + id)
                    assert r.ok

                return delete_
    return DataBuilder()
