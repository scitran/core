import os
import time
import json
import pytest
import pymongo
import requests


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
def base_url():
    return os.environ.get('BASE_URL', 'http://localhost:8080/api')


class RequestsAccessor(object):
    def __init__(self, base_url, default_params):
        self.base_url = base_url
        self.default_params = default_params

    def _get_params(self, **kwargs):
        params = self.default_params.copy()
        if ("params" in kwargs):
            params.update(kwargs["params"])
        return params

    def post(self, url_path, *args, **kwargs):
        kwargs['params'] = self._get_params(**kwargs)
        return requests.post(self.base_url + url_path, verify=False, *args, **kwargs)

    def delete(self, url_path, *args, **kwargs):
        kwargs['params'] = self._get_params(**kwargs)
        return requests.delete(self.base_url + url_path, verify=False, *args, **kwargs)

    def get(self, url_path, *args, **kwargs):
        kwargs['params'] = self._get_params(**kwargs)
        return requests.get(self.base_url + url_path, verify=False, *args, **kwargs)

    def put(self, url_path, *args, **kwargs):
        kwargs['params'] = self._get_params(**kwargs)
        return requests.put(self.base_url + url_path, verify=False, *args, **kwargs)


@pytest.fixture(scope="module")
def api_as_admin(base_url):
    accessor = RequestsAccessor(base_url, {"user": "admin@user.com", "root": "true"})
    return accessor


@pytest.fixture(scope="module")
def api_as_user(base_url):
    accessor = RequestsAccessor(base_url, {"user": "admin@user.com"})
    return accessor


@pytest.fixture(scope="module")
def api_accessor(base_url):
    class RequestsAccessorWithBaseUrl(RequestsAccessor):
        def __init__(self, user):
            super(self.__class__, self).__init__(base_url, {"user": user})

    return RequestsAccessorWithBaseUrl


@pytest.fixture()
def with_a_group_and_a_project(api_as_admin, data_builder, request, bunch):

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
def data_builder(api_as_admin):
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
                    r = api_as_admin.post(api_path, data=payload)
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
                    r = api_as_admin.delete(api_path + '/' + id)
                    assert r.ok

                return delete_
    return DataBuilder()
