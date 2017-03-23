import os

import pytest
import requests


class BaseUrlSession(requests.Session):
    def __init__(self, *args, **kwargs):
        super(BaseUrlSession, self).__init__(*args, **kwargs)
        self.base_url = os.environ.get('BASE_URL', 'http://localhost:8080/api')

    def request(self, method, url, **kwargs):
        return super(BaseUrlSession, self).request(method, self.base_url + url, **kwargs)


@pytest.fixture(scope='session')
def as_admin():
    session = BaseUrlSession()
    session.headers.update({'Authorization': 'scitran-user XZpXI40Uk85eozjQkU1zHJ6yZHpix+j0mo1TMeGZ4dPzIqVPVGPmyfeK'})
    session.params.update({'root': 'true'})
    return session


@pytest.fixture(scope='session')
def as_user():
    session = BaseUrlSession()
    session.headers.update({'Authorization': 'scitran-user XZpXI40Uk85eozjQkU1zHJ6yZHpix+j0mo1TMeGZ4dPzIqVPVGPmyfeK'})
    return session


# use function scope to enable customizing session w/o affecting other tests
@pytest.fixture(scope='function')
def as_public():
    return BaseUrlSession()
