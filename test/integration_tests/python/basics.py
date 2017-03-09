# New test fixtures!
#
# The intention is to slowly build these up until we like them, then port the tests to them and replace parts of conftest.py

import pytest
import requests

# The request Session object has no support for a base URL; this is a subclass to embed one.
# Dynamically generated via a pytest fixture so that we can use the upstream fixtures.
@pytest.fixture(scope="module")
def base_url_session(base_url):
    class BaseUrlSession(requests.Session):
        def request(self, method, url, params=None, data=None, headers=None, cookies=None, files=None, auth=None, timeout=None, allow_redirects=True, proxies=None, hooks=None, stream=None, verify=None, cert=None, json=None):

            url = base_url + url

            return super(BaseUrlSession, self).request(method, url, params, data, headers, cookies, files, auth, timeout, allow_redirects, proxies, hooks, stream, verify, cert, json)

    return BaseUrlSession


_apiAsAdmin = None

# A replacement for the RequestsAccessor class. Less boilerplate, no kwarg fiddling.
# This has the added benefit of re-using HTTP connections, which might speed up our testing considerably.
#
# Ref: http://stackoverflow.com/a/34491383
@pytest.fixture(scope="module")
def as_admin(base_url_session):

    global _apiAsAdmin

    # Create one session and reuse it.
    if _apiAsAdmin is None:

        s = base_url_session()

        s.headers.update({
            "Authorization":"scitran-user XZpXI40Uk85eozjQkU1zHJ6yZHpix+j0mo1TMeGZ4dPzIqVPVGPmyfeK"
        })
        s.params.update({
            "root": "true"
        })

        _apiAsAdmin = s

    return _apiAsAdmin


_apiAsUser = None

@pytest.fixture(scope="module")
def as_user(base_url_session):

    global _apiAsUser

    # Create one session and reuse it.
    if _apiAsUser is None:

        s = base_url_session()

        s.headers.update({
            "Authorization":"scitran-user XZpXI40Uk85eozjQkU1zHJ6yZHpix+j0mo1TMeGZ4dPzIqVPVGPmyfeK"
        })

        _apiAsUser = s

    return _apiAsUser


_apiAsPublic = None

@pytest.fixture(scope="module")
def as_public(base_url_session):

    global _apiAsPublic

    # Create one session and reuse it.
    if _apiAsPublic is None:

        s = base_url_session()

        _apiAsPublic = s

    return _apiAsPublic
