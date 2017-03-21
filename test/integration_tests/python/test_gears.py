import json
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_gear_add(as_admin):
    r = as_admin.post('/gears/basic', json={
        "category" : "converter",
        "gear" : {
            "inputs" : {
                "wat" : {
                    "base" : "file",
                    "type" : {
                        "enum" : [
                            "wat"
                        ]
                    }
                }
            },
            "maintainer" : "Example",
            "description" : "Example",
            "license" : "BSD-2-Clause",
            "author" : "Example",
            "url" : "https://example.example",
            "label" : "wat",
            "flywheel" : "0",
            "source" : "https://example.example",
            "version" : "0.0.1",
            "config" : {},
            "name" : "basic"
        },
        "exchange" : {
            "git-commit" : "aex",
            "rootfs-hash" : "sha384:oy",
            "rootfs-url" : "https://example.example"
        }
    })
    assert r.ok

    _id = r.json()['_id']

    r = as_admin.get('/gears/' + _id)
    assert r.ok
    assert r.json()['gear']['name'] == 'basic'

    r = as_admin.get('/gears/basic')
    assert not r.ok

    r = as_admin.delete('/gears/' + _id)
    assert r.ok


def test_gear_add_versioning(as_admin):

    # Add 0.0.1
    # Add 0.0.2
    # Add 0.0.2 again, should fail

    r = as_admin.post('/gears/multi-version', json={
        "category" : "converter",
        "gear" : {
            "inputs" : {
                "wat" : {
                    "base" : "file",
                    "type" : {
                        "enum" : [
                            "wat"
                        ]
                    }
                }
            },
            "maintainer" : "Example",
            "description" : "Example",
            "license" : "BSD-2-Clause",
            "author" : "Example",
            "url" : "https://example.example",
            "label" : "wat",
            "flywheel" : "0",
            "source" : "https://example.example",
            "version" : "0.0.1",
            "config" : {},
            "name" : "multi-version"
        },
        "exchange" : {
            "git-commit" : "aex",
            "rootfs-hash" : "sha384:oy",
            "rootfs-url" : "https://example.example"
        }
    })
    assert r.ok
    _id = r.json()['_id']

    r = as_admin.get('/gears/' + _id)
    assert r.ok
    assert r.json()['gear']['name']    == 'multi-version'
    assert r.json()['gear']['version'] == '0.0.1'

    r = as_admin.get('/gears?fields=all')
    assert r.ok
    matched = filter(lambda x: x['gear']['name'] == 'multi-version', r.json())
    assert len(matched) == 1

    r = as_admin.post('/gears/multi-version', json={
        "category" : "converter",
        "gear" : {
            "inputs" : {
                "wat" : {
                    "base" : "file",
                    "type" : {
                        "enum" : [
                            "wat"
                        ]
                    }
                }
            },
            "maintainer" : "Example",
            "description" : "Example",
            "license" : "BSD-2-Clause",
            "author" : "Example",
            "url" : "https://example.example",
            "label" : "wat",
            "flywheel" : "0",
            "source" : "https://example.example",
            "version" : "0.0.2",
            "config" : {},
            "name" : "multi-version"
        },
        "exchange" : {
            "git-commit" : "aex",
            "rootfs-hash" : "sha384:oy",
            "rootfs-url" : "https://example.example"
        }
    })
    assert r.ok
    _id = r.json()['_id']


    r = as_admin.get('/gears/' + _id)
    assert r.ok
    assert r.json()['gear']['name']    == 'multi-version'
    assert r.json()['gear']['version'] == '0.0.2'

    r = as_admin.get('/gears?fields=all')
    assert r.ok
    matched = filter(lambda x: x['gear']['name'] == 'multi-version', r.json())
    assert len(matched) == 1

    r = as_admin.post('/gears/multi-version', json={
        "category" : "converter",
        "gear" : {
            "inputs" : {
                "wat" : {
                    "base" : "file",
                    "type" : {
                        "enum" : [
                            "wat"
                        ]
                    }
                }
            },
            "maintainer" : "Example",
            "description" : "Example",
            "license" : "BSD-2-Clause",
            "author" : "Example",
            "url" : "https://example.example",
            "label" : "wat",
            "flywheel" : "0",
            "source" : "https://example.example",
            "version" : "0.0.2",
            "config" : {},
            "name" : "multi-version"
        },
        "exchange" : {
            "git-commit" : "aex",
            "rootfs-hash" : "sha384:oy",
            "rootfs-url" : "https://example.example"
        }
    })
    assert not r.ok

    r = as_admin.get('/gears?fields=all')
    assert r.ok
    matched = filter(lambda x: x['gear']['name'] == 'multi-version', r.json())
    assert len(matched) == 1


def test_gear_add_invalid(as_admin):
    # try to add invalid gear - missing name
    r = as_admin.post('/gears/test_gear', json={})
    assert r.status_code == 400

    # try to add invalid gear - manifest validation error
    r = as_admin.post('/gears/test_gear', json={
        "gear": { "name": "test_gear" }
    })
    assert r.status_code == 400

    # try to add invalid gear - manifest validation error of non-root-level key
    r = as_admin.post('/gears/test_gear', json={
        "gear": {
            "author": "Example",
            "config": {"invalid": "config"},
            "description": "Example",
            "inputs": {"invalid": "inputs"},
            "label": "Example",
            "license": "BSD-2-Clause",
            "name": "test_gear",
            "source": "https://example.example",
            "url": "https://example.example",
            "version": "0.0.0"
        }
    })
    assert r.status_code == 400


def test_gear_access(with_gear, as_public, as_user):
    gear = with_gear

    # test login required
    r = as_public.get('/gears')
    assert r.status_code == 403

    r = as_public.get('/gears/' + gear)
    assert r.status_code == 403

    r = as_public.get('/gears/' + gear + '/invocation')
    assert r.status_code == 403

    r = as_public.get('/gears/' + gear + '/suggest/test-container/test-id')
    assert r.status_code == 403

    # test superuser required
    r = as_user.post('/gears/test-gear', json={'test': 'payload'})
    assert r.status_code == 403

    r = as_user.delete('/gears/test-gear')
    assert r.status_code == 403


def test_gear_invocation_and_suggest(with_gear, with_hierarchy, as_user, as_admin):
    data = with_hierarchy
    gear = with_gear

    # test invocation
    r = as_admin.get('/gears/' + gear + '/invocation')
    assert r.ok

    # test suggest
    r = as_admin.get('/gears/' + gear + '/suggest/session/' + data.session)
    assert r.ok

    # test suggest permission
    r = as_user.get('/gears/' + gear + '/suggest/session/' + data.session)
    assert r.ok
