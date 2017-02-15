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
    # Try to add invalid gear
    r = as_admin.post('/gears/test_gear', json={
        "category": "converter",
        "input": { },
        "name": "test_gear",
        "manifest": {
            "name": "test_gear",
            "inputs": {
                "any text file <= 100 KB": {
                    "base": "file",
                    "name": {
                        "pattern": "^.*.txt$"
                    },
                    "size": {
                        "maximum": 100000
                    }
                }
            },
            "description": "A gear to test the API",
            "license": "Other",
            "author": "",
            "url": "https://unknown.example",
            "label": "An exported gear",
            "source": "https://unknown.example",
            "config": {
                "two-digit multiple of ten": {
                    "exclusiveMaximum": True,
                    "type": "number",
                    "multipleOf": 10,
                    "maximum": 100
                }
            }
        }
    })
    assert r.status_code == 400
