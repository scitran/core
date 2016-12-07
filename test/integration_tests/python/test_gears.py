import json
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_gear_add(as_admin):
    r = as_admin.post('/gears/test-case-gear', json={
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
            "name" : "test-case-gear"
        },
        "exchange" : {
            "git-commit" : "aex",
            "rootfs-hash" : "sha384:oy",
            "rootfs-url" : "https://example.example"
        }
    })
    assert r.ok
