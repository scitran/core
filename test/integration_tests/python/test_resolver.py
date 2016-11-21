import json
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_resolver_root(as_admin, with_hierarchy_and_file_data):
    r = as_admin.post('/resolve', json={'path': []})
    assert r.ok

    result   = r.json()
    path     = result['path']
    children = result['children']

    # root node should not walk
    assert len(path) == 0

    # should be 3 groups
    assert len(children) == 3

    for node in children:
        assert node['node_type'] == 'group'


def test_resolver_group(as_admin, with_hierarchy_and_file_data):
    r = as_admin.post('/resolve', json={'path': [ 'scitran' ]})
    assert r.ok

    result   = r.json()
    path     = result['path']
    children = result['children']

    # group node is one down from root
    assert len(path) == 1

    # should be no children
    assert len(children) == 0
