from dateutil.parser import parse

def test_groups(as_admin, data_builder):
    # Cannot find a non-existant group
    r = as_admin.get('/groups/non-existent')
    assert r.status_code == 404

    group = data_builder.create_group()

    # Able to find new group
    r = as_admin.get('/groups/' + group)
    assert r.ok
    first_modified = r.json()['modified']

    # Able to change group name
    group_name = 'New group name'
    r = as_admin.put('/groups/' + group, json={'name': group_name})
    assert r.ok
    second_modified = r.json()['modified']
    d1 = parse(first_modified)
    d2 = parse(second_modified)
    assert d2 > d1

    r = as_admin.get('/groups/' + group)
    assert r.ok
    assert r.json()['name'] == group_name
