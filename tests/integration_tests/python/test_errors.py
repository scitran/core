def test_extra_param(as_admin):
    label = 'extra_param'

    r = as_admin.post('/projects', json={
        'group': 'unknown',
        'label': label,
        'public': False,
        'extra_param': 'some_value'
    })
    assert r.status_code == 400

    r = as_admin.get('/projects')
    assert r.ok
    assert not any(project['label'] == label for project in r.json())

def test_error_response(as_admin, as_user, as_public, as_root, data_builder, api_db):

    group = data_builder.create_group()
    project = data_builder.create_project()

    # Test dao exception
    r = as_admin.post('/users', json={'_id': "admin@user.com", 'firstname': "Firstname", 'lastname': "Lastname"})
    assert r.status_code == 409
    assert r.json().get('request_id')

    # Test schema exceptions
    r = as_admin.post('/groups', json={'foo':'bar'})
    assert r.status_code == 400
    assert r.json().get('request_id')

    # Test Permission exception
    r = as_user.put('/projects/' + project, json={'label':'Project'})
    assert r.status_code == 403
    assert r.json().get('request_id')

    # Test Key Error
    analysis = api_db.analyses.insert_one({'label':'no-parent'}).inserted_id
    r = as_root.get('/analyses/' + str(analysis))
    assert r.status_code == 500
    assert r.json().get('message') == "Key 'parent' was not found"
    api_db.analyses.delete_one({'_id': analysis})
