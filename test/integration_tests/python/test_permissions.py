def test_permissions(data_builder, as_admin):
    project = data_builder.create_project()
    user_1 = data_builder.create_user(_id='test-permissions-1@user.com')
    user_2 = data_builder.create_user(_id='test-permissions-2@user.com')

    permissions_path = '/projects/' + project + '/permissions'
    user_1_path = permissions_path + '/' + user_1
    user_2_path = permissions_path + '/' + user_2

    # GET is not allowed for general permissions path
    r = as_admin.get(permissions_path)
    assert r.status_code == 405

    # Add permissions for user 1
    r = as_admin.post(permissions_path, json={
        '_id': user_1,
        'access': 'ro'
    })
    assert r.ok

    # Verify permissions for user 1
    r = as_admin.get(user_1_path)
    assert r.ok
    perms = r.json()
    assert perms['_id'] == user_1
    assert perms['access'] == 'ro'

    # Update user 1 to have admin access
    r = as_admin.put(user_1_path, json={'access': 'admin'})
    assert r.ok

    # Add user 2 to have ro access
    r = as_admin.post(permissions_path, json={
        '_id': user_2,
        'access': 'ro'
    })
    assert r.ok

    # Attempt to change user 2's id to user 1
    r = as_admin.put(user_2_path, json={'_id': user_1})
    assert r.status_code == 404

    # Delete user 2
    r = as_admin.delete(user_2_path)
    assert r.ok

    # Ensure user 2 is gone
    r = as_admin.get(user_2_path)
    assert r.status_code == 404
