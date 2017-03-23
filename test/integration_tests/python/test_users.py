import json
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_users(as_admin):
    new_user_id = 'new@user.com'

    # List users
    r = as_admin.get('/users')
    assert r.ok

    # Get self
    r = as_admin.get('/users/self')
    assert r.ok

    # Try adding new user missing required attr
    payload = json.dumps({
        '_id': 'jane.doe@gmail.com',
        'lastname': 'Doe',
        'email': 'jane.doe@gmail.com',
    })
    r = as_admin.post('/users', data=payload)
    assert r.status_code == 400
    assert "'firstname' is a required property" in r.text

    # Add new user
    r = as_admin.get('/users/' + new_user_id)
    assert r.status_code == 404
    payload = json.dumps({
        '_id': new_user_id,
        'firstname': 'New',
        'lastname': 'User',
    })
    r = as_admin.post('/users', data=payload)
    assert r.ok
    r = as_admin.get('/users/' + new_user_id)
    assert r.ok

    # Modify existing user
    payload = json.dumps({
        'firstname': 'Realname'
    })
    r = as_admin.put('/users/' + new_user_id, data=payload)
    assert r.ok

    # Cleanup
    r = as_admin.delete('/users/' + new_user_id)
    assert r.ok
