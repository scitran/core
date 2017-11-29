def test_collections(data_builder, as_admin, as_user):
    session = data_builder.create_session()
    acquisition = data_builder.create_acquisition()

    # create collection
    r = as_admin.post('/collections', json={
        'label': 'SciTran/Testing'
    })
    assert r.ok
    collection = r.json()['_id']

    # get all collections w/ stats=true
    r = as_admin.get('/collections', params={'stats': 'true'})
    assert r.ok
    assert all('session_count' in coll for coll in r.json())

    # get collection
    r = as_admin.get('/collections/' + collection)
    assert r.ok

    # test empty update
    r = as_admin.put('/collections/' + collection, json={})
    assert r.status_code == 400

    # add session to collection
    r = as_admin.put('/collections/' + collection, json={
        'contents': {
            'operation': 'add',
            'nodes': [
                {'level': 'session', '_id': session}
            ],
        }
    })
    assert r.ok

    # test if collection is listed at acquisition
    r = as_admin.get('/acquisitions/' + acquisition)
    assert r.ok
    assert collection in r.json()['collections']


    ###
    #   Test user only sees sessions/acquisitions they have access to
    ###

    project2 = data_builder.create_project()
    session2 = data_builder.create_session(project=project2)
    acquisition2 = data_builder.create_acquisition(session=session2)

    # add session2 to collection
    r = as_admin.put('/collections/' + collection, json={
        'contents': {
            'operation': 'add',
            'nodes': [
                {'level': 'session', '_id': session2}
            ],
        }
    })
    assert r.ok

    # test user cannot access sessions/acquisitions of collection without perms
    r = as_user.get('/collections/' + collection)
    assert r.status_code == 403
    r = as_user.get('/collections/' + collection + '/sessions')
    assert r.status_code == 403
    r = as_user.get('/collections/' + collection + '/acquisitions')
    assert r.status_code == 403


    # add user to collection
    r = as_user.get('/users/self')
    assert r.ok
    uid = r.json()['_id']

    r = as_admin.post('/collections/' + collection + '/permissions', json={'_id': uid, 'access': 'ro'})
    assert r.ok

    # test user cannot see sessions or acquisitions
    r = as_user.get('/collections/' + collection + '/sessions')
    assert r.ok
    assert r.json() == []

    r = as_user.get('/collections/' + collection + '/acquisitions')
    assert r.ok
    assert r.json() == []

    # add user to project
    r = as_admin.post('/projects/' + project2 + '/permissions', json={'_id': uid, 'access': 'ro'})
    assert r.ok

    # test user can now see some of sessions and acquisitions
    r = as_user.get('/collections/' + collection + '/sessions')
    assert r.ok
    sessions = r.json()
    assert len(sessions) == 1
    assert sessions[0]['_id'] == session2

    r = as_user.get('/collections/' + collection + '/acquisitions')
    assert r.ok
    acquisitions = r.json()
    assert len(acquisitions) == 1
    assert acquisitions[0]['_id'] == acquisition2


    # delete collection
    r = as_admin.delete('/collections/' + collection)
    assert r.ok

    # try to get deleted collection
    r = as_admin.get('/collections/' + collection)
    assert r.status_code == 404

    # test if collection is listed at acquisition
    r = as_admin.get('/acquisitions/' + acquisition)
    assert collection not in r.json()['collections']
