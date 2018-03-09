def path_in_result(path, result):
    return [node.get('_id', node.get('name')) for node in result['path']] == path


def child_in_result(child, result):
    return sum(all((k in c and c[k]==v) for k,v in child.iteritems()) for c in result['children']) == 1

def idz(s):
    return '<id:' + s + '>'

def test_resolver(data_builder, as_admin, as_user, as_public, file_form):
    # ROOT
    # try accessing resolver w/o logging in
    r = as_public.post('/resolve', json={'path': []})
    assert r.status_code == 403

    # try resolving invalid (non-list) path
    r = as_admin.post('/resolve', json={'path': 'test'})
    assert r.status_code == 400 

    # resolve root (empty)
    r = as_admin.post('/resolve', json={'path': []})
    result = r.json()
    assert r.ok
    assert result['path'] == []
    assert result['children'] == []

    # resolve root (1 group)
    group = data_builder.create_group()
    r = as_admin.post('/resolve', json={'path': []})
    result = r.json()
    assert r.ok
    assert result['path'] == []
    assert child_in_result({'_id': group, 'node_type': 'group'}, result)

    # try to resolve non-existent root/child
    r = as_admin.post('/resolve', json={'path': ['child']})
    assert r.status_code == 404


    # GROUP
    # try to resolve root/group as different (and non-root) user
    r = as_user.post('/resolve', json={'path': [group]})
    assert r.status_code == 403

    # resolve root/group (empty)
    r = as_admin.post('/resolve', json={'path': [group]})
    result = r.json()
    assert r.ok
    assert path_in_result([group], result)
    assert result['children'] == []

    # resolve root/group (1 project)
    project_label = 'test-resolver-project-label'
    project = data_builder.create_project(label=project_label)
    r = as_admin.post('/resolve', json={'path': [group]})
    result = r.json()
    assert r.ok
    assert path_in_result([group], result)
    assert child_in_result({'_id': project, 'node_type': 'project'}, result)

    # try to resolve non-existent root/group/child
    r = as_admin.post('/resolve', json={'path': [group, 'child']})
    assert r.status_code == 404


    # PROJECT
    # resolve root/group/project (empty)
    r = as_admin.post('/resolve', json={'path': [group, project_label]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project], result)
    assert result['children'] == []

    # resolve root/group/project (1 file)
    project_file = 'project_file'
    r = as_admin.post('/projects/' + project + '/files', files=file_form(project_file))
    assert r.ok
    r = as_admin.post('/resolve', json={'path': [group, project_label]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project], result)
    assert child_in_result({'name': project_file, 'node_type': 'file'}, result)
    assert len(result['children']) == 1

    # resolve root/group/project (1 file, 1 session)
    session_label = 'test-resolver-session-label'
    session = data_builder.create_session(label=session_label)
    r = as_admin.post('/resolve', json={'path': [group, project_label]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project], result)
    assert child_in_result({'_id': session, 'node_type': 'session'}, result)
    assert len(result['children']) == 2

    # resolve root/group/project/file (old way)
    r = as_admin.post('/resolve', json={'path': [group, project_label, project_file]})
    result = r.json()
    assert r.status_code == 404

    # resolve root/group/project/file
    r = as_admin.post('/resolve', json={'path': [group, project_label, 'files', project_file]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project, project_file], result)
    assert result['children'] == []

    # try to resolve non-existent root/group/project/child
    r = as_admin.post('/resolve', json={'path': [group, project_label, 'child']})
    assert r.status_code == 404


    # SESSION
    # resolve root/group/project/session (empty)
    r = as_admin.post('/resolve', json={'path': [group, project_label, session_label]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project, session], result)
    assert result['children'] == []

    # resolve root/group/project/session (1 file)
    session_file = 'session_file'
    r = as_admin.post('/sessions/' + session + '/files', files=file_form(session_file))
    assert r.ok
    r = as_admin.post('/resolve', json={'path': [group, project_label, session_label]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project, session], result)
    assert child_in_result({'name': session_file, 'node_type': 'file'}, result)
    assert len(result['children']) == 1

    # resolve root/group/project/session (1 file, 1 acquisition)
    acquisition_label = 'test-resolver-acquisition-label'
    acquisition = data_builder.create_acquisition(label=acquisition_label)
    r = as_admin.post('/resolve', json={'path': [group, project_label, session_label]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project, session], result)
    assert child_in_result({'_id': acquisition, 'node_type': 'acquisition'}, result)
    assert len(result['children']) == 2

    # resolve root/group/project/session/file
    r = as_admin.post('/resolve', json={'path': [group, project_label, session_label, 'files', session_file]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project, session, session_file], result)
    assert result['children'] == []

    # try to resolve non-existent root/group/project/session/child
    r = as_admin.post('/resolve', json={'path': [group, project_label, session_label, 'child']})
    assert r.status_code == 404


    # ACQUISITION
    # resolve root/group/project/session/acquisition (empty)
    r = as_admin.post('/resolve', json={'path': [group, project_label, session_label, acquisition_label]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project, session, acquisition], result)
    assert result['children'] == []

    # resolve root/group/project/session/acquisition (1 file)
    acquisition_file = 'acquisition_file'
    r = as_admin.post('/acquisitions/' + acquisition + '/files', files=file_form(acquisition_file))
    assert r.ok
    r = as_admin.post('/resolve', json={'path': [group, project_label, session_label, acquisition_label]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project, session, acquisition], result)
    assert child_in_result({'name': acquisition_file, 'node_type': 'file'}, result)
    assert len(result['children']) == 1

    # resolve root/group/project/session/acquisition/file
    r = as_admin.post('/resolve', json={'path': [group, project_label, session_label, acquisition_label, 'files', acquisition_file]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project, session, acquisition, acquisition_file], result)
    assert result['children'] == []

    # resolve root/group/project/session/acquisition/file with id
    r = as_admin.post('/resolve', json={'path': [idz(group), idz(project), idz(session), idz(acquisition), 'files', acquisition_file]})
    result = r.json()
    assert r.ok
    assert path_in_result([group, project, session, acquisition, acquisition_file], result)
    assert result['children'] == []

    # resolve root/group/project/session/acquisition/file with invalid id
    r = as_admin.post('/resolve', json={'path': [idz(group), idz(project), idz('not-valid'), idz(acquisition), 'files', acquisition_file]})
    assert r.status_code == 400

    # try to resolve non-existent root/group/project/session/acquisition/child
    r = as_admin.post('/resolve', json={'path': [group, project_label, session_label, acquisition_label, 'child']})
    assert r.status_code == 404


    # FILE
    # try to resolve non-existent (also invalid) root/group/project/session/acquisition/file/child
    r = as_admin.post('/resolve', json={'path': [group, project_label, session_label, acquisition_label, acquisition_file, 'child']})
    assert r.status_code == 404

def test_lookup(data_builder, as_admin, as_user, as_public, file_form):
    # ROOT
    # try accessing lookupr w/o logging in
    r = as_public.post('/lookup', json={'path': []})
    assert r.status_code == 403

    # try resolving invalid (non-list) path
    r = as_admin.post('/lookup', json={'path': 'test'})
    assert r.status_code == 400 

    # lookup root (empty)
    r = as_admin.post('/lookup', json={'path': []})
    result = r.json()
    assert r.status_code == 404

    # lookup root (1 group)
    group = data_builder.create_group()
    r = as_admin.post('/lookup', json={'path': []})
    result = r.json()
    assert r.status_code == 404

    # try to lookup non-existent root/child
    r = as_admin.post('/lookup', json={'path': ['child']})
    assert r.status_code == 404


    # GROUP
    # try to lookup root/group as different (and non-root) user
    r = as_user.post('/lookup', json={'path': [group]})
    assert r.status_code == 403

    # lookup root/group (empty)
    r = as_admin.post('/lookup', json={'path': [group]})
    result = r.json()
    assert r.ok
    assert result['node_type'] == 'group'
    assert result['_id'] == group

    # try to lookup non-existent root/group/child
    r = as_admin.post('/lookup', json={'path': [group, 'child']})
    assert r.status_code == 404


    # PROJECT
    # lookup root/group/project (empty)
    project_label = 'test-lookupr-project-label'
    project = data_builder.create_project(label=project_label)

    r = as_admin.post('/lookup', json={'path': [group, project_label]})
    result = r.json()
    assert r.ok
    assert result['node_type'] == 'project'
    assert result['_id'] == project 

    # lookup root/group/project/file
    project_file = 'project_file'
    r = as_admin.post('/projects/' + project + '/files', files=file_form(project_file))
    assert r.ok

    r = as_admin.post('/lookup', json={'path': [group, project_label, 'files', project_file]})
    result = r.json()
    assert r.ok
    assert result['node_type'] == 'file'
    assert result['name'] == project_file 
    assert 'mimetype' in result
    assert 'size' in result

    # try to lookup non-existent root/group/project/child
    r = as_admin.post('/lookup', json={'path': [group, project_label, 'child']})
    assert r.status_code == 404


    # SESSION
    # lookup root/group/project/session (empty)
    session_label = 'test-lookupr-session-label'
    session = data_builder.create_session(label=session_label)

    r = as_admin.post('/lookup', json={'path': [group, project_label, session_label]})
    result = r.json()
    assert r.ok
    assert result['node_type'] == 'session'
    assert result['_id'] == session

    # lookup root/group/project/session/file
    session_file = 'session_file'
    r = as_admin.post('/sessions/' + session + '/files', files=file_form(session_file))
    assert r.ok

    r = as_admin.post('/lookup', json={'path': [group, project_label, session_label, 'files', session_file]})
    result = r.json()
    assert r.ok
    assert result['node_type'] == 'file'
    assert result['name'] == session_file 
    assert 'mimetype' in result
    assert 'size' in result

    # try to lookup non-existent root/group/project/session/child
    r = as_admin.post('/lookup', json={'path': [group, project_label, session_label, 'child']})
    assert r.status_code == 404

    # ACQUISITION
    # lookup root/group/project/session/acquisition (empty)
    acquisition_label = 'test-lookupr-acquisition-label'
    acquisition = data_builder.create_acquisition(label=acquisition_label)
    r = as_admin.post('/lookup', json={'path': [group, project_label, session_label, acquisition_label]})
    result = r.json()
    assert r.ok
    assert result['node_type'] == 'acquisition'
    assert result['_id'] == acquisition 

    # lookup root/group/project/session/acquisition/file
    acquisition_file = 'acquisition_file'
    r = as_admin.post('/acquisitions/' + acquisition + '/files', files=file_form(acquisition_file))
    assert r.ok

    r = as_admin.post('/lookup', json={'path': [group, project_label, session_label, acquisition_label, 'files', acquisition_file]})
    result = r.json()
    assert r.ok
    assert result['node_type'] == 'file'
    assert result['name'] == acquisition_file
    assert 'mimetype' in result
    assert 'size' in result

    # lookup root/group/project/session/acquisition with id
    r = as_admin.post('/lookup', json={'path': [idz(group), idz(project), idz(session), idz(acquisition)]})
    result = r.json()
    assert r.ok
    assert result['node_type'] == 'acquisition'
    assert result['_id'] == acquisition 

    # lookup root/group/project/session/acquisition/file with id
    r = as_admin.post('/lookup', json={'path': [idz(group), idz(project), idz(session), idz(acquisition), 'files', acquisition_file]})
    result = r.json()
    assert r.ok
    assert result['node_type'] == 'file'
    assert result['name'] == acquisition_file
    assert 'mimetype' in result
    assert 'size' in result

    # try to lookup non-existent root/group/project/session/acquisition/child
    r = as_admin.post('/lookup', json={'path': [group, project_label, session_label, acquisition_label, 'child']})
    assert r.status_code == 404


    # FILE
    # try to lookup non-existent (also invalid) root/group/project/session/acquisition/file/child
    r = as_admin.post('/lookup', json={'path': [group, project_label, session_label, acquisition_label, 'files', acquisition_file, 'child']})
    assert r.status_code == 404

