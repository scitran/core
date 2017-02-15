import json
import os

import requests


class FixtureSession(requests.Session):
    def __init__(self, *args, **kwargs):
        super(FixtureSession, self).__init__(*args, **kwargs)
        self.base_url = os.environ.get('BASE_URL', 'http://localhost:8080/api')
        self.headers.update({'Authorization': 'scitran-user XZpXI40Uk85eozjQkU1zHJ6yZHpix+j0mo1TMeGZ4dPzIqVPVGPmyfeK'})
        self.params.update({'root': 'true'})

    def request(self, method, url, **kwargs):
        return super(FixtureSession, self).request(method, self.base_url + url, **kwargs)


def main():
    session = FixtureSession()

    # create test-group
    r = session.post('/groups', json={'_id': 'test-group'})
    assert r.ok

    # upload file to test-project-1/test-session-1/test-acquisition-1
    # depends on 'create test-group'
    r = session.post('/upload/label', files={
        'file': ('test-1.dcm', open('test/integration_tests/python/test_files/test-1.dcm', 'rb')),
        'metadata': ('', json.dumps({
            'group': { '_id': 'test-group' },
            'project': {
                'label': 'test-project-1'
            },
            'session': {
                'label': 'test-session-1',
                'subject': {
                    'age': 25,
                    'sex': 'male',
                    'firstname': 'xyz'
                }
            },
            'acquisition': {
                'label': 'test-acquisition-1',
                'files': [{ 'name': 'test-1.dcm' }]
            }
        }))
    })
    assert r.ok

    # list projects
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.get('/projects')
    assert r.ok
    assert r.json()[0]['label'] == 'test-project-1'
    test_project = r.json()[0]

    # list sessions
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.get('/sessions')
    assert r.ok
    assert r.json()[0]['label'] == 'test-session-1'
    test_session = r.json()[0]

    # list acquisitions for test-session-1
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.get('/sessions/' + test_session['_id'] + '/acquisitions')
    assert r.ok
    assert r.json()[0]['label'] == 'test-acquisition-1'
    test_acquisition = r.json()[0]

    # add test-case-gear
    r = session.post('/gears/test-case-gear', json={
        'category': 'converter',
        'gear': {
            'inputs': {
                'wat': {
                    'base': 'file',
                    'type': { 'enum': [ 'wat' ] }
                }
            },
            'maintainer': 'Example',
            'description': 'Example',
            'license': 'BSD-2-Clause',
            'author': 'Example',
            'url': 'https://example.example',
            'label': 'wat',
            'flywheel': '0',
            'source': 'https://example.example',
            'version': '0.0.1',
            'config': {},
            'name': 'test-case-gear'
        },
        'exchange': {
            'git-commit': 'aex',
            'rootfs-hash': 'sha384:oy',
            'rootfs-url': 'https://example.example'
        }
    })
    assert r.ok
    test_gear = r.json()

    # create test-collection-1
    r = session.post('/collections', json={
        'label': 'test-collection-1'
    })
    assert r.ok
    test_collection = r.json()

    # add test-session-1 to test-collection-1
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    # depends on 'create test-collection-1'
    r = session.put('/collections/' + test_collection['_id'], json={
        'contents':{
            'operation': 'add',
            'nodes': [{
                'level': 'session',
                '_id': test_session['_id']
            }]
        }
    })
    assert r.ok

    # upload file to test-collection-1
    # depends on 'create test-collection-1'
    r = session.post('/collections/' + test_collection['_id'] + '/files', files={
        'file': ('notes.txt', open('test/integration_tests/python/test_files/notes.txt', 'rb'))
    })
    assert r.ok

    # create test-collection-2
    r = session.post('/collections', json={
        'label': 'test-collection-2'
    })
    assert r.ok

    # upload file to test-project-1
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.post('/projects/' + test_project['_id'] + '/files', files={
        'file': ('notes.txt', open('test/integration_tests/python/test_files/notes.txt', 'rb'))
    })
    assert r.ok

    # upload file to test-session-1
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.post('/sessions/' + test_session['_id'] + '/files', files={
        'file': ('notes.txt', open('test/integration_tests/python/test_files/notes.txt', 'rb'))
    })
    assert r.ok

    # add a note to test-project-1
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.post('/projects/' + test_project['_id'] + '/notes', json={
        'text': 'test note'
    })
    assert r.ok

    # add a note to test-session-1
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.post('/sessions/' + test_session['_id'] + '/notes', json={
        'text': 'test note'
    })
    assert r.ok

    # add a note to test-acquisition-1
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.post('/acquisitions/' + test_acquisition['_id'] + '/notes', json={
        'text': 'test note'
    })
    assert r.ok

    # add a note to test-collection-1
    # depends on 'create test-collection-1'
    r = session.post('/collections/' + test_collection['_id'] + '/notes', json={
        'text': 'test note'
    })
    assert r.ok

    # create session 1 test-analysis-1 (job)
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    # depends on 'add test-case-gear'
    r = session.post('/sessions/' + test_session['_id'] + '/analyses?job=true', json={
        'analysis': { 'label': 'Test Analysis 1' },
        'job': {
            'gear_id': test_gear['_id'],
            'inputs': {
                'dicom': {
                    'type': 'acquisition',
                    'id': test_acquisition['_id'],
                    'name': 'test-1.dcm'
                }
            },
            'tags': ['example']
        }
    })
    assert r.ok
    test_session_analysis = r.json()

    # create session 1 test-analysis (file upload)
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.post('/sessions/' + test_session['_id'] + '/analyses', files={
        'file': ('test-1.dcm', open('test/integration_tests/python/test_files/test-1.dcm', 'rb')),
        'metadata': ('', json.dumps({
            'label': 'test analysis',
            'inputs': [ { 'name': 'test-1.dcm' } ]
        }))
    })
    assert r.ok
    test_session_analysis_upload = r.json()

    # delete session 1 test analysis (file upload)
    # depends on 'create session 1 test-analysis (file upload)'
    r = session.delete('/sessions/' + test_session['_id'] + '/analyses/' + test_session_analysis_upload['_id'])
    assert r.ok

    # create acquisition 1 test-analysis (file upload)
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.post('/acquisitions/' + test_acquisition['_id'] + '/analyses', files={
        'file': ('test-1.dcm', open('test/integration_tests/python/test_files/test-1.dcm', 'rb')),
        'metadata': ('', json.dumps({
            'label': 'test analysis',
            'inputs': [ { 'name': 'test-1.dcm' } ]
        }))
    })
    assert r.ok
    test_acquisition_analysis_upload = r.json()

    # create acquisition 1 test-analysis 2 (file upload)
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.post('/acquisitions/' + test_acquisition['_id'] + '/analyses', files={
        'file': ('test-1.dcm', open('test/integration_tests/python/test_files/test-1.dcm', 'rb')),
        'metadata': ('', json.dumps({
            'label': 'test analysis',
            'inputs': [ { 'name': 'test-1.dcm' } ]
        }))
    })
    assert r.ok

    # create collection 1 test-analysis (file upload)
    # depends on 'create test-collection-1'
    r = session.post('/collections/' + test_collection['_id'] + '/analyses', files={
        'file': ('test-1.dcm', open('test/integration_tests/python/test_files/test-1.dcm', 'rb')),
        'metadata': ('', json.dumps({
            'label': 'test analysis',
            'inputs': [ { 'name': 'test-1.dcm' } ]
        }))
    })
    assert r.ok
    test_collection_analysis_upload = r.json()

    # create collection 1 test-analysis 2 (file upload)
    # depends on 'create test-collection-1'
    r = session.post('/collections/' + test_collection['_id'] + '/analyses', files={
        'file': ('test-1.dcm', open('test/integration_tests/python/test_files/test-1.dcm', 'rb')),
        'metadata': ('', json.dumps({
            'label': 'test analysis 2',
            'inputs': [ { 'name': 'test-1.dcm' } ]
        }))
    })
    assert r.ok

    # create project 1 test-analysis (file upload)
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.post('/projects/' + test_project['_id'] + '/analyses', files={
        'file': ('test-1.dcm', open('test/integration_tests/python/test_files/test-1.dcm', 'rb')),
        'metadata': ('', json.dumps({
            'label': 'test analysis',
            'inputs': [ { 'name': 'test-1.dcm' } ]
        }))
    })
    assert r.ok
    test_project_analysis_upload = r.json()

    # create project 1 test-analysis 2 (file upload)
    # depends on 'upload file to test-project-1/test-session-1/test-acquisition-1'
    r = session.post('/projects/' + test_project['_id'] + '/analyses', files={
        'file': ('test-1.dcm', open('test/integration_tests/python/test_files/test-1.dcm', 'rb')),
        'metadata': ('', json.dumps({
            'label': 'test analysis',
            'inputs': [ { 'name': 'test-1.dcm' } ]
        }))
    })
    assert r.ok

    # add a note to test-acquisition-1 test-analysis-1
    # depends on 'create acquisition 1 test-analysis (file upload)'
    r = session.post('/acquisitions/' + test_acquisition['_id'] + '/analyses/' + test_acquisition_analysis_upload['_id'] + '/notes', json={
        'text': 'test note'
    })
    assert r.ok

    # add a note to test-collection-1 test-analysis-1
    # depends on 'create test-collection-1'
    r = session.post('/collections/' + test_collection['_id'] + '/analyses/' + test_collection_analysis_upload['_id'] + '/notes', json={
        'text': 'test note'
    })
    assert r.ok

    # add a note to test-session-1 test-analysis-1
    # depends on 'create session 1 test-analysis (file upload)'
    r = session.post('/sessions/' + test_session['_id'] + '/analyses/' + test_session_analysis['_id'] + '/notes', json={
        'text': 'test note'
    })
    assert r.ok

    # add a note to test-project-1 test-analysis-1
    # depends on 'create project 1 test-analysis (file upload)'
    r = session.post('/projects/' + test_project['_id'] + '/analyses/' + test_project_analysis_upload['_id'] + '/notes', json={
        'text': 'test note'
    })
    assert r.ok

    # create project
    r = session.post('/projects', json={
        'group': 'test-group',
        'label': 'Project with template',
        'public': False
    })
    assert r.ok
    st_project = r.json()

    # create compliant session
    # depends on 'create project'
    r = session.post('/sessions', json={
        'subject': { 'code': 'ex8945' },
        'label': 'Compliant Session',
        'project': st_project['_id'],
        'public': False
    })
    assert r.ok
    st_compliant_session = r.json()

    # create non-compliant session
    # depends on 'create project'
    r = session.post('/sessions', json={
        'subject': { 'code': 'ex9849' },
        'label': 'Non-compliant Session',
        'project': st_project['_id'],
        'public': False
    })
    assert r.ok
    st_noncompliant_session = r.json()

    # create acquisition-1 for compliant session
    # depends on 'create compliant session'
    r = session.post('/acquisitions', json={
        'label': 'c-acquisition-1-t1',
        'session': st_compliant_session['_id'],
        'public': False
    })
    assert r.ok

    # create acquisition-2 for compliant session
    # depends on 'create compliant session'
    r = session.post('/acquisitions', json={
        'label': 'c-acquisition-2-t1',
        'session': st_compliant_session['_id'],
        'public': False
    })
    assert r.ok

    # create acquisition-1 for noncompliant session
    # depends on 'create non-compliant session'
    r = session.post('/acquisitions', json={
        'label': 'nc-acquisition-1-t1',
        'session': st_noncompliant_session['_id'],
        'public': False
    })
    assert r.ok

    # add project template
    r = session.post('/projects/' + st_project['_id'] + '/template', json={
        'session': { 'subject': { 'code' : '^ex' } },
        'acquisitions': [{
            'label': 't1',
            'minimum': 2
        }]
    })
    assert r.ok
    assert r.json()['modified'] == 1

    # create acquisition-2 for noncompliant session
    # depends on 'create non-compliant session'
    r = session.post('/acquisitions', json={
        'label': 'nc-acquisition-2-t1',
        'session': st_noncompliant_session['_id'],
        'public': False
    })
    assert r.ok

    # update session 2 to be non-compliant
    # depends on 'create non-compliant session'
    r = session.put('/sessions/' + st_noncompliant_session['_id'], json={
        'subject': { 'code': 'bad-subject-code' }
    })
    assert r.ok


if __name__ == '__main__':
    main()
