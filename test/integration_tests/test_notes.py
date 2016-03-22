import json
import time
import pytest
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_notes(with_a_group_and_a_project, api_as_user):
    data = with_a_group_and_a_project
    notes_path = '/projects/' + data.project_id + '/notes'

    # Add a note
    new_note = json.dumps({'text': 'test note'})
    r = api_as_user.post(notes_path, data=new_note)
    assert r.ok

    # Verify note is present in project
    r = api_as_user.get('/projects/' + data.project_id)
    assert r.ok
    project = json.loads(r.content)
    assert len(project['notes']) == 1

    note_id = project['notes'][0]['_id']
    note_path = '/projects/' + data.project_id + '/notes/' + note_id
    r = api_as_user.get(note_path)
    assert r.ok and json.loads(r.content)['_id'] == note_id

    # Modify note
    modified_note = json.dumps({'text': 'modified test note'})
    r = api_as_user.put(note_path, data=modified_note)
    assert r.ok

    # Verify modified note
    r = api_as_user.get(note_path)
    assert r.ok and json.loads(r.content)['text'] == 'modified test note'

    # Delete note
    r = api_as_user.delete(note_path)
    assert r.ok

    r = api_as_user.get(note_path)
    assert r.status_code == 404
