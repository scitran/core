import json
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_tags(with_a_group_and_a_project, as_admin):
    data = with_a_group_and_a_project
    tag = 'test_tag'
    new_tag = 'new_test_tag'
    other_tag = 'other_test_tag'
    short_tag = 't'
    too_long_tag = 'this_tag_is_much_too_long_only_allow_32_characters'

    tags_path = '/projects/' + data.project_id + '/tags'
    tag_path = tags_path + '/' + tag
    new_tag_path = tags_path + '/' + new_tag
    other_tag_path = tags_path + '/' + other_tag
    short_tag_path = tags_path + '/' + short_tag

    # Add tag and verify
    r = as_admin.get(tag_path)
    assert r.status_code == 404
    payload = json.dumps({'value': tag})
    r = as_admin.post(tags_path, data=payload)
    assert r.ok
    r = as_admin.get(tag_path)
    assert r.ok
    assert json.loads(r.content) == tag

    # Add new tag and verify
    payload = json.dumps({'value': new_tag})
    r = as_admin.post(tags_path, data=payload)
    assert r.ok
    # Add a duplicate tag, returns 404
    payload = json.dumps({'value': new_tag})
    r = as_admin.post(tags_path, data=payload)
    assert r.status_code == 409
    r = as_admin.get(new_tag_path)
    assert r.ok
    assert json.loads(r.content) == new_tag

    # Add short tag and verify
    payload = json.dumps({'value': short_tag})
    r = as_admin.post(tags_path, data=payload)
    assert r.ok
    # Add too long tag and verify
    payload = json.dumps({'value': too_long_tag})
    r = as_admin.post(tags_path, data=payload)
    assert r.status_code == 400

    # Attempt to update tag, returns 404
    payload = json.dumps({'value': new_tag})
    r = as_admin.put(tag_path, data=payload)
    assert r.status_code == 404

    # Update existing tag to other_tag
    r = as_admin.get(other_tag_path)
    assert r.status_code == 404
    payload = json.dumps({'value': other_tag})
    r = as_admin.put(tag_path, data=payload)
    assert r.ok
    r = as_admin.get(other_tag_path)
    assert r.ok
    assert json.loads(r.content) == other_tag
    r = as_admin.get(tag_path)
    assert r.status_code == 404

    # Cleanup
    r = as_admin.delete(other_tag_path)  # url for 'DELETE' is the same as the one for 'GET'
    assert r.ok
    r = as_admin.get(other_tag_path)
    assert r.status_code == 404
    r = as_admin.delete(new_tag_path)  # url for 'DELETE' is the same as the one for 'GET'
    assert r.ok
    r = as_admin.get(new_tag_path)
    assert r.status_code == 404
    r = as_admin.delete(short_tag_path)  # url for 'DELETE' is the same as the one for 'GET'
    assert r.ok
    r = as_admin.get(short_tag_path)
    assert r.status_code == 404
