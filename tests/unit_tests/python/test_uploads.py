import requests_mock


def test_signed_url_upload(as_drone, mocker):

    metadata = {
        "metadata": {
                "group": {"_id": "scitran"},
                "project": {"label": ""},
                "session": {
                "uid": "session_uid",
                "subject": {"code": "bela"}
            },
            "acquisition": {
                "uid": "acquisition_uid",
                "files": [{"name": "test"}]
            }
        },
        "filename": "test"
    }

    r = as_drone.post('/upload/signed/reaper?ticket=',
                     json=metadata)

    assert r.status_code == 405

    mock_fs = mocker.patch('api.upload.config.fs')
    mock_fs.get_signed_url.return_value = 'url'
    r = as_drone.post('/upload/signed/reaper?ticket=',
                      json=metadata)

    assert r.ok
    assert r.json['upload_url'] == 'url'

    ticket_id = r.json['ticket']

    r = as_drone.post('/upload/signed/reaper?ticket=' + ticket_id)
    assert r.ok

    assert mock_fs.move.called