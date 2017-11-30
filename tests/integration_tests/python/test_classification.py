def test_modalities(data_builder, as_admin, as_user):

    payload = {
        '_id': 'MR',
        'classification': {
            'Intent': ["Structural", "Functional", "Localizer"],
            'Contrast': ["B0", "B1", "T1", "T2"]
        }
    }

    # test adding new modality
    r = as_admin.post('/modalities', json=payload)
    assert r.ok
    assert r.json()['_id'] == payload['_id']
    modality1 = payload['_id']

    # get specific modality
    r = as_user.get('/modalities/' + modality1)
    assert r.ok
    assert r.json() == payload

    # try replacing existing modality via POST
    r = as_admin.post('/modalities', json=payload)
    assert r.status_code == 409

    # list modalities as non-admin
    r = as_user.get('/modalities')
    assert r.ok
    modalities = r.json()
    assert len(modalities) == 1
    assert modalities[0]['_id'] == modality1

    # replace existing modality
    update = {
        'classification': {
            'Intent': ["new", "stuff"]
        }
    }
    r = as_admin.put('/modalities/' + modality1, json=update)
    assert r.ok
    r = as_admin.get('/modalities/' + modality1)
    assert r.ok
    assert r.json()['classification'] == update['classification']

    # try to replace missing modality
    r = as_admin.put('/modalities/' + 'madeup', json=update)
    assert r.status_code == 404

    # delete modality
    r = as_admin.delete('/modalities/' + modality1)
    assert r.ok

    # try to delete missing modality
    r = as_admin.delete('/modalities/' + modality1)
    assert r.status_code == 404





