from api import files

def test_extension(as_drone):
    r = as_drone.post('/filetype', json={'_id': 'pdf', 'regex': '.*\.pdf$'})
    assert r.ok
    assert files.guess_type_from_filename('example.pdf') == 'pdf'

def test_multi_extension(as_drone):
    r = as_drone.post('/filetype',
                      json={'_id': 'archive',
                            'regex': '.*\.(zip$|tbz2$|tar\.gz$|tbz$|tar\.bz2$|tgz$|tar$|txz$|tar\.xz$)'})
    assert r.ok
    r = as_drone.post('/filetype', json={'_id': 'gephysio', 'regex': '.*\.gephysio\.zip$'})
    assert r.ok
    assert files.guess_type_from_filename('example.zip') == 'archive'
    assert files.guess_type_from_filename('example.gephysio.zip') == 'gephysio'

def test_nifti(as_drone):
    r = as_drone.post('/filetype', json={'_id': 'nifti', 'regex': '.*\.(nii\.gz$|nii$)'})
    assert r.ok
    assert files.guess_type_from_filename('example.nii') == 'nifti'
    assert files.guess_type_from_filename('example.nii.gz') == 'nifti'
    assert files.guess_type_from_filename('example.nii.x.gz') == None

def test_qa(as_drone):
    r = as_drone.post('/filetype', json={'_id': 'image', 'regex': '.*\.(jpg$|tif$|jpeg$|gif$|bmp$|png$|tiff$)'})
    assert r.ok
    r = as_drone.post('/filetype', json={'_id': 'qa', 'regex': '.*\.(qa\.png$|qa\.json|qa\.html$)'})
    assert r.ok
    assert files.guess_type_from_filename('example.png') == 'image'
    assert files.guess_type_from_filename('example.qa.png') == 'qa'
    assert files.guess_type_from_filename('example.qa') == None
    assert files.guess_type_from_filename('example.qa.png.unknown') == None

def test_unknown():
    assert files.guess_type_from_filename('example.unknown') == None

def test_get_insert_delete(as_drone):
    r = as_drone.get('/filetype')
    assert r.ok
    r = as_drone.post('/filetype', json={'_id': 'new', 'regex': '.*\.new$'})
    assert r.ok
    assert files.guess_type_from_filename('example.new') == 'new'
    r = as_drone.post('/filetype', json={'_id': 'new', 'regex': '.*\.new2$'})
    assert r.ok
    assert files.guess_type_from_filename('example.new') == None
    assert files.guess_type_from_filename('example.new2') == 'new'
    r = as_drone.delete('/filetype/new')
    assert r.ok

def test_insert_delete_abort(as_drone):
    r = as_drone.delete('/filetype/notexists')
    assert r.status_code == 404