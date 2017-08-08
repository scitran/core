
def test_search_saving(as_admin, data_builder):

	# Try posting a malformed search
	r = as_admin.post('/savesearch', json={"not-label":"random-string"})
	assert r.status_code == 400

	# Try getting a non-existent saved search
	r = as_admin.get('/savesearch/000000000000000000000000')
	assert r.status_code == 404

	# Save a search
	r = as_admin.post('/savesearch', json={'label': 'search1', 'search': {'return_type': 'session'}})
	assert r.ok
	search = r.json()['_id']

	# Get all searched user has access to
	r = as_admin.get('/savesearch')
	assert r.ok

	# Get the saved search by id
	r = as_admin.get('/savesearch/' + search)
	assert r.ok
	assert r.json()['label'] == 'search1'

	# Malformed search replace
	payload = {'label': 'good-label', 'search' : { 'not-return-type' : 'not-container'}}
	r = as_admin.post('/savesearch/' + search, json=payload)
	assert r.status_code == 400

	# Replace search
	r = as_admin.get('/savesearch/' + search)
	assert r.ok
	assert r.json()['label'] == 'search1'
	payload = r.json()
	payload['label'] = 'newSearch'
	r = as_admin.post('/savesearch/' + search, json=payload)
	assert r.ok
	assert r.json()['_id'] == search
	r = as_admin.get('/savesearch/' + search)
	assert r.ok
	assert r.json()['label'] == 'newSearch'

	# Delete saved search
	r = as_admin.delete('/savesearch/' + search)
	assert r.ok
	r = as_admin.get('/savesearch')
	assert r.ok
	assert len(r.json()) == 0
