def test_search(as_public, as_drone, es):
    # try to access search w/o login
    r = as_public.post('/dataexplorer/search')
    assert r.status_code == 403

    # try to search w/o body
    r = as_drone.post('/dataexplorer/search')
    assert r.status_code == 400

    # try to search w/o return_type in body
    r = as_drone.post('/dataexplorer/search', json={})
    assert r.status_code == 400

    # try to search w/ invalid return_type
    r = as_drone.post('/dataexplorer/search', json={'return_type': 'test'})
    assert r.status_code == 400

    # try to search w/ invalid filters
    r = as_drone.post('/dataexplorer/search', json={'return_type': 'file', 'filters': 'test'})
    assert r.status_code == 400

    # session search against elastic mock
    cont_type, filter_key, filter_value, filter_range, search_str, results = 'session', 'key', 'value', 'range', 'search', 'results'
    es.search.return_value = {'aggregations': {'by_container': {'buckets': [
        {'by_top_hit': {'hits': {'hits': [results]}}},
    ]}}}
    # NOTE maybe sources would read better as module level constants in dataexplorerhandler?
    session_sources = [
        'permissions.*', 'session._id', 'session.label', 'session.created', 'session.timestamp', 'subject.code',
        'project.label', 'group.label', 'group._id', 'project._id', 'session.archived', 'project.archived']
    r = as_drone.post('/dataexplorer/search', json={'return_type': cont_type, 'search_string': search_str, 'filters': [
        {'terms': {filter_key: filter_value}},
        {'range': filter_range},
    ]})
    es.search.assert_called_with(
        body={
            'size': 0,
            'query': {'bool': {
                'must': {'match': {'_all': 'search'}},
                'filter': {'bool': {'must': [
                    {'terms': {filter_key + '.raw': filter_value}},
                    {'range': filter_range},
                    {'term': {'permissions._id': None}}
                ]}},
            }},
            'aggs': {'by_container': {'terms':
                {'field': cont_type + '._id', 'size': 100},
                'aggs': {'by_top_hit': {'top_hits': {
                    '_source': session_sources,
                    'size': 1
                }}}
            }}
        },
        doc_type='flywheel',
        index='data_explorer')
    assert r.status_code == 200
    assert r.json['results'] == [results]

    # acquisition search
    cont_type = 'acquisition'
    acquisition_sources = session_sources + ['acquisition._id', 'acquisition.label',
        'acquisition.created', 'acquisition.timestamp', 'acquisition.archived']
    r = as_drone.post('/dataexplorer/search', json={'return_type': cont_type, 'all_data': True})
    es.search.assert_called_with(
        body={
            'size': 0,
            'query': {'match_all': {}},
            'aggs': {'by_container': {'terms':
                {'field': cont_type + '._id', 'size': 100},
                'aggs': {'by_top_hit': {'top_hits': {
                    '_source': acquisition_sources,
                    'size': 1
                }}}
            }}
        },
        doc_type='flywheel',
        index='data_explorer')
    assert r.status_code == 200
    assert r.json['results'] == [results]

    # analysis search
    cont_type = 'analysis'
    analysis_sources = session_sources + ['analysis._id', 'analysis.label', 'analysis.created']
    r = as_drone.post('/dataexplorer/search', json={'return_type': cont_type, 'all_data': True})
    es.search.assert_called_with(
        body={
            'size': 0,
            'query': {'match_all': {}},
            'aggs': {'by_container': {'terms':
                {'field': cont_type + '._id', 'size': 100},
                'aggs': {'by_top_hit': {'top_hits': {
                    '_source': analysis_sources,
                    'size': 1
                }}}
            }}
        },
        doc_type='flywheel',
        index='data_explorer')
    assert r.status_code == 200
    assert r.json['results'] == [results]

    # file search
    cont_type = 'file'
    file_sources = [
        'permissions.*', 'session._id', 'session.label', 'session.created', 'session.timestamp',
        'subject.code', 'project.label', 'group.label', 'acquisition.label', 'acquisition._id',
        'group._id', 'project._id', 'analysis._id', 'analysis.label', 'session.archived',
        'acquisition.archived', 'project.archived', 'file.name', 'file.created', 'file.type',
        'file.measurements', 'file.size', 'parent']
    es.search.return_value = {'hits': {'hits': results}}
    r = as_drone.post('/dataexplorer/search', json={'return_type': cont_type, 'all_data': True})
    es.search.assert_called_with(
        body={
            '_source': file_sources,
            'query': {'bool': {'filter': {'bool': {'must': [{'term': {'container_type': cont_type}}]}}}},
            'size': 100},
        doc_type='flywheel',
        index='data_explorer')
    assert r.status_code == 200
    assert r.json['results'] == results

    # file search w/ search string and filter
    r = as_drone.post('/dataexplorer/search', json={'return_type': cont_type, 'all_data': True, 'search_string': search_str, 'filters': [
        {'terms': {filter_key: filter_value}},
        {'range': filter_range},
    ]})
    es.search.assert_called_with(
        body={
            '_source': file_sources,
            'query': {'bool': {
                'must': {'match': {'_all': search_str}},
                'filter': {'bool': {'must': [
                    {'term': {'container_type': cont_type}},
                    {'terms': {filter_key + '.raw': filter_value}},
                    {'range': filter_range},
                ]}}
            }},
            'size': 100},
        doc_type='flywheel',
        index='data_explorer')
    assert r.status_code == 200
    assert r.json['results'] == results
