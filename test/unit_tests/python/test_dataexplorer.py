import copy
import json

import api.handlers.dataexplorerhandler as deh


def test_search(as_public, as_drone, es):
    # try to search w/o login
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
    # NOTE try: str(x) will never fail (probably not the desired behavior) at
    # * _parse_request#248
    # * search_fields#284
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


def test_get_facets(as_public, as_drone, es):
    # try to get facets w/o login
    r = as_public.post('/dataexplorer/facets')
    assert r.status_code == 403

    # get facets w/o sending body
    subject_age = 'test'
    es.search.return_value = {'aggregations': {
        'session_age': {'subject.age': subject_age},
        'by_session': {}}}
    r = as_drone.post('/dataexplorer/facets')
    body = copy.deepcopy(deh.FACET_QUERY)
    body.update({'query': {'match_all': {}}})
    es.search.assert_called_with(body=body, doc_type='flywheel', index='data_explorer')
    assert r.status_code == 200
    assert r.json == {'facets': {'by_session': {'subject.age': subject_age}}}


def test_search_fields(as_public, as_drone, es):
    # try to search fields w/o login
    r = as_public.post('/dataexplorer/search/fields')
    assert r.status_code == 403

    # search fields
    query_field, result_source = 'field', 'source'
    es.search.return_value = {'hits': {'hits': [{'_source': result_source}]}}
    r = as_drone.post('/dataexplorer/search/fields', json={'field': query_field})
    es.search.assert_called_with(
        body={'size': 20, 'query': {'match': {'name': query_field}}},
        doc_type='flywheel_field',
        index='data_explorer_fields')
    assert r.status_code == 200
    assert r.json == [result_source]


def test_index_fields(as_public, as_drone, es):
    # try to index fields w/o login
    r = as_public.post('/dataexplorer/index/fields')
    assert r.status_code == 403

    # setup es indices mock
    indices = set()
    def es_indices_exists(index): return index in indices
    def es_indices_create(index=None, body=None): indices.add(index)
    def es_indices_delete(index): indices.remove(index)
    es.indices.exists.side_effect = es_indices_exists

    # try to index fields before data_explorer index is available
    r = as_drone.post('/dataexplorer/index/fields')
    es.indices.exists.assert_called_with('data_explorer')
    assert r.status_code == 404
    indices.add('data_explorer')

    # try to (re)index data_explorer_fields w/ hard-reset=true (exc @ delete)
    indices.add('data_explorer_fields')
    es.indices.delete.side_effect = Exception('delete')
    r = as_drone.post('/dataexplorer/index/fields?hard-reset=true')
    es.indices.delete.assert_called_with(index='data_explorer_fields')
    assert r.status_code == 500
    es.indices.delete.side_effect = es_indices_delete

    # try to (re)index data_explorer_fields w/ hard-reset=true (exc @ create)
    es.indices.create.side_effect = Exception('create')
    r = as_drone.post('/dataexplorer/index/fields?hard-reset=true')
    es.indices.exists.assert_called_with('data_explorer_fields')
    assert es.indices.create.called
    assert r.status_code == 500
    es.indices.create.side_effect = es_indices_create

    # try to (re)index data_explorer_fields w/ hard-reset=true (exc @ get_mapping)
    es.indices.get_mapping.side_effect = Exception('get_mapping')
    r = as_drone.post('/dataexplorer/index/fields?hard-reset=true')
    assert r.status_code == 404
    es.indices.get_mapping.side_effect = None

    # (re)index data_explorer_fields w/ hard-reset=true
    r = as_drone.post('/dataexplorer/index/fields?hard-reset=true')
    es.indices.create.assert_called_with(index='data_explorer_fields', body={
        'settings': {'number_of_shards': 1, 'number_of_replicas': 0, 'analysis': deh.ANALYSIS},
        'mappings': {'_default_': {'_all': {'enabled' : True}, 'dynamic_templates': deh.DYNAMIC_TEMPLATES}, 'flywheel': {}}})
    assert r.status_code == 200

    # index data_explorer_fields - test ignored fields
    ignored_fields = ['_all', 'dynamic_templates', 'analysis_reference', 'file_reference', 'parent', 'container_type', 'origin', 'permissions', '_id']
    fields = {field: None for field in ignored_fields}
    es.indices.get_mapping.return_value = {'data_explorer': {'mappings': {'flywheel': {'properties': fields}}}}
    es.index.reset_mock()
    r = as_drone.post('/dataexplorer/index/fields')
    assert not es.indices.index.called
    assert r.status_code == 200

    # index data_explorer_fields - test type "flattening"
    type_map = {
        'string':  ['text', 'keyword'],
        'integer': ['long', 'integer', 'short', 'byte'],
        'float':   ['double', 'float'],
        'date':    ['date'],
        'boolean': ['boolean'],
        'object':  ['object'],
        None:      ['unrecognized'],
        # NOTE _get_field_type returns None for unrecognized field_types
    }
    type_map_r = {vi: k for k, v in type_map.iteritems() for vi in v}
    fields = {k + 'field': {'type': k} for k in type_map_r}
    es.indices.get_mapping.return_value = {'data_explorer': {'mappings': {'flywheel': {'properties': fields}}}}
    es.index.reset_mock()
    r = as_drone.post('/dataexplorer/index/fields')
    for field_name in fields:
        field_type = type_map_r[field_name.replace('field', '')]
        if field_type == 'object':
            continue
        es.index.assert_any_call(
            body=json.dumps({'name': field_name, 'type': field_type}),
            doc_type='flywheel_field',
            id=field_name,
            index='data_explorer_fields')
    assert r.status_code == 200

    # TODO index data_explorer_fields - test recursion
