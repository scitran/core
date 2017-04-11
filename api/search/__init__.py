def es_query(input_query, doc_type, min_score=0.5, additional_filter=None, source_filter=None):
    """wrap the body to filter by doc_type
    as some full text queries seem to break in ElasticSearch
    if, instead, we pass the doc_type in the URL path."""
    es_filter = {
        'type': {
            'value': doc_type
        }
    }

    if additional_filter:
        es_filter = {
            'bool': {
                'must': [
                    es_filter,
                    additional_filter
                ]
            }
        }
    query = {
        'query': {
            'filtered': {
                'query': input_query,
                'filter': es_filter
            }
        },
        'min_score': min_score,
        '_source': {
            'exclude': ['*.info']
        }
    }

    if source_filter:
        query['_source'] = source_filter

    return query

def es_aggs(doc_type, field_name, additional_filter=None):
    query = {
      "size": 0,
      "aggs": {}
    }
    if additional_filter:
        query['query'] = {
          "filtered": {
            "query": {
              "match_all": {}
            },
            "filter": {
              "term": {
                additional_filter[0] : additional_filter[1]
              }
            }
          }
        }

    query['aggs'][field_name] = {
        "terms": {
            "field": '.'.join((doc_type, field_name, 'exact_'+field_name))
        }
    }

    return query

def add_filter_from_list(query, field, list_ids):
    filtered = {'query': query}
    filtered['filter'] = {
        'terms': {
            field: list_ids
        }
    }
    return {'filtered': filtered}

def add_filter(query, field, value):
    filtered = {'query': query}
    filtered['filter'] = {
        'term': {
            field: value
        }
    }
    return {'filtered': filtered}
