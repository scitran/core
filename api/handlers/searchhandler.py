import bson
import datetime
import elasticsearch

from .. import base
from .. import config

log = config.log

parent_container = {
    'acquisitions': 'sessions',
    'sessions': 'projects'
}

def _filter_body_by_type(body, doc_type, min_score):
    """wrap the body to filter by doc_type
    as some full text queries seem to break in ElasticSearch
    if, instead, we pass the doc_type in the URL path."""
    query = {
        'query': {
            'filtered': {
                'query': body,
                'filter': {
                    'type': {
                        'value': doc_type
                    }
                }
            }
        },
        'min_score': min_score
    }
    return query

class SearchHandler(base.RequestHandler):
    """This class allows to proxy queries to elasticsearch
    The get method just wraps the body in a convenient elasticsearch query.
    The get_datatree (for the special doc_type 'files') for each result build the datatree
    with the containers in their hierarchy.
    output example:
    [
        {
          "mimetype": "application/zip",
          "hash": "v0-sha384-8607a3c17008ff24d0cb9e1ccd60f5c7bcc1810b8c1dc9ee0f14ee91b7b1f897b78fcb035ff0135520a58bebfcdbd78b",
          "name": "8613_6_1_t1.zip",
          "project": {
            "group": "scitran",
            "created": "2016-03-08T22:46:01.941000+00:00",
            "modified": "2016-03-08T22:46:33.030000+00:00",
            "label": "Neuroscience",
            "_id": "56df5629b13d67a9cbfca1ea",
            "public": false
          },
          "session": {
            "group": "scitran",
            "created": "2016-03-08T22:46:16.221000+00:00",
            "modified": "2016-03-08T22:46:18.822000+00:00",
            "label": "1.2.840.113619.6.353.50113891957665820485497041858168751557",
            "project": "56df5629b13d67a9cbfca1ea",
            "_id": "56df5638b13d67a9cbfca1f7",
            "public": false,
            "subject": {
              "code": "ex8613"
            }
          },
          "container_name": "acquisitions",
          "type": "dicom",
          "acquisition": {
            "created": "2016-03-08T22:46:17.164000",
            "timestamp": "2015-01-07T17:38:09",
            "modified": "2016-03-08T22:46:17.164000",
            "label": "T1_high-res_inplane_Ret_knk",
            "instrument": "MRI",
            "session": "56df5638b13d67a9cbfca1f7",
            "measurement": "anatomical",
            "timezone": "America/Los_Angeles",
            "_id": "56df5639b13d67a9cbfca1f9",
            "public": false
          },
          "size": 3216386
        },
        ...
    ]
    """

    def __init__(self, request=None, response=None):
        super(SearchHandler, self).__init__(request, response)

    def get(self, cont_name=None, **kwargs):
        if self.public_request:
            self.abort(403, 'search is available only for authenticated users')
        size = self.get_param('size')
        min_score = self.get_param('min_score', 0.5)
        body = self.request.json_body
        query = _filter_body_by_type(body, cont_name, min_score)
        try:
            results = config.es.search(index='scitran', body=body, _source=['_id'], size=size or 10)
        except elasticsearch.exceptions.ConnectionError as e:
            self.abort(503, 'elasticsearch is not available')
        return results['hits']['hits']

    def get_datatree(self, **kwargs):
        if self.public_request:
            self.abort(403, 'search is available only for authenticated users')
        size = self.get_param('size')
        min_score = self.get_param('min_score', 0.5)
        body = self.request.json_body
        query = _filter_body_by_type(body, 'files', min_score)
        try:
            es_results = config.es.search(index='scitran', body=query, size=size or 10)
            ## elastic search results are wrapped in subkey ['hits']['hits']
            es_results = es_results['hits']['hits']
            results = []
            for result in es_results:
                # extract the source of the result
                result = result['_source']
                # add to the result the container hierarchy references
                container = result.pop('container')
                cont_name = result['container_name']
                result[cont_name[:-1]] = container
                while parent_container.get(cont_name):
                    parent_cont_name = parent_container[cont_name]
                    parent_id = bson.objectid.ObjectId(container[parent_cont_name[:-1]])
                    container = config.db[parent_cont_name].find_one({'_id': parent_id})
                    container.pop('permissions')
                    result[parent_cont_name[:-1]] = container
                    cont_name = parent_cont_name
                results.append(result)
        except elasticsearch.exceptions.ConnectionError as e:
            self.abort(503, 'elasticsearch is not available')
        return results

