import bson
import elasticsearch

from ..web import base
from .. import config
from .. import util
from ..search import pathparser, queryprocessor, es_query, es_aggs
from ..dao import APINotFoundException

log = config.log

parent_container_dict = {
    'acquisitions': 'sessions',
    'sessions': 'projects',
    'projects': 'groups',
}

allowable_subject_agg_fields = [
    'code',
    'sex',
    'race',
    'ethnicity'
]

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
        self.search_containers = None
        self.cached_containers = {}

    def advanced_search(self):
        if self.public_request:
            self.abort(403, 'search is available only for authenticated users')
        queries = self.request.json_body
        path = queries.pop('path')
        all_data = self.is_true('all_data')
        limit = self.get_param('limit')
        if limit:
            try:
                limit = int(limit)
                if limit < 1:
                    raise ValueError
            except ValueError:
                self.abort(400, 'Limit must be int')
        # if the path starts with collections force the targets to exists within a collection
        if path.startswith('collections'):
            queries['collections'] = queries.get('collections', {"match_all": {}})
        target_paths = pathparser.PathParser(path).paths
        search = queryprocessor.PreparedSearch(target_paths, queries, all_data, self.uid)
        results = search.process_search()
        self.search_containers = search.search_containers
        for result_type, results_for_type in results.iteritems():
            if limit and len(results_for_type) > limit:
                results[result_type] = results_for_type = results_for_type[:limit]
            for result in results_for_type:
                self._augment_result(result, result_type)
        return results

    def _augment_result(self, result, result_type):
        # Check if result type is special leaf node
        if result_type in ['files', 'notes', 'analyses']:

            # Find leaf's parent container, add to source as parent_container_name: parent_container
            container = result['_source'].pop('container')
            container_name = result['_source']['container_name']
            result['_source'][container_name[:-1]] = container

            # Get user_permissions results from strip, add to leaf
            self._strip_other_permissions(container, container_name)
            result['_source']['user_permissions'] = container.get('user_permissions')

        else:
            container = result['_source']
            container_name = result_type
        result['_source'].update(self._get_parents(container, container_name))


    def _strip_other_permissions(self, container, cont_name):
        perm_list = container.pop('roles', None) if cont_name == 'groups' else container.pop('permissions', None)
        if perm_list:
            p = util.user_perm(perm_list, self.uid)
            container['user_permissions'] = p.get('access', None) if p else None

    def _get_parents(self, container, cont_name):
        parents = {}
        self._strip_other_permissions(container, cont_name)
        if parent_container_dict.get(cont_name) is None:
            return parents
        parent_name = parent_container_dict[cont_name]
        parent_id = container[parent_name[:-1]]
        parent_results = self.search_containers[parent_name].results
        parent_container = None
        if parent_results is not None:
            parent_container = parent_results.get(parent_id, {}).get('_source')
        if parent_container is None:
            try:
                parent_container = self._get_container_from_es(parent_name, parent_id)
            except APINotFoundException:
                # if the parent is missing we return only the _id
                # and we stop the recursion
                parents[parent_name[:-1]] = {'_id': parent_id}
                return parents
        parents[parent_name[:-1]] = parent_container
        parents.update(self._get_parents(parent_container, parent_name))
        return parents

    def _get_container_from_es(self, container_name, container_id):
        if self.cached_containers.get((container_name, container_id)):
            return self.cached_containers[(container_name, container_id)]
        search = {"query":
            {"filtered":
                {"filter":
                    {"ids":
                      {"values": [container_id]}
                    }
                }
            }
        }
        try:
            es_results = config.es.search(# pylint: disable=unexpected-keyword-arg
                index='scitran',
                doc_type=container_name,
                body=search,
                size=2)
            es_results = es_results['hits']['hits']
            if len(es_results) > 1:
                self.abort(500,
                    """More than one result returned by ElasticSearch for container:
                    container_id: {}
                    container_name: {}
                    """.format(container_id, container_name))
            elif len(es_results) == 0:
                raise APINotFoundException(
                    """Zero result returned by ElasticSearch for container:
                    container_id: {}
                    container_name: {}
                    """.format(container_id, container_name))
            else:
                result = es_results[0]['_source']
                self.cached_containers[(container_name, container_id)] = result
                return result
        except elasticsearch.exceptions.ConnectionError:
            raise APINotFoundException('elasticsearch is not available')

    def get_datatree(self):
        if self.public_request:
            self.abort(403, 'search is available only for authenticated users')
        size = self.get_param('size')
        min_score = self.get_param('min_score', 0.5)
        body = self.request.json_body
        collection = self.get_param('collection')
        additional_filter = None
        if collection:
            collection = config.db.collections.find_one({'label': collection})
            if not collection:
                self.abort(404, 'collection not found')
            acquisitions = config.db.acquisitions.find({'collections': collection['_id']})
            acq_ids = [str(a['_id']) for a in acquisitions]
            additional_filter = {
                'terms': {
                    'container_id': acq_ids
                }
            }
        query = es_query(body, 'files', min_score, additional_filter)
        try:
            # pylint disable can be removed after PyCQA/pylint#258 is fixed
            es_results = config.es.search(index='scitran', body=query, size=size or 10) # pylint: disable=unexpected-keyword-arg
            ## elastic search results are wrapped in subkey ['hits']['hits']
            es_results = es_results['hits']['hits']
            results = []
            for result in es_results:
                # extract the source of the result
                result = result['_source']
                # add to the result the container hierarchy references
                cont_id = bson.objectid.ObjectId(result.pop('container_id'))
                cont_name = result['container_name']
                container = config.db[cont_name].find_one({'_id': cont_id})
                result[cont_name[:-1]] = container
                while parent_container_dict.get(cont_name):
                    parent_cont_name = parent_container_dict[cont_name]
                    parent_id = container[parent_cont_name[:-1]]
                    if parent_cont_name != 'groups':
                        parent_id = bson.objectid.ObjectId(parent_id)
                    container = config.db[parent_cont_name].find_one({'_id': parent_id})
                    self._strip_other_permissions(container, parent_cont_name)
                    result[parent_cont_name[:-1]] = container
                    cont_name = parent_cont_name
                if collection:
                    result['collection'] = collection
                results.append(result)
        except elasticsearch.exceptions.ConnectionError:
            self.abort(503, 'elasticsearch is not available')
        return results

    def get_terms_for_field(self):
        if self.public_request:
            self.abort(403, 'search is available only for authenticated users.')
        all_data = self.is_true('all_data')
        payload = self.request.json_body
        doc_type = payload.get('doc_type')
        field_name = payload.get('field')

        # Confirm reasonable inputs
        # Policy descisions around potential PHI:
        #   - term lists for info fields are not allowed
        #   - subject fields with term lists are whitelisted

        if not doc_type or not field_name:
            self.abort(400, 'Must supply doc_type and field name.')
        if field_name.startswith('info'):
            self.abort(400, 'Terms for info fields are not supported.')
        if doc_type == 'subject' and field_name not in allowable_subject_agg_fields:
            self.abort(400, 'Allowable fields for subject doc_type: {}.'.format(allowable_subject_agg_fields))

        # Add filters for permissions if not aggregating all data:
        additional_filter = None
        if not all_data:
            additional_filter = ('permissions._id', self.uid)

        query = es_aggs(doc_type, field_name, additional_filter=additional_filter)

        try:
            # pylint disable can be removed after PyCQA/pylint#258 is fixed
            es_results = config.es.search(index='scitran', body=query) # pylint: disable=unexpected-keyword-arg
            ## elastic search results are wrapped in subkey ['hits']['hits']
            results = es_results['aggregations'][field_name]['buckets']
        except elasticsearch.exceptions.ConnectionError:
            self.abort(503, 'elasticsearch is not available')
        return results
