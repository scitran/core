import bson
import copy
import dateutil
import elasticsearch

from ..web import base
from .. import config
from ..auth import require_login, require_superuser

log = config.log

MATCH_ALL= {"match_all": {}}

FACET_QUERY = {
    "size": 0,
    "aggs" : {
        "by_session": {
            "filter": {"term": {"container_type": "session"}},
            "aggs": {
                "subect.sex" : {
                    "terms" : {
                        "field" : "subject.sex.raw",
                        "size" : 15
                    }
                },
                "subject.code" : {
                    "terms" : {
                        "field" : "subject.code.raw",
                        "size" : 15
                    }
                },
                "session.tags" : {
                    "terms" : {
                        "field" : "session.tags.raw",
                        "size" : 15
                    }
                },
                "project.label" : {
                    "terms" : {
                        "field" : "project.label.raw",
                        "size" : 15
                    }
                },
                "session.timestamp" : {
                    "date_histogram" : {
                        "field" : "session.timestamp",
                        "interval" : "month"
                    }
                },
                "session.created" : {
                    "date_histogram" : {
                        "field" : "session.created",
                        "interval" : "month"
                    }
                }
            }
        },
        "session_age": {
            "filter": {
                "bool" : {
                  "must" : [
                     {"range": {"subject.age": {"gte": -31556952, "lte": 3155695200}}},
                     {"term": {"container_type": "session"}}
                  ]
                }
            },
            "aggs": {
                "subject.age" : {
                    "histogram" : {
                        "field" : "subject.age",
                        "interval" : 31556952,
                        "extended_bounds" : {
                            "min" : -31556952,
                            "max" : 3155695200
                        }
                    }
                }
            }
        },
        "by_file": {
            "filter": {"term": {"container_type": "file"}},
            "aggs": {

                "file.measurements" : {
                    "terms" : {
                        "field" : "file.measurements.raw",
                        "size" : 15
                    }
                },
                "file.type" : {
                    "terms" : {
                        "field" : "file.type.raw",
                        "size" : 15
                    }
                }
            }
        }
    }
}

EXAMPLE_SESSION_QUERY = {
  "size": 0,
  "query": {
    "match": {
      "_all": "test'"
    }
  },
  "aggs": {
    "by_session": {
      "terms": {
        "field": "session._id",
        "size": 100
      },
      "aggs": {
        "by_top_hit": {
          "top_hits": {
            "size": 1
          }
        }
      }
    }
  }
}

EXAMPLE_ACQUISITION_QUERY = {
  "size": 0,
  "query": {
    "match": {
      "_all": "megan'"
    }
  },
  "aggs": {
    "by_session": {
      "terms": {
        "field": "acquisition._id",
        "size": 100
      },
      "aggs": {
        "by_top_hit": {
          "top_hits": {
            "size": 1
          }
        }
      }
    }
  }
}

EXAMPLE_FILE_QUERY = {
  "size": 100,
  "query": {
    "bool": {
      "must": {
        "match": {
          "_all": "brain"
        }
      },
      "filter": {
        "bool" : {
          "must" : [
             { "term" : {"file.type" : "dicom"}},
             { "term" : {"container_type" : "file"}}
          ]
        }
      }
    }
  }
}


class DataExplorerHandler(base.RequestHandler):

    def __init__(self, request=None, response=None):
        super(DataExplorerHandler, self).__init__(request, response)

    @require_login
    def get_facets(self):
        aggs = config.es.search(
            index='data_explorer',
            doc_type='flywheel',
            body=FACET_QUERY
        )['aggregations']

        # This aggregation needs an extra filter to filter out outliers (only shows ages between -1 and 100)
        # Add it back in to the session aggregation node
        age_node = aggs.pop('session_age')
        aggs['by_session']['subject.age'] = age_node['subject.age']
        return {'facets': aggs}


    @require_login
    def search(self):
        request = self.request.json_body

        # Parse and validate return_type
        return_type = request.get('return_type')
        if not return_type or return_type not in ['file', 'session', 'acquisition']:
            self.abort(400, 'Must specify return type')

        # Parse and "validate" filters, allowed to be non-existent
        filters = request.get('filters', [])
        if type(filters) is not list:
            self.abort(400, 'filters must be a list')

        # Parse and "validate" search_string, allowed to be non-existent
        search_string = request.get('search_string', '')
        try:
            search_string = str(search_string)
        except Exception:
            self.abort(400, 'search_string must be of type string')

        return self._run_query(self._construct_query(return_type, search_string, filters), return_type)


    ## CONSTRUCTING QUERIES ##

    def _construct_query(self, return_type, search_string, filters):
        if return_type == 'file':
            return self._construct_file_query(search_string, filters)

        source = [ "permissions.*", "session._id", "session.label", "session.created", "session.timestamp", "subject.code", "project.label", "group.label" ]

        if return_type == 'acquisition':
            source.extend(["acquisition._id", "acquisition.label", "acquisition.created", "acquisition.timestamp"])

        query = {
            "size": 0,
            "query": {
                "bool": {
                  "must": {
                    "match": {
                      "_all": ""
                    }
                  },
                  "filter": {
                    "bool" : {
                      "must" : []
                    }
                  }
                }
            },
            "aggs": {
                "by_container": {
                    "terms": {
                        "field": return_type+"._id",
                        "size": 100
                    },
                    "aggs": {
                        "by_top_hit": {
                            "top_hits": {
                                "_source": source,
                                "size": 1
                            }
                        }
                    }
                }
            }
        }
        # Add search_string to "match on _all fields" query, otherwise remove unneeded logic
        if search_string:
            query['query']['bool']['must']['match']['_all'] = search_string
        else:
            query['query']['bool'].pop('must')

        # Add filters list to filter key on query if exists
        if filters:
            query['query']['bool']['filter']['bool']['must'].extend(filters)

        if not search_string and not filters:
            query['query'] = MATCH_ALL

        return query

    def _construct_file_query(self, search_string, filters):
        source = [ "permissions.*", "session._id", "session.label", "session.created", "session.timestamp", "subject.code", "project.label", "group.label" ]
        source.extend(["file.name", "file.created", "file.type", "file.measurements", "file.size"])
        query = {
          "size": 100,
          "_source": source,
          "query": {
            "bool": {
              "must": {
                "match": {
                  "_all": ""
                }
              },
              "filter": {
                "bool" : {
                  "must" : [{ "term" : {"container_type" : "file"}}]
                }
              }
            }
          }
        }

        # Add search_string to "match on _all fields" query, otherwise remove unneeded logic
        if search_string:
            query['query']['bool']['must']['match']['_all'] = search_string
        else:
            query['query']['bool'].pop('must')

        # Add filters list to filter key on query if exists
        if filters:
            query['query']['bool']['filter']['bool']['must'].extend(filters)

        return query


    ## RUNNING QUERIES AND PROCESSING RESULTS ##

    def _run_query(self, es_query, result_type):
        config.log.debug(es_query)
        results = config.es.search(
            index='data_explorer',
            doc_type='flywheel',
            body=es_query
        )
        return self._process_results(results, result_type)

    def _process_results(self, results, result_type):
        if result_type == 'file':
            return self._process_file_results(results)
        else:
            containers = results['aggregations']['by_container']['buckets']
            modified_results = []
            for c in containers:
                modified_results.append(c['by_top_hit']['hits']['hits'][0])
            return modified_results

    def _process_file_results(self, results):
        return results['hits']['hits']
