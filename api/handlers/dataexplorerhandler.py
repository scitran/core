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
            "size": 15
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
            "size": 15
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

        return self._run_query(self._construct_query(return_type, search_string, filters))

    def _construct_query(self, return_type, search_string, filters):
        if return_type == 'file':
            return self._construct_file_query(search_string, filters)
        else:
            return {}

    def _construct_file_query(self, search_string, filters):
        return {}

    def _run_query(self, es_query):
        results = config.es.search(
            index='data_explorer',
            doc_type='file',
            body=es_query,
            size=10000
        )
        return { 'results': results['hits']['hits'], 'result_count': results['hits']['total']}
