import bson
import copy
import dateutil
import elasticsearch

from ..web import base
from .. import config
from ..auth import require_login, require_superuser

log = config.log
TEST_QUERY = {
  "query": {
    "filtered": {
      "query": {"match_all": {}},
      "filter": {
        "and": [
          {"term": {"dicom_header.SeriesDescription": "fmri"}},
          {
            "has_parent": {
              "type": "acquisition",
              "query": {
                "term": {"project.label": "neuro"}
              }
            }
          }
        ]
      }
    }
  }
}
MATCH_ALL= {"match_all": {}}

BASE_QUERY = {
  "query": {
    "filtered": {
      "query": MATCH_ALL,
      "filter": {
        "and": [
          {
            "has_parent": {
              "type": "acquisition"
            }
          }
        ]
      }
    }
  }
}

OLD_FACET_QUERY = {
    "size": 0,
    "aggs" : {
        "Series Description" : {
            "terms" : {
                "field" : "dicom_header.SeriesDescription_term",
                "size" : 5
            }
        },
        "Series Description Fragment" : {
            "terms" : {
                "field" : "dicom_header.SeriesDescription",
                "size" : 5
            }
        },
        "Patient Name" : {
            "terms" : {
                "field" : "dicom_header.PatientName_term",
                "size" : 5
            }
        },
        "Patient ID" : {
            "terms" : {
                "field" : "dicom_header.PatientID_term",
                "size" : 5
            }
        },
        "Modality" : {
            "terms" : {
                "field" : "dicom_header.Modality_term",
                "size" : 5
            }
        },
        "Study Date" : {
            "date_histogram" : {
                "field" : "dicom_header.StudyDate",
                "interval" : "day"
            }
        }
    }
}


FACET_QUERY = {
    "size": 0,
    "aggs" : {
        "session": {
            "filter": {"term": {"container_type": "session"}},
            "aggs": {
                "subect.sex" : {
                    "terms" : {
                        "field" : "subect.sex.raw",
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
                },
                "subject.age" : {
                    "histogram" : {
                        "field" : "subject.age",
                        "interval" : 31556952
                    }
                }
            }
        },
        "file": {
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
    def search(self):
        user_query = self.request.json_body.get('query')
        return self._run_query(self._construct_query(user_query))

    def _construct_query(self, user_query):
        es_query = copy.deepcopy(BASE_QUERY)
        and_block = es_query['query']['filtered']['filter']['and']
        parent_block = and_block[0]['has_parent']


        user_flywheel_query = user_query.get('flywheel')
        if user_flywheel_query:
            parent_block['query'] = {'term': user_flywheel_query}
        else:
            parent_block['filter'] = MATCH_ALL

        user_file_query = user_query.get('file')
        if user_file_query:
            log.debug('adding stuff')
            for k,v in user_file_query.iteritems():
                and_block.append({'term': {k: v}})
        log.debug(es_query)
        return es_query

    def _run_query(self, es_query):
        results = config.es.search(
            index='data_explorer',
            doc_type='file',
            body=es_query,
            size=10000
        )
        return { 'results': results['hits']['hits'], 'result_count': results['hits']['total']}

    def get_facets(self):
        results = config.es.search(
            index='data_explorer',
            doc_type='file',
            body=FACET_QUERY,
            size=10000
        )['aggregations']
        return {'facets': results}
