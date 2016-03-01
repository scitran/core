import os
import json
import datetime

from .. import base
from .. import config

log = config.log

class SchemaHandler(base.RequestHandler):

    def __init__(self, request=None, response=None):
        super(SchemaHandler, self).__init__(request, response)

    def get(self, schema, **kwargs):
        schema_path = os.path.join(config.get_item('persistent', 'schema_path'), schema)
        try:
            with open(schema_path, 'ru') as f:
                return json.load(f)
        except IOError as e:
            self.abort(404, str(e))
