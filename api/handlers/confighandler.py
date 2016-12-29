import json

from .. import encoder
from .. import base
from .. import config

class Config(base.RequestHandler):

    def get(self):
        """Return public Scitran configuration information."""
        return config.get_public_config()

    def get_js(self):
        """Return scitran config in javascript format."""
        self.response.write(
            'config = ' +
            json.dumps( self.get(), sort_keys=True, indent=4, separators=(',', ': '), default=encoder.custom_json_serializer,) +
            ';'
        )

class Version(base.RequestHandler):

    def get(self):
        """Return database schema version"""
        return config.get_version()
