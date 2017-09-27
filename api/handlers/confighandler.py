import json

from ..web import encoder
from ..web import base
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
        resp = config.get_version()
        if resp != None:
            return resp
        else:
            self.abort(404, "Version document does not exist")
