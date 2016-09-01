# vim: filetype=python
import sys
import os.path

from api import api

application = api.app_factory()
