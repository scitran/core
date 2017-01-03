# vim: filetype=python
from api.web import start

application = start.app_factory()
