import atexit
import json
import os
import traceback
import warnings

import webapp2


# Disable warnings for implicit tempfile.TemporaryDirectory cleanup
warnings.filterwarnings('ignore', message=r'Implicitly cleaning up <TemporaryDirectory')


# Enable code coverage for testing when API is started
# Start coverage before local module loading so their def and imports are counted
#   http://coverage.readthedocs.io/en/coverage-4.2/faq.html
if os.environ.get("SCITRAN_RUNTIME_COVERAGE") == "true": # pragma: no cover - oh, the irony
    def save_coverage(cov):
        print("Saving coverage")
        cov.stop()
        cov.save()

    def start_coverage():
        import coverage
        print("Enabling code coverage")
        cov = coverage.coverage(source=["api"], data_suffix="integration-tests")
        cov.start()
        atexit.register(save_coverage, cov)

    start_coverage()

# Enable collecting endpoints for checking documentation coverage
if os.environ.get("SCITRAN_COLLECT_ENDPOINTS") == "true": #pragma no cover
    ENDPOINTS = set()

    def save_endpoints():
        print('Saving endpoints')
        try:
            results = list(sorted(ENDPOINTS))
            with open('endpoints.json', 'w') as f:
                json.dump(results, f)

        except: #pylint: disable=bare-except
            print('Could not save endpoints.json: {0}'.format(traceback.format_exc()))

    def start_collecting_endpoints():
        print('Collecting endpoints...')
        atexit.register(save_endpoints)

    def collect_endpoint(request):
        ENDPOINTS.add('{0} {1}'.format(request.method, request.path))

    start_collecting_endpoints()

else:
    def collect_endpoint(request):
        #pylint: disable=unused-argument
        pass

from ..api import endpoints
from .. import config
from . import encoder
from .. import util
from .request import SciTranRequest

try:
    import uwsgi
except ImportError:
    uwsgi = None

log = config.log

def dispatcher(router, request, response):
    try:
        if uwsgi is not None:
            uwsgi.set_logvar('request_id', request.id)
    except: # pylint: disable=bare-except
        request.logger.error("Error setting request_id log var", exc_info=True)

    collect_endpoint(request)

    try:
        rv = router.default_dispatcher(request, response)
        if rv is not None:
            response.write(json.dumps(rv, default=encoder.custom_json_serializer))
            response.headers['Content-Type'] = 'application/json; charset=utf-8'
    except webapp2.HTTPException as e:
        util.send_json_http_exception(response, str(e), e.code, request.id)
    except Exception as e: # pylint: disable=broad-except
        request.logger.error("Error dispatching request", exc_info=True)
        if config.get_item('core', 'debug'):
            message = traceback.format_exc()
        else:
            message = 'Internal Server Error'
        util.send_json_http_exception(response, message, 500, request.id)

def app_factory(*_, **__):
    # pylint: disable=protected-access,unused-argument

    # don't use config.get_item() as we don't want to require the database at startup
    application = webapp2.WSGIApplication(endpoints, debug=config.__config['core']['debug'])
    application.router.set_dispatcher(dispatcher)
    application.request_class = SciTranRequest
    return application
