import logging
import time
import uuid

from webob.request import Request

from .. import config
from .. import util

AccessType = util.Enum('AccessType', {
    'view_container':   'view_container',
    'view_subject':     'view_subject',
    'view_file':        'view_file',
    'download_file':    'download_file',
    'delete_file':      'delete_file',
    'user_login':       'user_login',
    'user_logout':      'user_logout'
})

logging.basicConfig(
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    filename='user_access.log'
)
access_log = logging.getLogger('scitran.access')

class SciTranRequest(Request):
    """Extends webob.request.Request"""
    def __init__(self, *args, **kwargs):
        super(SciTranRequest, self).__init__(*args, **kwargs)
        self.id = "{random_chars}-{timestamp}".format(
            timestamp = str(int(time.time())),
            random_chars = str(uuid.uuid4().hex)[:8]
            )
        self.logger =  get_request_logger(self.id)

class RequestLoggerAdapter(logging.LoggerAdapter):
    """A LoggerAdapter to add request_id context"""
    def process(self, msg, kwargs):
        context_message =  "{0} request_id={1}".format(
            msg, self.extra['request_id']
            )
        return context_message, kwargs

def get_request_logger(request_id):
    """Given a request_id, produce a Logger or LoggerAdapter"""
    extra = {"request_id":request_id}
    logger = RequestLoggerAdapter(config.log, extra=extra)
    return logger


def log_access(access_type, cont_arg='cont_name', cont_id_arg='cid'):
    """
    A decorator to log a user or drone's access to an endpoint
    """
    def log_access_decorator(handler_method):
        def log_path_and_user(self, *args, **kwargs):
            result = handler_method(self, *args, **kwargs)
            access_log.warn('{} {} {} {}'.format(
                access_type,
                self.request.method,
                self.request.path,
                self.origin
            ))
            return result
        return log_path_and_user
    return log_access_decorator
