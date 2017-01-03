import logging
import time
import uuid

from webob.request import Request

from .. import config

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
