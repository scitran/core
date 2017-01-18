import datetime
import logging
import json
import time
import uuid

from webob.request import Request
from pymongo.errors import ServerSelectionTimeoutError

from .. import config
from .. import util
from ..dao.hierarchy import get_parent_tree
from .encoder import custom_json_serializer

AccessType = util.Enum('AccessType', {
    'view_container':   'view_container',
    'view_subject':     'view_subject',
    'view_file':        'view_file',
    'download_file':    'download_file',
    'delete_file':      'delete_file',
    'user_login':       'user_login',
    'user_logout':      'user_logout'
})

access_log  = logging.getLogger('scitran.access')
formatter   = logging.Formatter('%(message)s')
try:
    access_log_filename = config.get_item('core', 'access_log_path')
except ServerSelectionTimeoutError:
    access_log_filename = config.DEFAULT_CONFIG['core']['access_log_path']

handler     = logging.FileHandler(access_log_filename)

handler.setFormatter(formatter)
access_log.addHandler(handler)
access_log.setLevel(logging.INFO)

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


def log_access(access_type, cont_kwarg='cont_name', cont_id_kwarg='cid'):
    """
    A decorator to log a user or drone's access to an endpoint
    """
    def log_access_decorator(handler_method):
        def log_user_access(self, *args, **kwargs):
            result = handler_method(self, *args, **kwargs)

            cont_name = None
            cont_id = None

            if access_type not in [AccessType.user_login, AccessType.user_logout]:

                cont_name = kwargs.get(cont_kwarg)
                cont_id = kwargs.get(cont_id_kwarg)

                # Only log view_container events when the container is a session
                if access_type is AccessType.view_container and cont_name not in ['sessions', 'session']:
                    return result

            log_map = {
                'access_type':      access_type.value,
                'request_method':   self.request.method,
                'request_path':     self.request.path,
                'origin':           self.origin,
                'timestamp':        datetime.datetime.utcnow()
            }

            if access_type not in [AccessType.user_login, AccessType.user_logout]:

                # Create a context tree for the container
                context = {}

                if cont_name in ['collection', 'collections']:
                    context['collection'] = {'id': cont_id}
                else:
                    tree = get_parent_tree(cont_name, cont_id)

                    for k,v in tree.iteritems():
                        context[k] = {'id': v['_id'], 'label': v.get('label')}
                        if k == 'group':
                            context[k]['label'] = v.get('name')
                log_map['context'] = context

            access_log.info(json.dumps(log_map, sort_keys=True, default=custom_json_serializer))
            return result
        return log_user_access
    return log_access_decorator



