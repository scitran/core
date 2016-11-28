import datetime as dt

from .. import base
from .. import config
from .. import util
from ..dao import containerstorage, APIPermissionException, APINotFoundException
from ..types import Origin

log = config.log

Status = util.Enum('Origin', {
    'ok':       'ok',       # Device's last seen time is shorter than expected interval for checkin, no errors listed.
    'missing':  'missing',  # Device's last seen time is longer than the expected interval for checkin, but no errors listed.
    'error':    'error' ,   # Device has errors listed.
    'unknown':  'unknown'   # Device did not set an expected checkin interval.
})


def require_login(handler_method):
    """
    A decorator to ensure the request is not a public request.

    Accepts superuser and non-superuser requests.
    Accepts drone and user requests.
    """
    def check_login(self, *args, **kwargs):
        if self.public_request:
            raise APIPermissionException('Login required.')
        return handler_method(self, *args, **kwargs)
    return check_login

def require_superuser(handler_method):
    """
    A decorator to ensure the request is made as superuser.

    Accepts drone and user requests.
    """
    def check_superuser(self, *args, **kwargs):
        if not self.superuser_request:
            raise APIPermissionException('Superuser required.')
        return handler_method(self, *args, **kwargs)
    return check_superuser

def require_drone(handler_method):
    """
    A decorator to ensure the request is made as a drone.

    Will also ensure superuser, which is implied with a drone request.
    """
    def check_drone(self, *args, **kwargs):
        if self.origin.get('type', '') != Origin.device:
            raise APIPermissionException('Drone request required.')
        if not self.superuser_request:
            raise APIPermissionException('Superuser required.')
        return handler_method(self, *args, **kwargs)
    return check_drone


class DeviceHandler(base.RequestHandler):

    def __init__(self, request=None, response=None):
        super(DeviceHandler, self).__init__(request, response)
        self.storage = containerstorage.ContainerStorage('devices', use_object_id=False)

    @require_login
    def get(self, device_id):
        return self.storage.get_container(device_id)

    @require_login
    def get_all(self):
        return self.storage.get_all_el(None, None, None)

    @require_drone
    def get_self(self):
        device_id = self.origin.get('id', '')
        return self.storage.get_container(device_id)

    @require_drone
    def post(self):
        device_id = self.origin.get('id', '')
        payload = self.request.json_body
        #validators.validate_data(payload, 'device.json', 'input', 'PUT', optional=True)

        result = self.storage.update_el(device_id, payload)
        if result.modified_count == 1:
            return {'modified': result.modified_count}
        else:
            raise APINotFoundException('Device with id {} not found, state not updated'.format(device_id))

    @require_superuser
    def delete(self, device_id):
        raise NotImplementedError()

    def get_status(self):
        devices = self.storage.get_all_el(None, None, None)
        response = {}
        now = dt.datetime.now()
        for d in devices:
            d_obj = {}
            d_obj['last_seen'] =  d.get('last_seen')

            if d.get('errors'):
                d_obj['status'] = str(Status.error)
                d_obj['errors'] = d.get('errors')
                response[d.get('_id')] = d_obj.copy()

            elif not d.get('interval'):
                d_obj['status'] = str(Status.unknown)
                response[d.get('_id')] = d_obj.copy()

            elif (now-d.get('last_seen', now)).seconds > d.get('interval'):
                d_obj['status'] = str(Status.missing)
                response[d.get('_id')] = d_obj.copy()

            else:
                d_obj['status'] = str(Status.ok)
                response[d.get('_id')] = d_obj.copy()

        return response





