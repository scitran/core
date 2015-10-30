from api import validators
import logging
import nose.tools
log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)

class StubHandler:
    def abort(iself, code, message):
        err_m = str(code) + ' ' + message
        #log.error(err_m)
        raise Exception(err_m)

default_handler = StubHandler()

@nose.tools.raises(Exception)
def test_payload():
    payload = {
        'files': [],
        'group': 'unknown',
        'label': 'SciTran/Testing',
        'public': False,
        'permissions': [],
        'extra_params': 'testtest'
    }
    payload_validator = validators.payload_from_schema_file(default_handler, 'input/project.json')
    payload_validator(payload, 'POST')




