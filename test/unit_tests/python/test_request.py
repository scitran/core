import unittest

import mock
from testfixtures import LogCapture

import api.web.request

class TestRequest(unittest.TestCase):
    def setUp(self):
        self.log_capture = LogCapture()
        self.request = api.web.request.SciTranRequest({})

    def tearDown(self):
        LogCapture.uninstall_all()

    def test_request_id(self):
        self.assertEqual(len(self.request.id), 19)

    def test_request_logger_adapter(self):
        test_log_message = "test log message"
        self.request.logger.error(test_log_message)
        expected_log_output = "{0} request_id={1}".format(
            test_log_message, self.request.id
        )
        self.log_capture.check(('scitran.api', 'ERROR', expected_log_output))
