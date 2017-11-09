import sys

import mock
import newrelic.api.exceptions
import pytest

import api.web.start


def test_newrelic(config, mocker):
    # set config var to trigger newrelic setup and setup mocks
    config['core']['newrelic'] = 'test'
    init = mocker.patch('newrelic.agent.initialize')
    log = mocker.patch('api.web.start.log')

    # successfully enable monitoring via newrelic
    api.web.start.app_factory()
    init.assert_called_with('test')
    log.info.assert_called_with('New Relic detected and loaded. Monitoring enabled.')

    # newrelic import error => log error and exit
    init.side_effect = ImportError
    with pytest.raises(SystemExit):
        api.web.start.app_factory()
    log.critical.assert_called_with('New Relic libraries not found.')
    init.side_effect = None

    # newrelic config error => log error and exit
    init.side_effect = newrelic.api.exceptions.ConfigurationError
    with pytest.raises(SystemExit):
        api.web.start.app_factory()
    log.critical.assert_called_with('New Relic detected, but configuration invalid.')
