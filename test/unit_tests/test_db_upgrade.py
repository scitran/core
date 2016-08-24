import pytest

from mock import Mock, patch

from bin import database
from api import config

CDV = database.CURRENT_DATABASE_VERSION

def test_all_upgrade_scripts_exist():
    for i in range(1, CDV):
        script_name = 'upgrade_to_{}'.format(i)
        assert getattr(database, script_name) is not None

def test_CDV_was_bumped():
    script_name = 'upgrade_to_{}'.format(CDV+1)
    assert getattr(database, script_name) is None



def _version_obj(version):
    return {'database': version}

@patch('config.get_version', Mock(return_value=_version_obj(5)))
def test_get_db_version_from_config():
    assert database.get_db_version() == 5

@patch('config.get_version', Mock(return_value=None))
@patch('config.db.version.find_one', Mock(return_value=_version_obj(3)))
def test_get_db_version_from_db():
    assert database.get_db_version() == 3

@patch('config.get_version', Mock(return_value=None))
@patch('config.get_version', Mock(return_value=None))
def test_get_db_version_no_version():
    assert database.get_db_version() == 0

@patch('config.get_version', Mock(return_value=_version_obj(7)))
@patch('config.db.version.find_one', Mock(return_value=_version_obj(3)))
def test_get_db_version_uses_config():
    assert database.get_db_version() == 7



@pytest.fixture(scope='function')
def database_mock_setup():
    for i in range(1, CDV):
        script_name = 'upgrade_to_{}'.format(i)
        database[script_name] = Mock()

@patch('database.get_db_version', Mock(return_value=0))
def test_all_upgrade_scripts_ran(database_mock_setup):
    for i in range(1, CDV):
        script_name = 'upgrade_to_{}'.format(i)
        assert getattr(database, script_name).called

@patch('database.get_db_version', Mock(return_value=CDV-4))
def test_necessary_upgrade_scripts_ran(database_mock_setup):
    # Assert the necessary scripts were called
    for i in range(CDV-3, CDV):
        script_name = 'upgrade_to_{}'.format(i)
        assert getattr(database, script_name).called
    # But not the scripts before it
    for i in range(1, CDV-4):
        script_name = 'upgrade_to_{}'.format(i)
        assert getattr(database, script_name).called is False
