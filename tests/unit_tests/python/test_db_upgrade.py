import os.path
import sys

from mock import Mock, patch
import pytest

bin_path = os.path.join(os.getcwd(), "bin")
sys.path.insert(0, bin_path)
import db_upgrade

from api import config

CDV = db_upgrade.CURRENT_DATABASE_VERSION

def test_all_upgrade_scripts_exist():
    for i in range(1, CDV):
        script_name = 'upgrade_to_{}'.format(i)
        assert hasattr(db_upgrade, script_name)

def test_CDV_was_bumped():
    script_name = 'upgrade_to_{}'.format(CDV+1)
    assert hasattr(db_upgrade, script_name) is False


@patch('api.config.get_version', Mock(return_value={'database': 5}))
def test_get_db_version_from_config():
    assert db_upgrade.get_db_version() == 5


@pytest.fixture(scope='function')
def db_upgrade_mock_setup():
    setattr(config.db.singletons, 'update_one', Mock())
    for i in range(1, CDV):
        script_name = 'upgrade_to_{}'.format(i)
        setattr(db_upgrade, script_name, Mock())

@patch('db_upgrade.get_db_version', Mock(return_value=0))
def test_all_upgrade_scripts_ran(db_upgrade_mock_setup):
    with pytest.raises(SystemExit):
        db_upgrade.upgrade_schema()
    for i in range(1, CDV):
        script_name = 'upgrade_to_{}'.format(i)
        assert getattr(db_upgrade, script_name).called

@patch('db_upgrade.get_db_version', Mock(return_value=CDV-4))
def test_necessary_upgrade_scripts_ran(db_upgrade_mock_setup):
    with pytest.raises(SystemExit):
        db_upgrade.upgrade_schema()
    # Assert the necessary scripts were called
    for i in range(CDV-3, CDV):
        script_name = 'upgrade_to_{}'.format(i)
        assert getattr(db_upgrade, script_name).called
    # But not the scripts before it
    for i in range(1, CDV-4):
        script_name = 'upgrade_to_{}'.format(i)
        assert getattr(db_upgrade, script_name).called is False
