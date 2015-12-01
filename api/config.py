import logging
import datetime

from . import mongo


logging.basicConfig(
    format='%(asctime)s %(name)16.16s %(filename)24.24s %(lineno)5d:%(levelname)4.4s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG,
)
log = logging.getLogger('scitran.api')

logging.getLogger('MARKDOWN').setLevel(logging.WARNING) # silence Markdown library
logging.getLogger('requests').setLevel(logging.WARNING) # silence Requests library


def set_log_level(log, level):
    log.setLevel(getattr(logging, level.upper()))


last_config_update = datetime.datetime.utcfromtimestamp(0)
latest_config = None

def get_config():
    return mongo.db.config.find_one({'latest': True}, {'_id': 0, 'latest': 0}) or {}

def _get_item(item):
    global last_config_update, latest_config
    now = datetime.datetime.utcnow()
    if now - last_config_update > datetime.timedelta(seconds=120) or latest_config is None:
        latest_config = mongo.db.config.find_one({'latest': True}) or {}
        last_config_update = now
    return latest_config.get(item)

def site_id():
    return _get_item('site_id')
