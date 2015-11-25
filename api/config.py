import datetime

import mongo

last_update = datetime.datetime.utcfromtimestamp(0)
latest_config = None


def _get_item(item):
    global last_update, latest_config
    now = datetime.datetime.utcnow()
    if now - last_update > datetime.timedelta(seconds=120) or latest_config is None:
        latest_config = mongo.db.config.find_one({'latest': True}) or {}
        last_update = now
    return latest_config.get(item)

def site_id():
    return _get_item('site_id')
