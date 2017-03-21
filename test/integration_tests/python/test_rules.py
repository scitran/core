import json
import logging

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)


def test_rule_access(as_user):
    r = as_user.get('/rules')
    assert r.status_code == 403

    r = as_user.post('/rules', json={'test': 'rule'})
    assert r.status_code == 403
