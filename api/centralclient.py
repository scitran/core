"""
Client registers this instance with a central instance registery.

Client sends information about non-local users who are permitted
to access data in the local instance.  The local instance will
recieve information about other registered instances, and which of it's
local users are permitted to access data in other instances.
"""

import re
import json
import requests

from . import base
from . import config

import logging
import logging.config

# FIXME logging should be properly ported using the new config
logging.basicConfig()
log = logging.getLogger('scitran.api.centralclient')
logging.getLogger('urllib3').setLevel(logging.WARNING)  # silence Requests library logging

fail_count = 0


def update(db, api_uri, site_name, site_id, ssl_cert, central_url):
    """Send is-alive signal to central peer registry."""
    proj_userlist = [p['permissions'] for p in db.projects.find(None, {'_id': False, 'permissions._id': True, 'permissions.site': True})]
    col_userlist = [c['permissions'] for c in db.collections.find(None, {'_id': False, 'permissions._id': True, 'permissions.site': True})]
    grp_userlist = [g['roles'] for g in db.groups.find(None, {'_id': False, 'roles._id': True, 'roles.site': True})]
    # cannot hash on dictionary; temporarily use tuple
    remote_users = set([(user['_id'], user['site']) for container in proj_userlist+col_userlist+grp_userlist for user in container if user.get('site') is not None])
    remote_users = [{'user': user[0], 'site': user[1]} for user in remote_users]

    payload = json.dumps({'api_uri': api_uri, 'users': remote_users, 'name': site_name})
    route = '%s/%s/%s' % (central_url, 'instances', site_id)
    try:
        r = requests.put(route, data=payload, cert=ssl_cert)
    except requests.exceptions.ConnectionError:
        log.debug('SDMC is not reachable')
    else:
        if r.status_code == 200:
            # expecting
            # {'sites': [{'_id': 'foo.example.org', 'name': 'Example', 'api_uri': 'foo.example.org/api'},],
            #  'users': {'username1': [{'_id': 'site.hostname.edu', 'name': 'FooFooLand'}],
            #            }
            # }
            response = (json.loads(r.content))
            sites = response.get('sites')
            users = response.get('users')
            log.debug('recieved sites: %s ' % ', '.join(s['_id'] for s in sites))
            log.debug('recieved users: %s' % ', '.join([key for key in users]))
            if response.get('users'):
                for _id, remotes in response['users'].iteritems():
                    db.users.update_one({'_id': _id}, {'$set': {'remotes': remotes}})
            if sites:
                db.sites.delete_many({'_id': {'$nin': [site['_id'] for site in response['sites']]}})
                [db.sites.replace_one({'_id': site['_id']}, site, upsert=True) for site in sites]
                db.users.update_many(   # clean users who no longer have remotes
                        {'remotes': {'$exists': True}, '_id': {'$nin': users.keys()}},
                        {'$unset': {'remotes': ''}},
                        )
            log.info('%3d users with remote data, %3d remotes' % (
                    len([u['_id'] for u in db.users.find({'remotes': {'$exists': True}}, {'_id': True})]),
                    len([s['_id'] for s in db.sites.find({}, {'_id': True})])
                    ))
            return True
        else:
            # r.reason contains generic description for the specific error code
            # need the part of the error response body that contains the detailed explanation
            reason = re.search('<br /><br />\n(.*)\n\n\n </body>\n</html>', r.content)
            if reason:
                msg = reason.group(1)
            else:
                msg = r.reason
            log.warning('%s - %s' % (r.status_code, msg))
            return False


def clean_remotes(db, site_id):
    """Remove db.sites, and removes remotes field from all db.users."""
    log.debug('removing remotes from users, and remotes collection')
    db.sites.delete_many({'_id': {'$ne': [site_id]}})
    db.users.update_many({'remotes': {'$exists': True}}, {'$unset': {'remotes': ''}})


class CentralClient(base.RequestHandler):

    def sites(self):
        """
        .. http:get:: /api/sites

            Return local and remote sites.

            :statuscode 200: no error

            **Example request**:

            .. sourcecode:: http

                GET /api/sites HTTP/1.1
                Host: demo.flywheel.io
                Accept: */*


            **Example response**:

            .. sourcecode:: http

                HTTP/1.1 200 OK
                Vary: Accept-Encoding
                Content-Type: application/json; charset=utf-8
                [{"onload": true, "_id": "local", "name": "BaliDemo"}]


        """

        projection = ['name', 'onload']
        # TODO onload for local is true
        site_id = config.get_item('site', 'id')
        if self.public_request or self.is_true('all'):
            sites = list(config.db.sites.find(None, projection))
        else:
            # TODO onload based on user prefs
            remotes = (config.db.users.find_one({'_id': self.uid}, ['remotes']) or {}).get('remotes', [])
            remote_ids = [r['_id'] for r in remotes] + [site_id]
            sites = list(config.db.sites.find({'_id': {'$in': remote_ids}}, projection))
        for s in sites:  # TODO: this for loop will eventually move to public case
            if s['_id'] == site_id:
                s['onload'] = True
                break
        return sites

    def register(self):
        """
        .. http:post:: /api/register

            NOT IMPLEMENTED -- Return local and remote sites.

            :statuscode 404: Not Implemented
        """

        self.abort(404, 'register endpoint is not implemented')
        # FIXME the code below should be properly ported using the new config
        # every request to this route is aborted at the moment
        if not config.get_item('site', 'registered'):
            self.abort(400, 'Site not registered with central')
        if not config.get_item('site', 'ssl_cert'):
            self.abort(400, 'SSL cert not configured')
        if not config.get_item('site', 'central_url'):
            self.abort(400, 'Central URL not configured')
        if not update(config.db, config.get_item('site', 'ssl_cert'), config.get_item('site', 'central_url')):
            fail_count += 1
        else:
            centralclient.fail_count = 0
        if centralclient.fail_count == 3:
            log.warning('scitran central unreachable, purging all remotes info')
            clean_remotes(config.db)
