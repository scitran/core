import os
import copy
import json
import pytz
import webapp2
import datetime
import bson.objectid
import webapp2_extras.routes

from . import core
from . import jobs
from . import users
from handlers import listhandler

from . import api2


routes = [
    webapp2.Route(r'/api',                                          core.Core),
    webapp2_extras.routes.PathPrefixRoute(r'/api', [
        webapp2.Route(r'/download',                                 core.Core, handler_method='download', methods=['GET', 'POST'], name='download'),
        webapp2.Route(r'/upload',                                   core.Core, handler_method='upload', methods=['POST']),
        webapp2.Route(r'/reaper',                                   core.Core, handler_method='reaper', methods=['POST']),
        webapp2.Route(r'/sites',                                    core.Core, handler_method='sites', methods=['GET']),
        webapp2.Route(r'/search',                                   core.Core, handler_method='search', methods=['GET', 'POST']),
    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/api/schema', [
        webapp2.Route(r'/group',                                    users.Group, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/user',                                     users.User, handler_method='schema', methods=['GET']),
    ]),
    webapp2.Route(r'/api/users',                                    users.Users),
    webapp2_extras.routes.PathPrefixRoute(r'/api/users', [
        webapp2.Route(r'/self',                                     users.User, handler_method='self', methods=['GET']),
        webapp2.Route(r'/roles',                                    users.User, handler_method='roles', methods=['GET']),
        webapp2.Route(r'/<:[^/]+>',                                 users.User, name='user'),
        webapp2.Route(r'/<:[^/]+>/groups',                          users.Groups, name='groups'),
    ]),
    webapp2.Route(r'/api/groups',                                   users.Groups),
    webapp2_extras.routes.PathPrefixRoute(r'/api/groups', [
        webapp2.Route(r'/<:[^/]+>',                                 users.Group, name='group'),
        webapp2.Route(r'/<cid:[^/]+>/roles',                        listhandler.ListHandler, name='g_roles', methods=['POST'], defaults={'coll_name': 'groups', 'list_name': 'roles'}),
        webapp2.Route(r'/<cid:[^/]+>/roles/<site:[^/]+>/<_id:[^/]+>',
                                                                    listhandler.ListHandler, name='g_roles', methods=['GET', 'PUT', 'DELETE'], defaults={'coll_name': 'groups', 'list_name': 'roles'}),
    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/api/projects', [
        webapp2.Route(r'/<cid:[^/]+>/file',                         listhandler.FileListHandler, name='pj_files_post', methods=['POST'], defaults={'coll_name': 'projects', 'list_name': 'files'}),
        webapp2.Route(r'/<cid:[^/]+>/file/<filename:[^/]+>',        listhandler.FileListHandler, name='pj_files', defaults={'coll_name': 'projects', 'list_name': 'files'}),
        webapp2.Route(r'/<cid:[^/]+>/tags',                         listhandler.ListHandler, methods=['POST'], name='pj_tags', defaults={'coll_name': 'projects', 'list_name': 'tags'}),
        webapp2.Route(r'/<cid:[^/]+>/tags/<value:[^/]+>',           listhandler.ListHandler, name='pj_tags', defaults={'coll_name': 'projects', 'list_name': 'tags'}),
        webapp2.Route(r'/<cid:[^/]+>/permissions',                  listhandler.ListHandler, name='pj_perms_post', methods=['POST'], defaults={'coll_name': 'projects', 'list_name': 'permissions'}),
        webapp2.Route(
            r'/<cid:[^/]+>/permissions/<site:[^/]+>/<_id:[^/]+>',   listhandler.ListHandler, name='pj_perms', defaults={'coll_name': 'projects', 'list_name': 'permissions'}),
        webapp2.Route(r'/<cid:[^/]+>/notes',                        listhandler.NotesListHandler, name='pj_notes_post', methods=['POST'], defaults={'coll_name': 'projects', 'list_name': 'notes'}),
        webapp2.Route(r'/<cid:[^/]+>/notes/<_id:[^/]+>',            listhandler.NotesListHandler, name='pj_notes', defaults={'coll_name': 'projects', 'list_name': 'notes'}),

    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/api/collections', [
        webapp2.Route(r'/<cid:[^/]+>/file',                         listhandler.FileListHandler, name='cl_files_post', methods=['POST'], defaults={'coll_name': 'collections', 'list_name': 'files'}),
        webapp2.Route(r'/<cid:[^/]+>/file/<filename:[^/]+>',        listhandler.FileListHandler, name='cl_files', defaults={'coll_name': 'collections', 'list_name': 'files'}),
        webapp2.Route(r'/<cid:[^/]+>/tags/<value:[^/]+>',           listhandler.ListHandler, name='cl_tags', defaults={'coll_name': 'collections', 'list_name': 'tags'}),
        webapp2.Route(r'/<cid:[^/]+>/permissions',                  listhandler.ListHandler, name='cl_perms_post', methods=['POST'], defaults={'coll_name': 'collections', 'list_name': 'permissions'}),
        webapp2.Route(
            r'/<cid:[^/]+>/permissions/<site:[^/]+>/<_id:[^/]+>',   listhandler.ListHandler, name='cl_perms', defaults={'coll_name': 'collections', 'list_name': 'permissions'}),
        webapp2.Route(r'/<cid:[^/]+>/notes',                        listhandler.NotesListHandler, name='cl_notes_post', methods=['POST'], defaults={'coll_name': 'collections', 'list_name': 'notes'}),
        webapp2.Route(r'/<cid:[^/]+>/notes/<_id:[^/]+>',            listhandler.NotesListHandler, name='cl_notes', defaults={'coll_name': 'collections', 'list_name': 'notes'}),
    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/api/sessions', [
        webapp2.Route(r'/<cid:[^/]+>/file',                         listhandler.FileListHandler, name='se_files_post', methods=['POST'], defaults={'coll_name': 'sessions', 'list_name': 'files'}),
        webapp2.Route(r'/<cid:[^/]+>/file/<filename:[^/]+>',        listhandler.FileListHandler, name='se_files', defaults={'coll_name': 'sessions', 'list_name': 'files'}),
        webapp2.Route(r'/<cid:[^/]+>/tags/<value:[^/]+>',           listhandler.ListHandler, name='se_tags', defaults={'coll_name': 'sessions', 'list_name': 'tags'}),
        webapp2.Route(r'/<cid:[^/]+>/permissions',                  listhandler.ListHandler, name='se_perms_post', methods=['POST'], defaults={'coll_name': 'sessions', 'list_name': 'permissions'}),
        webapp2.Route(
            r'/<cid:[^/]+>/permissions/<site:[^/]+>/<_id:[^/]+>',   listhandler.ListHandler, name='se_perms', defaults={'coll_name': 'sessions', 'list_name': 'permissions'}),
        webapp2.Route(r'/<cid:[^/]+>/notes',                        listhandler.NotesListHandler, name='se_notes_post', methods=['POST'], defaults={'coll_name': 'sessions', 'list_name': 'notes'}),
        webapp2.Route(r'/<cid:[^/]+>/notes/<_id:[^/]+>',            listhandler.NotesListHandler, name='se_notes', defaults={'coll_name': 'sessions', 'list_name': 'notes'}),
    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/api/acquisitions', [
        webapp2.Route(r'/<cid:[^/]+>/tags/<value:[^/]+>',           listhandler.ListHandler, name='aq_tags', defaults={'coll_name': 'acquisitions', 'list_name': 'tags'}),
        webapp2.Route(r'/<cid:[^/]+>/file',                         listhandler.FileListHandler, name='aq_files_post', methods=['POST'], defaults={'coll_name': 'acquisitions', 'list_name': 'files'}),
        webapp2.Route(r'/<cid:[^/]+>/file/<filename:[^/]+>',        listhandler.FileListHandler, name='aq_files', defaults={'coll_name': 'acquisitions', 'list_name': 'files'}),
        webapp2.Route(r'/<cid:[^/]+>/permissions',                  listhandler.ListHandler, name='aq_perms_post', methods=['POST'], defaults={'coll_name': 'acquisitions', 'list_name': 'permissions'}),
        webapp2.Route(
            r'/<cid:[^/]+>/permissions/<site:[^/]+>/<_id:[^/]+>',   listhandler.ListHandler, name='aq_perms', defaults={'coll_name': 'acquisitions', 'list_name': 'permissions'}),
        webapp2.Route(r'/<cid:[^/]+>/notes',                        listhandler.NotesListHandler, name='aq_notes_post', methods=['POST'], defaults={'coll_name': 'acquisitions', 'list_name': 'notes'}),
        webapp2.Route(r'/<cid:[^/]+>/notes/<_id:[^/]+>',            listhandler.NotesListHandler, name='aq_notes', defaults={'coll_name': 'acquisitions', 'list_name': 'notes'}),
    ]),
    webapp2.Route(r'/api/jobs',                                     jobs.Jobs),
    webapp2_extras.routes.PathPrefixRoute(r'/api/jobs', [
        webapp2.Route(r'/next',                                     jobs.Jobs, handler_method='next', methods=['GET']),
        webapp2.Route(r'/count',                                    jobs.Jobs, handler_method='count', methods=['GET']),
        webapp2.Route(r'/addTestJob',                               jobs.Jobs, handler_method='addTestJob', methods=['GET']),
        webapp2.Route(r'/<:[^/]+>',                                 jobs.Job,  name='job'),
    ]),
]

routes.extend(api2.routes)

with open(os.path.join(os.path.dirname(__file__), 'schema.json')) as fp:
    schema_dict = json.load(fp)
for cls in [
        users.Group,
        users.User,
        ]:
    cls.post_schema = copy.deepcopy(schema_dict[cls.__name__.lower()])
    cls.put_schema = copy.deepcopy(cls.post_schema)
    cls.put_schema['properties'].pop('_id', None)
    cls.put_schema.pop('required')


def custom_json_serializer(obj):
    if isinstance(obj, bson.objectid.ObjectId):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return pytz.timezone('UTC').localize(obj).isoformat()
    raise TypeError(repr(obj) + " is not JSON serializable")


def dispatcher(router, request, response):
    rv = router.default_dispatcher(request, response)
    if rv is not None:
        response.write(json.dumps(rv, default=custom_json_serializer))
        response.headers['Content-Type'] = 'application/json; charset=utf-8'


try:
    import newrelic.agent
    app = newrelic.agent.WSGIApplicationWrapper(webapp2.WSGIApplication(routes))
except ImportError:
    app = webapp2.WSGIApplication(routes)

app.router.set_dispatcher(dispatcher)
