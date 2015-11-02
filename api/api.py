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
from handlers import listhandler
from handlers import userhandler
from handlers import grouphandler
from handlers import containerhandler
from handlers import collectionshandler

routes = [
    webapp2.Route(r'/api',                                          core.Core),
    webapp2_extras.routes.PathPrefixRoute(r'/api', [
        webapp2.Route(r'/download',                                 core.Core, handler_method='download', methods=['GET', 'POST'], name='download'),
        webapp2.Route(r'/reaper',                                   core.Core, handler_method='reaper', methods=['POST']),
        webapp2.Route(r'/sites',                                    core.Core, handler_method='sites', methods=['GET'])
    ]),
    webapp2.Route(r'/api/users',                                    userhandler.UserHandler, handler_method='get_all', methods=['GET']),
    webapp2.Route(r'/api/users',                                    userhandler.UserHandler, methods=['POST']),
    webapp2_extras.routes.PathPrefixRoute(r'/api/users', [
        webapp2.Route(r'/self',                                     userhandler.UserHandler, handler_method='self', methods=['GET']),
        webapp2.Route(r'/roles',                                    userhandler.UserHandler, handler_method='roles', methods=['GET']),
        webapp2.Route(r'/<_id:[^/]+>',                              userhandler.UserHandler, name='user'),
        webapp2.Route(r'/<uid:[^/]+>/groups',                       grouphandler.GroupHandler, handler_method='get_all', methods=['GET'], name='groups'),
    ]),
    webapp2.Route(r'/api/groups',                                   grouphandler.GroupHandler, handler_method='get_all', methods=['GET']),
    webapp2.Route(r'/api/groups',                                   grouphandler.GroupHandler, methods=['POST']),
    webapp2_extras.routes.PathPrefixRoute(r'/api/groups', [
        webapp2.Route(r'/<_id:[^/]+>',                              grouphandler.GroupHandler, name='group'),
        webapp2.Route(r'/<cid:[^/]+>/roles',                        listhandler.ListHandler, name='g_roles', methods=['POST'], defaults={'coll_name': 'groups', 'list_name': 'roles'}),
        webapp2.Route(r'/<cid:[^/]+>/roles/<site:[^/]+>/<_id:[^/]+>',
                                                                    listhandler.ListHandler, name='g_roles', methods=['GET', 'PUT', 'DELETE'], defaults={'coll_name': 'groups', 'list_name': 'roles'}),
    ]),
    webapp2.Route(r'/api/jobs',                                     jobs.Jobs),
    webapp2_extras.routes.PathPrefixRoute(r'/api/jobs', [
        webapp2.Route(r'/next',                                     jobs.Jobs, handler_method='next', methods=['GET']),
        webapp2.Route(r'/count',                                    jobs.Jobs, handler_method='count', methods=['GET']),
        webapp2.Route(r'/addTestJob',                               jobs.Jobs, handler_method='addTestJob', methods=['GET']),
        webapp2.Route(r'/<:[^/]+>',                                 jobs.Job,  name='job'),
    ]),
    webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/tags',                       listhandler.ListHandler, methods=['POST'], name='tags_post', defaults={'list_name': 'tags'}),
    webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/tags/<value:[^/]+>',         listhandler.ListHandler, name='tags', defaults={'list_name': 'tags'}),
    webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/file',                       listhandler.FileListHandler, name='files_post', methods=['POST'], defaults={'list_name': 'files'}),
    webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/file/<filename:[^/]+>',      listhandler.FileListHandler, name='files', defaults={'list_name': 'files'}),
    webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/permissions',                listhandler.ListHandler, name='perms_post', methods=['POST'], defaults={'list_name': 'permissions'}),
    webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/permissions/<site:[^/]+>/<_id:[^/]+>',
                                                                                    listhandler.ListHandler, name='perms', defaults={ 'list_name': 'permissions'}),
    webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/notes',                      listhandler.NotesListHandler, name='notes_post', methods=['POST'], defaults={'list_name': 'notes'}),
    webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/notes/<_id:[^/]+>',          listhandler.NotesListHandler, name='notes', defaults={'list_name': 'notes'}),

    webapp2.Route(r'/api/<coll_name:[^/]+>/api/collections/curators',               collectionshandler.CollectionsHandler, handler_method='curators', methods=['GET']),

    webapp2.Route(r'/api/<coll_name:collections>',                                  collectionshandler.CollectionsHandler, name='colls', handler_method='get_all', methods=['GET']),
    webapp2.Route(r'/api/<coll_name:collections>',                                  collectionshandler.CollectionsHandler, methods=['POST']),
    webapp2.Route(r'/api/<coll_name:collections>/<cid:[^/]+>',                      collectionshandler.CollectionsHandler, name='coll_details', methods=['GET', 'PUT', 'DELETE']),
    webapp2.Route(r'/api/<coll_name:collections>/<cid:[^/]+>/sessions',             collectionshandler.CollectionsHandler, name='coll_ses', handler_method='get_sessions', methods=['GET']),
    webapp2.Route(r'/api/<coll_name:collections>/<cid:[^/]+>/acquisitions',         collectionshandler.CollectionsHandler, name='coll_acq',handler_method='get_acquisitions', methods=['GET']),

    webapp2.Route(r'/api/users/<uid:[^/]+>/<coll_name:[^/]+>',                      containerhandler.ContainerHandler, name='user_conts', handler_method='get_all_for_user', methods=['GET']),
    webapp2.Route(r'/api/projects/groups',                                          containerhandler.ContainerHandler, handler_method='get_groups_with_project', methods=['GET']),
    webapp2.Route(r'/api/<par_coll_name:[^/]+>/<par_id:[^/]+>/<coll_name:[^/]+>',   containerhandler.ContainerHandler, name='cont_sublist', handler_method='get_all', methods=['GET']),
    webapp2.Route(r'/api/<coll_name:[^/]+>',                                        containerhandler.ContainerHandler, name='cont_list', handler_method='get_all', methods=['GET']),
    webapp2.Route(r'/api/<coll_name:[^/]+>',                                        containerhandler.ContainerHandler, methods=['POST']),
    webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>',                            containerhandler.ContainerHandler, name='cont_details', methods=['GET','PUT','DELETE']),
]


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
