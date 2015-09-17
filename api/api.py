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
from . import projects
from . import sessions
from . import acquisitions
from . import collections
from . import listhandler
from . import permchecker
from dao import storage


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
        webapp2.Route(r'/count',                                    users.Users, handler_method='count', methods=['GET']),
        webapp2.Route(r'/self',                                     users.User, handler_method='self', methods=['GET']),
        webapp2.Route(r'/roles',                                    users.User, handler_method='roles', methods=['GET']),
        webapp2.Route(r'/<:[^/]+>',                                 users.User, name='user'),
        webapp2.Route(r'/<:[^/]+>/groups',                          users.Groups, name='groups'),
        webapp2.Route(r'/<uid:[^/]+>/projects',                     projects.Projects, name='u_projects'),
    ]),
    webapp2.Route(r'/api/groups',                                   users.Groups),
    webapp2_extras.routes.PathPrefixRoute(r'/api/groups', [
        webapp2.Route(r'/count',                                    users.Groups, handler_method='count', methods=['GET']),
        webapp2.Route(r'/<:[^/]+>',                                 users.Group, name='group'),
        webapp2.Route(r'/<gid:[^/]+>/projects',                     projects.Projects, name='g_projects'),
        webapp2.Route(r'/<gid:[^/]+>/sessions',                     sessions.Sessions, name='g_sessions', methods=['GET']),
        webapp2.Route(r'/<cid:[^/]+>/roles/<_id:[^/]+>',            listhandler.ListHandler, name='g_roles',
                                                                    defaults={
                                                                        'permchecker': permchecker.group_roles_sublist,
                                                                        'storage': storage.ListStorage('groups', 'roles')
                                                                    }),
    ]),
    webapp2.Route(r'/api/projects',                                 projects.Projects, methods=['GET'], name='projects'),
    webapp2_extras.routes.PathPrefixRoute(r'/api/projects', [
        webapp2.Route(r'/count',                                    projects.Projects, handler_method='count', methods=['GET']),
        webapp2.Route(r'/groups',                                   projects.Projects, handler_method='groups', methods=['GET']),
        webapp2.Route(r'/schema',                                   projects.Project, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/<:[0-9a-f]{24}>',                          projects.Project, name='project'),
        webapp2.Route(r'/<pid:[0-9a-f]{24}>/sessions',              sessions.Sessions, name='p_sessions'),
        webapp2.Route(r'/<cid:[^/]+>/file',                         listhandler.FileListHandler, name='pj_files_post', methods=['POST'],
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.ListStorage('projects', 'files', True)
                                                                    }),
        webapp2.Route(r'/<cid:[^/]+>/file/<filename:[^/]+>',        listhandler.FileListHandler, name='pj_files',
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.ListStorage('projects', 'files', True)
                                                                    }),
        webapp2.Route(r'/<cid:[^/]+>/tags/<tag:[^/]+>',             listhandler.ListHandler, name='pj_tags',
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.StringListStorage('projects', 'tags', True, 'tag')
                                                                    }),
    ]),
    webapp2.Route(r'/api/collections',                              collections.Collections),
    webapp2_extras.routes.PathPrefixRoute(r'/api/collections', [
        webapp2.Route(r'/count',                                    collections.Collections, handler_method='count', methods=['GET']),
        webapp2.Route(r'/curators',                                 collections.Collections, handler_method='curators', methods=['GET']),
        webapp2.Route(r'/schema',                                   collections.Collection, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/<:[0-9a-f]{24}>',                          collections.Collection, name='collection'),
        webapp2.Route(r'/<cid:[^/]+>/file',                         listhandler.FileListHandler, name='cl_files_post', methods=['POST'],
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.ListStorage('collections', 'files', True)
                                                                    }),
        webapp2.Route(r'/<cid:[^/]+>/file/<filename:[^/]+>',        listhandler.FileListHandler, name='cl_files',
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.ListStorage('collections', 'files', True)
                                                                    }),
        webapp2.Route(r'/<cid:[^/]+>/tags/<tag:[^/]+>',             listhandler.ListHandler, name='cl_tags',
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.StringListStorage('collections', 'tags', True, 'tag')
                                                                    }),
        webapp2.Route(r'/<:[0-9a-f]{24}>/sessions',                 collections.CollectionSessions, name='coll_sessions'),
        webapp2.Route(r'/<:[0-9a-f]{24}>/acquisitions',             collections.CollectionAcquisitions, name='coll_acquisitions'),
    ]),
    webapp2.Route(r'/api/sessions',                                 sessions.Sessions, methods=['GET'], name='sessions'),
    webapp2_extras.routes.PathPrefixRoute(r'/api/sessions', [
        webapp2.Route(r'/count',                                    sessions.Sessions, handler_method='count', methods=['GET']),
        webapp2.Route(r'/schema',                                   sessions.Session, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/<:[0-9a-f]{24}>',                          sessions.Session, name='session'),
        webapp2.Route(r'/<cid:[^/]+>/file',                         listhandler.FileListHandler, name='se_files_post', methods=['POST'],
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.ListStorage('sessions', 'files', True)
                                                                    }),
        webapp2.Route(r'/<cid:[^/]+>/file/<filename:[^/]+>',        listhandler.FileListHandler, name='se_files',
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.ListStorage('sessions', 'files', True)
                                                                    }),
        webapp2.Route(r'/<cid:[^/]+>/tags/<tag:[^/]+>',             listhandler.ListHandler, name='se_tags',
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.StringListStorage('sessions', 'tags', True, 'tag')
                                                                    }),
        webapp2.Route(r'/<:[0-9a-f]{24}>/acquisitions',             acquisitions.Acquisitions, name='acquisitions'),
    ]),
    webapp2_extras.routes.PathPrefixRoute(r'/api/acquisitions', [
        webapp2.Route(r'/count',                                    acquisitions.Acquisitions, handler_method='count', methods=['GET']),
        webapp2.Route(r'/schema',                                   acquisitions.Acquisition, handler_method='schema', methods=['GET']),
        webapp2.Route(r'/<:[0-9a-f]{24}>',                          acquisitions.Acquisition, name='acquisition'),
        webapp2.Route(r'/<cid:[^/]+>/tags/<tag:[^/]+>',             listhandler.ListHandler, name='aq_tags',
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.StringListStorage('acquisitions', 'tags', True, 'tag')
                                                                    }),
        webapp2.Route(r'/<cid:[^/]+>/file',                         listhandler.FileListHandler, name='aq_files_post', methods=['POST'],
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.ListStorage('acquisitions', 'files', True)
                                                                    }),
        webapp2.Route(r'/<cid:[^/]+>/file/<filename:[^/]+>',        listhandler.FileListHandler, name='aq_files',
                                                                    defaults={
                                                                        'permchecker': permchecker.default_sublist,
                                                                        'storage': storage.ListStorage('acquisitions', 'files', True)
                                                                    }),
    ]),
    webapp2.Route(r'/api/jobs',                                     jobs.Jobs),
    webapp2_extras.routes.PathPrefixRoute(r'/api/jobs', [
        webapp2.Route(r'/next',                                     jobs.Jobs, handler_method='next', methods=['GET']),
        webapp2.Route(r'/count',                                    jobs.Jobs, handler_method='count', methods=['GET']),
        webapp2.Route(r'/addTestJob',                               jobs.Jobs, handler_method='addTestJob', methods=['GET']),
        webapp2.Route(r'/<:[^/]+>',                                 jobs.Job,  name='job'),
    ]),
]


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
