import webapp2

from . import core
from handlers import containerhandler
from handlers import collectionshandler

routes = [
    # webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/tags/<value:[^/]+>',         listhandler.ListHandler, name='tags', defaults={'list_name': 'tags'}),
    # webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/file',                       listhandler.FileListHandler, name='files_post', methods=['POST'], defaults={'list_name': 'files'}),
    # webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/file/<filename:[^/]+>',      listhandler.FileListHandler, name='files', defaults={'list_name': 'files'}),
    # webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/permissions',                listhandler.ListHandler, name='perms_post', methods=['POST'], defaults={'list_name': 'permissions'}),
    # webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/permissions/<site:[^/]+>/<_id:[^/]+>',
    #                                                                                 listhandler.ListHandler, name='perms', defaults={ 'list_name': 'permissions'}),
    # webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/notes',                      listhandler.NotesListHandler, name='notes_post', methods=['POST'], defaults={'list_name': 'notes'}),
    # webapp2.Route(r'/api/<coll_name:[^/]+>/<cid:[^/]+>/notes/<_id:[^/]+>',          listhandler.NotesListHandler, name='notes', defaults={'list_name': 'notes'}),

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