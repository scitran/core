import webapp2

from . import core
from handlers import containerhandler
from handlers import collectionshandler

routes = [
    webapp2.Route(r'/api2/curators',                                                collectionshandler.CollectionsHandler, handler_method='curators', methods=['GET']),

    webapp2.Route(r'/api2/<coll_name:collections>',                                 collectionshandler.CollectionsHandler, name='colls', handler_method='get_all', methods=['GET']),
    webapp2.Route(r'/api2/<coll_name:collections>',                                 collectionshandler.CollectionsHandler, methods=['POST']),
    webapp2.Route(r'/api2/<coll_name:collections>/<cid:[^/]+>',                     collectionshandler.CollectionsHandler, name='coll_details', methods=['GET', 'PUT', 'DELETE']),
    webapp2.Route(r'/api2/<coll_name:collections>/<cid:[^/]+>/sessions',            collectionshandler.CollectionsHandler, name='coll_ses', handler_method='get_sessions', methods=['GET']),
    webapp2.Route(r'/api2/<coll_name:collections>/<cid:[^/]+>/acquisitions',        collectionshandler.CollectionsHandler, name='coll_acq',handler_method='get_acquisitions', methods=['GET']),

    webapp2.Route(r'/api2/users/<uid:[^/]+>/<coll_name:[^/]+>',                     containerhandler.ContainerHandler, name='user_conts', handler_method='get_all_for_user', methods=['GET']),

    webapp2.Route(r'/api2/<par_coll_name:[^/]+>/<par_id:[^/]+>/<coll_name:[^/]+>',  containerhandler.ContainerHandler, name='cont_sublist', handler_method='get_all', methods=['GET']),
    webapp2.Route(r'/api2/<coll_name:[^/]+>',                                       containerhandler.ContainerHandler, name='cont_list', handler_method='get_all', methods=['GET']),
    webapp2.Route(r'/api2/<coll_name:[^/]+>',                                       containerhandler.ContainerHandler, methods=['POST']),
    webapp2.Route(r'/api2/<coll_name:[^/]+>/<cid:[^/]+>',                           containerhandler.ContainerHandler, name='cont_details', methods=['GET','PUT','DELETE']),
]