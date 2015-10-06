import webapp2

from . import core
from handlers import containerhandler

routes = [
    webapp2.Route(r'/api2/curators',                                                containerhandler.CollectionsHandler, handler_method='get_all', methods=['GET']),

    webapp2.Route(r'/api2/collections',                                             containerhandler.CollectionsHandler, handler_method='get_all', methods=['GET']),
    webapp2.Route(r'/api2/collections',                                             containerhandler.CollectionsHandler, methods=['POST']),
    webapp2.Route(r'/api2/collections/<cid:[^/]+>',                                 containerhandler.CollectionsHandler, methods=['GET', 'PUT', 'DELETE']),

    webapp2.Route(r'/api2/users/<uid:[^/]+>/<coll_name:[^/]+>',                     containerhandler.ContainerHandler, handler_method='get_all_for_user', methods=['GET']),

    webapp2.Route(r'/api2/<par_coll_name:[^/]+>/<par_id:[^/]+>/<coll_name:[^/]+>',  containerhandler.ContainerHandler, handler_method='get_all', methods=['GET']),
    webapp2.Route(r'/api2/<coll_name:[^/]+>',                                       containerhandler.ContainerHandler, methods=['POST']),
    webapp2.Route(r'/api2/<coll_name:[^/]+>/<cid:[^/]+>',                           containerhandler.ContainerHandler, methods=['GET','PUT','DELETE']),
]