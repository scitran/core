from webob import exc

from ..web import base
from .. import config

class AbstractContainerHandler(base.RequestHandler):
    """
    Asbtract handler that removes the need to know a container's noun before performing an action.
    """

    def handle(self, cid, extra):
        """
        Redirect a request from /containers/x/... to its proper destination.
        For example:
            /containers/x/files --> x is a project ID --> /projects/x/files
        """

        # Efficiently check many databases -.-
        jsFunc = """
        function searchContainer(x) {
            return {
                'groups': db.getCollection('groups').findOne({"_id" : x}, {"_id": 1}),
                'projects': db.getCollection('projects').findOne({"_id" : ObjectId(x)}, {"_id": 1}),
                'sessions': db.getCollection('sessions').findOne({"_id" : ObjectId(x)}, {"_id": 1}),
                'acquisitions': db.getCollection('acquisitions').findOne({"_id" : ObjectId(x)}, {"_id": 1})
            }
        };
        """

        # Run command; check result
        command = config.db.command('eval', jsFunc + ' searchContainer("' + cid + '");')
        result = command.get('retval')

        if command.get('ok') != 1.0 or result is None:
            self.abort(500, 'Error running db command')

        # Find which container type was found, if any
        ctype = None
        for key in result.keys():
            if result[key] is not None:
                ctype = key; break
        else:
            self.abort(404, 'No container ' + cid + ' found')

        # Construct resultant URL
        destination = '/api/' + ctype + '/' + cid + extra


        url = self.request.path_qs

        print
        print
        print url
        print destination

        # print self.request.environ

        for route in self.app.router.match_routes:
            try:
                match = route.match(self.request)

                if match:

                    print route
                    print 'MATCHED'

                    import pprint
                    pprint.pprint(vars(route))

                    # route.handler_method(*args, **kwargs)

                    break

            except exc.HTTPMethodNotAllowed:
                pass
        else:
            print 'NOT MATCHED'



        # Technically, request.path should not have param args
        # self.request.path = destination
        # self.request.path_qs = destination

        # thing = self.app.router.dispatch(self.request, self.response)


        print 'FINISHED'

        # print thing

        # return super(base.RequestHandler, self).dispatch()

        # This needs to just serve a handler, rather than redirect
        # self.redirect(destination, permanent=False)
