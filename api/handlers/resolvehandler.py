"""
API request handlers for the jobs module
"""
from webapp2 import Request

from ..dao import containerutil
from ..web import base
from ..web.errors import APINotFoundException
from ..resolver import Resolver

class ResolveHandler(base.RequestHandler):

    """Provide /resolve API route."""

    def resolve(self):
        """Resolve a path through the hierarchy, and include node details with children"""
        return self._resolve_and_check_permissions(False)

    def lookup(self):
        """Locate a node by path, and re-route to the endpoint for that node"""
        result = self._resolve_and_check_permissions(True)

        # If we resolved a file, we can just return that file node
        path = result.get('path', [])
        
        if not path:
            raise APINotFoundException('No node matched that path')

        # In the event that we resolved a file, just return the file node
        dest = path[-1]
        if dest.get('node_type') == 'file':
            return dest

        # Reroute to the actual path that will log access, resolve analyses, etc
        path = self._get_node_path(dest)

        # Create new request instance using destination URI (eg. replace containers with cont_name)
        destination_environ = self.request.environ
        for key in 'PATH_INFO', 'REQUEST_URI':
            destination_environ[key] = destination_environ[key].replace('lookup', path, 1)
        # We also must update the method, and indicate that we want the node_type included
        # The client will depend on node_type being set so that it can map to the correct type
        destination_environ['REQUEST_METHOD'] = 'GET'
        destination_environ['fw_node_type'] = dest['node_type']
        destination_request = Request(destination_environ)

        # Apply SciTranRequest attrs
        destination_request.id = self.request.id
        destination_request.logger = self.request.logger

        # Dispatch the destination request
        self.app.router.dispatch(destination_request, self.response)

    def _get_node_path(self, node):
        """Get the actual resource path for node"""
        try:
            cname = containerutil.pluralize(node['node_type'])  
        except ValueError:
            # Handle everything else...
            cname = node['node_type'] + 's'
            
        return '{0}/{1}'.format(cname, node['_id'])

    def _resolve_and_check_permissions(self, id_only):
        """Resolve a path through the hierarchy."""
        if self.public_request:
            self.abort(403, 'Request requires login')

        doc = self.request.json

        resolver = Resolver(id_only=id_only)
        result = resolver.resolve(doc['path'])

        # Cancel the request if anything in the path is unauthorized; remove any children that are unauthorized.
        if not self.superuser_request:
            for x in result["path"]:
                ok = False
                if x['node_type'] in ['acquisition', 'session', 'project', 'group', 'analysis']:
                    perms = x.get('permissions', [])
                    for y in perms:
                        if y.get('_id') == self.uid:
                            ok = True
                            break

                    if not ok:
                        self.abort(403, "Not authorized")

            filtered_children = []
            for x in result["children"]:
                ok = False
                if x['node_type'] in ['acquisition', 'session', 'project', 'group', 'analysis']:
                    perms =  x.get('permissions', [])
                    for y in perms:
                        if y.get('_id') == self.uid:
                            ok = True
                            break
                else:
                    ok = True

                if ok:
                    filtered_children.append(x)

            result["children"] = filtered_children

        return result


