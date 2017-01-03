"""
API request handlers for the jobs module
"""

from ..web import base
from ..resolver import Resolver

class ResolveHandler(base.RequestHandler):

    """Provide /resolve API route."""

    def resolve(self):
        """Resolve a path through the hierarchy."""

        if self.public_request:
            self.abort(403, 'Request requires login')

        doc = self.request.json
        result = Resolver.resolve(doc['path'])

        # Cancel the request if anything in the path is unauthorized; remove any children that are unauthorized.
        if not self.superuser_request:
            for x in result["path"]:
                ok = False
                if x['node_type'] in ['acquisition', 'session', 'project', 'group']:
                    perms = x.get('roles', []) + x.get('permissions', [])
                    for y in perms:
                        if y.get('_id') == self.uid:
                            ok = True
                            break

                    if not ok:
                        self.abort(403, "Not authorized")

            filtered_children = []
            for x in result["children"]:
                ok = False
                if x['node_type'] in ['acquisition', 'session', 'project', 'group']:
                    perms = x.get('roles', []) + x.get('permissions', [])
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
