"""
Resolve an ambiguous path through the data hierarchy.
"""

from . import config
from .web.errors import APINotFoundException, InputValidationException
from bson.objectid import ObjectId
from collections import deque

class Node(object):
    # All lists obtained by the Resolver are sorted by the created timestamp, then the database ID as a fallback.
    # As neither property should ever change, this sort should be consistent
    sorting = [('created', 1), ('_id', 1)]

    # Globally disable extraneous properties of unbounded length, along with some PHI fields.
    default_projection = {
        'files':             0,
        'info':              0,
        'tags':              0,
        'subject.sex':       0,
        'subject.age':       0,
        'subject.race':      0,
        'subject.ethnicity': 0,
        'subject.info':      0,
        'subject.firstname': 0,
        'subject.lastname':  0,
    }

    # In some cases we only want to resolve the id of a container
    id_only_projection = {
        '_id':               1,
        'label':             1,
        'permissions':       1,
        'files':             1,
    }

    def __init__(self, collection, node_type, parent, files=True, use_id=False, object_id=True):
        self.collection = collection
        self.node_type = node_type
        self.parent = parent
        self.files = files
        self.use_id = use_id
        self.object_id = object_id

    def find(self, criterion, parent=None, id_only=False, include_files=False, use_id=False, limit=0): 
        query = {
            'deleted': {'$exists':  False}
        }

        # Setup criterion match
        if criterion:
            if use_id or self.use_id:
                if self.object_id:
                    query['_id'] = ObjectId(criterion)
                else:
                    query['_id'] = criterion
            else:
                query['label'] = criterion

        # Add parent to query
        if parent and self.parent:
            query[self.parent] = parent['_id']

        # Setup projection
        if id_only: 
            proj = Node.id_only_projection
        else:
            proj = Node.default_projection.copy()
            if include_files:
                del proj['files']

        results = list(config.db[self.collection].find(query, proj, sort=Node.sorting, limit=limit))
        for el in results:
            el['node_type'] = self.node_type
        return results


PROJECT_TREE = [
    Node('groups', 'group', None, False, True, False),
    Node('projects', 'project', 'group'),
    Node('sessions', 'session', 'project'),
    Node('acquisitions', 'acquisition', 'session')
]

def parse_criterion(value):
    if not value:
        return False, None

    use_id = False
    # Check for <id:xyz> syntax
    if value.startswith('<id:') and value.endswith('>'):
        value = value[4:len(value)-1]
        use_id = True

    return use_id, value

def pop_files(container):
    """
    Return a consistently-ordered set of files for a given container.
    """
    if not container:
        return []

    files = container.pop('files', [])

    files.sort(key=lambda f: f.get('name', ''))
    for f in files:
        f['node_type'] = 'file'

    return files

def find_file(files, name):
    for f in files:
        if str(f.get('name')) == name:
            return f
    return None

class Resolver(object):
    """
    Given an array of human-meaningful, possibly-ambiguous strings, resolve it as a path through the hierarchy.

    Does not tolerate ambiguity at any level of the path except the final node.
    """

    def __init__(self, id_only=False):
        self.id_only = id_only 

    def resolve(self, path):
        if not isinstance(path, list):
            raise InputValidationException("Path must be an array of strings")

        path = deque(path)
        tree = deque(PROJECT_TREE)
        resolved_path = []
        resolved_children = []
        last = None
        files = []

        # Short circuit - just return a list of groups
        if not path:
            resolved_children = tree[0].find(None, id_only=self.id_only)
            return {
                'path': resolved_path,
                'children': resolved_children
            }

        # Walk down the tree, building path until we get to the last node
        # Keeping in mind that path may be empty
        while len(path) > 0 and len(tree) > 0:
            node = tree.popleft()
            current_id, current = parse_criterion(path.popleft()) 
            
            # Find the next child 
            children = node.find(current, parent=last, id_only=self.id_only, include_files=True, use_id=current_id, limit=1) 

            # If children is empty, try to find a match in the last set of files
            if not children:
                # Check in last set of files
                if not current_id:
                    child = find_file(files, current)
                    if child:
                        children = [child]
                        files = []
                        if len(path) > 0:
                            raise APINotFoundException('Files have no children')

            if not children:
                # Not found
                or_file = 'or file ' if node.files else ''
                raise APINotFoundException('No {0} {1} {2} found.'.format(current, node.node_type, or_file))

            # Otherwise build up path
            resolved_path.append(children[0])
            last = resolved_path[-1]
            files = pop_files(last)

        # If there are path elements left, search in the last set of files
        if len(path) > 0:
            filename = path.popleft()
            f = find_file(files, filename)
            if not f:
                raise APINotFoundException('No ' + filename + ' file found.')
            if len(path) > 0:
                raise APINotFoundException('Files have no children')
            resolved_path.append(f)
            files = []

        # Resolve children 
        if not self.id_only:
            if last and last.get('node_type') != 'file':
                # Retrieve any child nodes
                if len(tree) > 0:
                    node = tree[0]
                    resolved_children = node.find(None, parent=last)

                # Add any files from the last node
                resolved_children = resolved_children + files

        elif len(path) > 0:
            raise APINotFoundException('Cannot retrieve id for file: {0}'.format(path[0]))

        return {
            'path': resolved_path,
            'children': resolved_children
        }

