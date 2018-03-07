"""
Resolve an ambiguous path through the data hierarchy.
"""
from .dao import containerstorage, containerutil
from .web.errors import APINotFoundException, InputValidationException
import bson
from collections import deque

class Node(object):
    # All lists obtained by the Resolver are sorted by the created timestamp, then the database ID as a fallback.
    # As neither property should ever change, this sort should be consistent
    sorting = [('created', 1), ('_id', 1)]

    # In some cases we only want to resolve the id of a container
    id_only_projection = {
        'label':             1,
        'permissions':       1,
        'files':             1,
    }

    def __init__(self, storage, parent, files=True, use_id=False):
        self.storage = storage
        self.node_type = containerutil.singularize(storage.cont_name)
        self.parent = parent
        self.files = files
        self.use_id = use_id

    def find(self, criterion, parent=None, id_only=False, include_files=True, use_id=False, limit=0): 
        query = {}

        # Setup criterion match
        if criterion:
            if use_id or self.use_id:
                if self.storage.use_object_id:
                    try:
                        query['_id'] = bson.ObjectId(criterion)
                    except bson.errors.InvalidId as e:
                        raise InputValidationException(e.message)
                else:
                    query['_id'] = criterion
            else:
                query['label'] = criterion

        # Add parent to query
        if parent and self.parent:
            query[self.parent] = parent['_id']

        # Setup projection
        if id_only: 
            proj = Node.id_only_projection.copy()
        else:
            proj = self.storage.get_list_projection()
            if not include_files:
                proj['files'] = 0

        # We don't use the user field here because we want to return a 403 if
        # they try to resolve something they don't have access to
        results = self.storage.get_all_el(query, None, proj, sort=Node.sorting, limit=limit)
        for el in results:
            self.storage.filter_deleted_files(el)
            el['node_type'] = self.node_type

        return results


PROJECT_TREE = [
    Node(containerstorage.GroupStorage(), None, files=False, use_id=True),
    Node(containerstorage.ProjectStorage(), 'group'),
    Node(containerstorage.SessionStorage(), 'project'),
    Node(containerstorage.AcquisitionStorage(), 'session')
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
    """ 
    Find a file by name
    """
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
                    resolved_children = node.find(None, parent=last, include_files=False)

                # Add any files from the last node
                resolved_children = resolved_children + files

        elif len(path) > 0:
            raise APINotFoundException('Cannot retrieve id for file: {0}'.format(path[0]))

        return {
            'path': resolved_path,
            'children': resolved_children
        }

