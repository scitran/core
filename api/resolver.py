"""
Resolve an ambiguous path through the data hierarchy.

The goal of the resolver is to provide a virtual graph that can be navigated using
path notation. Below is how the graph will ultimately be represented. Currently
subjects are not formalized and are excluded from the implementation.

Quoted strings represent literal nodes in the graph. For example, to find the gear
called dicom-mr-classifier, you would use the path: ["gears", "dicom-mr-classifier"]

+----+   +-------+   +-----+   +-------+
|Root+---+"gears"+---+Gears+---+Version|
+-+--+   +-------+   +-----+   +-------+
  |
+-+----+
|Groups|
+-+----+
  |
+-+------+
|Projects+---+
+-+------+   |
  |          |
+-+------+   |   +----------+   +--------+
|Subjects+---+---+"analyses"+---+Analyses|
+-+------+   |   +----------+   +---+----+
  |          |                      |
+-+------+   |                      |
|Sessions+---+-------+              |
+-+------+           |          +---+---+   +-----+
  |                  +----------+"files"+---+Files|
+-+----------+       |          +-------+   +-----+
|Acquisitions+-------+
+------------+
"""
import bson

from collections import deque

from .dao import containerutil
from .dao.basecontainerstorage import ContainerStorage
from .jobs import gears
from .web.errors import APINotFoundException, InputValidationException

def path_peek(path):
    """Return the next path element or None"""
    if len(path) > 0:
        return path[0]
    return None

def parse_criterion(path_in):
    """Parse criterion, returning true if we got an id"""
    if not path_in:
        return False, None

    value = path_in.popleft()
    use_id = False
    # Check for <id:xyz> syntax
    if value.startswith('<id:') and value.endswith('>'):
        value = value[4:len(value)-1]
        use_id = True

    return use_id, value

def get_parent(path_out):
    """Return the last parent element or None"""
    if path_out:
        return path_out[-1]
    return None

def apply_node_type(lst, node_type):
    """Apply node_type to each item in in the list"""
    if lst:
        for item in lst:
            item['node_type'] = node_type

def pop_files(container):
    """Return a consistently-ordered set of files for a given container."""
    if not container:
        return []

    files = container.pop('files', [])

    files.sort(key=lambda f: f.get('name', ''))
    apply_node_type(files, 'file')

    return files

def find_file(files, name):
    """Find a file  by name"""
    for f in files:
        if str(f.get('name')) == name:
            return f
    return None

class BaseNode(object):
    """Base class for all nodes in the resolver tree"""
    def next(self, path_in, path_out, id_only):
        # pylint: disable=W0613
        pass

    def get_children(self, path_out):
        # pylint: disable=W0613
        return []

class RootNode(BaseNode):
    """The root node of the resolver tree"""
    def __init__(self):
        self.groups_node = ContainerNode('groups', files=False, use_id=True)

    def next(self, path_in, path_out, id_only):
        """Get the next node in the hierarchy"""
        path_el = path_peek(path_in)

        if path_el == 'gears':
            path_in.popleft()
            return GearsNode()

        if path_el:
            return self.groups_node

        # TODO: Gears
        return None

    def get_children(self, path_out):
        """Get the children of the current node in the hierarchy"""
        return ContainerNode.get_container_children('groups')

class FilesNode(BaseNode):
    """Node that represents filename resolution"""
    def next(self, path_in, path_out, id_only):
        """Get the next node in the hierarchy"""
        filename = path_in.popleft()

        parent = get_parent(path_out)
        if not parent:
            raise APINotFoundException('No ' + filename + ' file found.')

        f = find_file(pop_files(parent), filename)
        if f is not None:
            path_out.append(f)
            return None

        raise APINotFoundException('No ' + filename + ' file found.')

class ContainerNode(BaseNode):
    # All lists obtained by the Resolver are sorted by the created timestamp, then the database ID as a fallback.
    # As neither property should ever change, this sort should be consistent
    sorting = [('created', 1), ('_id', 1)]

    # In some cases we only want to resolve the id of a container
    id_only_projection = {
        'label':             1,
        'permissions':       1,
        'files':             1,
    }

    def __init__(self, cont_name, files=True, use_id=False):
        self.cont_name = cont_name
        self.storage = ContainerStorage.factory(cont_name)
        # node_type is also the parent id field name
        self.node_type = containerutil.singularize(cont_name)
        self.files = files
        self.use_id = use_id        
        self.child_name = self.storage.get_child_container_name()

    def next(self, path_in, path_out, id_only):
        """Get the next node in the hierarchy, adding any value found to path_out"""
        # If there is no path in, don't try to resolve
        if not path_in:
            return None

        use_id, criterion = parse_criterion(path_in)
        parent = get_parent(path_out)
        # Peek to see if we need files for the next path element
        fetch_files = (path_peek(path_in) in ['files', None])

        # Setup criterion match
        query = {}
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
        if parent:
            query[parent['node_type']] = parent['_id']

        # Setup projection
        if id_only: 
            proj = ContainerNode.id_only_projection.copy()
            if fetch_files:
                proj['files'] = 1
        else:
            proj = self.storage.get_list_projection()
            if proj and not fetch_files:
                proj['files'] = 0

        # We don't use the user field here because we want to return a 403 if
        # they try to resolve something they don't have access to
        results = self.storage.get_all_el(query, None, proj, sort=ContainerNode.sorting, limit=1)
        if not results:
            raise APINotFoundException('No {0} {1} found.'.format(criterion, self.node_type))
        
        child = results[0]

        self.storage.filter_deleted_files(child)
        child['node_type'] = self.node_type
        path_out.append(child)

        # Get the next node
        if not path_in:
            return None

        if fetch_files:
            path_in.popleft()
            return FilesNode()

        # TODO: Check for analyses

        if self.child_name:
            return ContainerNode(self.child_name)

        return None

    def get_children(self, path_out):
        """Get all children of the last node"""
        parent = get_parent(path_out)

        # Get container chilren
        if self.child_name:
            query = {}
            if parent:
                query[parent['node_type']] = parent['_id']

            children = ContainerNode.get_container_children(self.child_name, query)
        else:
            children = []

        # TODO: Add analyses?

        # Add files
        return children + pop_files(parent)

    @classmethod
    def get_container_children(cls, cont_name, query=None):
        """Get all children of container named cont_name, using query"""
        storage = ContainerStorage.factory(cont_name)

        proj = storage.get_list_projection()
        if proj:
            proj['files'] = 0

        children = storage.get_all_el(query, None, proj, sort=ContainerNode.sorting)
        apply_node_type(children, containerutil.singularize(cont_name))

        return children

class GearsNode(BaseNode):
    """The top level "gears" node"""
    def next(self, path_in, path_out, id_only):
        """Get the next node in the hierarchy, adding any value found to path_out"""
        if not path_in:
            return None

        use_id, criterion = parse_criterion(path_in)
        if use_id:
            gear = gears.get_gear(criterion)
        else:
            gear = gears.get_gear_by_name(criterion)

        if not gear:
            raise APINotFoundException('No gear {0} found.'.format(criterion))

        gear['node_type'] = 'gear'
        path_out.append(gear)

        return None

    def get_children(self, path_out):
        """Get a list of all gears"""

        # No children for gears yet
        if path_out:
            return []

        results = gears.get_gears()

        for gear in results:
            gear['node_type'] = 'gear'

        return list(results)


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
        node = None
        next_node = RootNode()

        resolved_path = []
        resolved_children = []

        # Walk down the tree, building path until we get to a leaf node
        # Keeping in mind that path may be empty
        while next_node:
            node = next_node
            next_node = node.next(path, resolved_path, self.id_only)

        # If we haven't consumed path, then we didn't find what we were looking for
        if len(path) > 0:
            raise APINotFoundException('Could not resolve node for: ' + '/'.join(path))

        if hasattr(node, 'get_children'):
            resolved_children = node.get_children(resolved_path)

        return {
            'path': resolved_path,
            'children': resolved_children
        }

