"""
Resolve an ambiguous path through the data hierarchy.
"""

from . import config
from .web.errors import APINotFoundException

class Node(object):

    # All lists obtained by the Resolver are sorted by the created timestamp, then the database ID as a fallback.
    # As neither property should ever change, this sort should be consistent
    sorting = [('created', 1), ('_id', 1)]

    # Globally disable extraneous properties of unbounded length, along with some PHI fields.
    projection = {
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

    # Add some more fields for debugging purposes.
    # projection['roles']       = 0
    # projection['permissions'] = 0

    @staticmethod
    def get_children(parent):
        raise NotImplementedError() # pragma: no cover

    @staticmethod
    def filter(children, criterion, _id=False):
        raise NotImplementedError() # pragma: no cover

def _get_files(table, match):
    """
    Return a consistently-ordered set of files for a given container query.
    """

    pipeline = [
        {'$match': match },
        {'$unwind': '$files'},
        {'$sort': {'files.name': 1}},
        {'$group': {'_id':'$_id', 'files': {'$push':'$files'}}}
    ]

    result = config.mongo_pipeline(table, pipeline)
    if len(result) == 0:
        return []

    files = result[0]['files']
    for x in files:
        x.update({'node_type': 'file'})
    return files

def _get_docs(table, label, match):
    results = list(config.db[table].find(match, Node.projection, sort=Node.sorting))
    for y in results:
        y.update({'node_type': label})
    return results


class FileNode(Node):
    @staticmethod
    def get_children(parent):
        return []

    @staticmethod
    def filter(children, criterion, _id=False):
        raise APINotFoundException("Files have no children")

class AcquisitionNode(Node):
    @staticmethod
    def get_children(parent):
        files    = _get_files('acquisitions', {'_id' : parent['_id'] })

        return files

    @staticmethod
    def filter(children, criterion, _id=False):
        for x in children:
            if x['node_type'] == "file" and x.get('name') == criterion:
                return x, FileNode
        raise APINotFoundException('No ' + criterion + ' file found.')

class SessionNode(Node):

    @staticmethod
    def get_children(parent):
        acqs = _get_docs('acquisitions', 'acquisition', {'session' : parent['_id']})
        files    = _get_files('sessions', {'_id' : parent['_id'] })

        return list(acqs) + files

    @staticmethod
    def filter(children, criterion, _id=False):
        if _id:
            selectAcq = '_id'
            selectFil = '_id'
        else:
            selectAcq = 'label'
            selectFil = 'name'

        for x in children:
            if x['node_type'] == "acquisition" and str(x.get(selectAcq)) == criterion:
                return x, AcquisitionNode
            if x['node_type'] == "file" and str(x.get(selectFil)) == criterion:
                return x, FileNode
        raise APINotFoundException('No ' + criterion + ' acquisition or file found.')

class ProjectNode(Node):

    @staticmethod
    def get_children(parent):
        sessions = _get_docs('sessions', 'session', {'project' : parent['_id']})
        files    = _get_files('projects', {'_id' : parent['_id'] })

        return list(sessions) + files

    @staticmethod
    def filter(children, criterion, _id=False):
        if _id:
            selectSes = '_id'
            selectFil = '_id'
        else:
            selectSes = 'label'
            selectFil = 'name'

        for x in children:
            if x['node_type'] == "session" and str(x.get(selectSes)) == criterion:
                return x, SessionNode
            if x['node_type'] == "file" and str(x.get(selectFil)) == criterion:
                return x, FileNode
        raise APINotFoundException('No ' + criterion + ' session or file found.')

class GroupNode(Node):

    @staticmethod
    def get_children(parent):
        projects = _get_docs('projects', 'project', {'group' : parent['_id']})
        return projects

    @staticmethod
    def filter(children, criterion, _id=False):
        if _id:
            select = '_id'
        else:
            select = 'label'

        for x in children:
            if str(x.get(select)) == criterion:
                return x, ProjectNode
        raise APINotFoundException('No ' + criterion + ' project found.')

class RootNode(Node):

    @staticmethod
    def get_children(parent):
        groups = _get_docs('groups', 'group', {})
        return groups

    @staticmethod
    def filter(children, criterion, _id=False):
        for x in children:
            if x.get('_id') == criterion:
                return x, GroupNode
        raise APINotFoundException('No ' + criterion + ' group found.')


class Resolver(object):
    """
    Given an array of human-meaningful, possibly-ambiguous strings, resolve it as a path through the hierarchy.

    Does not tolerate ambiguity at any level of the path except the final node.
    """

    @staticmethod
    def resolve(path):

        if not isinstance(path, list):
            raise Exception("Path must be an array of strings")

        node, resolved, last = Resolver._resolve(path, RootNode)
        children = node.get_children(last)

        return {
            'path': resolved,
            'children': children
        }

    @staticmethod
    def _resolve(path, node, parents=None):

        if parents is None:
            parents = []

        last = None
        if len(parents) > 0:
            last = parents[len(parents) - 1]

        if len(path) == 0:
            return node, parents, last

        current = path[0]
        current_id = False

        # Check for <id:xyz> syntax
        if current.startswith('<id:') and current.endswith('>'):
            current = current[4:len(current)-1]
            current_id = True
            print current

        children = node.get_children(last)
        selected, next_ = node.filter(children, current, current_id)

        path = path[1:]
        parents.append(selected)

        return Resolver._resolve(path, next_, parents)
