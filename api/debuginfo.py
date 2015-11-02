import logging

log = logging.getLogger('scitran.api')

child_collections = {
    'projects': 'sessions',
    'sessions': 'acquisitions',
    'groups': 'projects'
}

def add_debuginfo(handler, coll_name, response):
    if type(response) == list:
        _add_di_list(handler, coll_name, response)
    else:
        _add_di(handler, coll_name, response)

def _add_di_list(handler, coll_name, response):
    for elem in response:
        _add_di(handler, coll_name, elem)
        elem['debug']['details'] = handler.uri_for(
            'cont_details',
            coll_name=coll_name,
            cid=elem['_id'],
            _full=True) + '?' + handler.request.query_string

def _add_di(handler, coll_name, response):
    response['debug'] = {}
    if child_collections.get(coll_name):
        child_coll_name = child_collections[coll_name]
        response['debug'][child_coll_name] = handler.uri_for(
            'cont_sublist',
            par_coll_name=coll_name,
            par_id=response['_id'],
            coll_name=child_coll_name,
            _full=True) + '?' + handler.request.query_string
    if response.get('project'):
        response['debug']['project'] = handler.uri_for(
            'cont_details',
            coll_name='projects',
            cid=response['project'],
            _full=True) + '?' + handler.request.query_string
    if response.get('group'):
        response['debug']['group'] = handler.uri_for(
            'group',
            _id=response['group'],
            _full=True) + '?' + handler.request.query_string

