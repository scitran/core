import copy

from . import (
    es_query, add_filter_from_list, add_filter
)
from .. import config

log = config.log

querygraph = {
    'acquisitions': {
        'parents': ['sessions', 'collections'],
    },
    'sessions': {
        'parents': ['projects'],
        'children': ['acquisitions']
    },
    'projects': {
        'parents': ['groups'],
        'children': ['sessions']
    },
    'groups': {
        'children': ['projects']
    },
    'collections': {
        'children': ['acquisitions']
    }
}

_min_score = 1

class SearchContainer(object):

    def __init__(self, cont_name, query, targets, all_data=False, user=None):
        self.cont_name = cont_name
        self.query = query
        self.is_target = False
        self.child_targets = set()
        self.results = None
        self.all_data = all_data
        self.user = user

        for t in targets:
            if t == cont_name:
                self.is_target = True
            else:
                self.child_targets.add(t)


    def get_results(self):
        if self.query is None:
            return
        else:
            if self.results is None:
                self.results = self._exec_query(self.query)
            return self.results

    def _exec_query(self, query):
        # pylint: disable=unexpected-keyword-arg
        # pylint disable can be removed after PyCQA/pylint#258 is fixed


        if not self.all_data and self.user and self.cont_name == 'projects':
            # Filter on permissions for searches that do not include all data
            self.query = add_filter(self.query, 'permissions._id', self.user)

        source_filter = None
        if self.all_data and self.cont_name == 'sessions':
            # Filter out firtname and lastname of subjects when searching all data
            source_filter = { 'exclude': ['subject.firstname', 'subject.lastname', 'subject.firstnamehash', 'subject.lastnamehash']}

        q = es_query(query, self.cont_name, _min_score, source_filter=source_filter)
        results = config.es.search(
            index='scitran',
            doc_type=self.cont_name,
            body=q,
            size=10000
        )['hits']['hits']
        return {r['_id']: r for r in results}

    def receive(self, source, source_results, from_child=False):
        if source_results is None:
            return
        if from_child and self.cont_name == 'collections':
            filter_on_field = '_id'
            list_ids = []
            for r in source_results.values():
                for _id in r['_source'].get('collections',[]):
                    list_ids.append(_id)
        elif from_child:
            filter_on_field = '_id'
            list_ids = [r['_source'][self.cont_name[:-1]] for r in source_results.values()]
        else:
            filter_on_field = source[:-1] if source != 'collections' else source
            list_ids = source_results.keys()
        self.query = add_filter_from_list(self.query, filter_on_field, list_ids)
        if self.results is not None:
            updated_results = {}
            log.debug('{} {} {}'.format(self.cont_name, list_ids, filter_on_field))
            for _id, r in self.results.iteritems():
                # if we are not filtering on the _id we need to get the _source
                doc = r if filter_on_field == '_id' else r['_source']
                log.debug('{} {}'.format(self.cont_name, doc.get(filter_on_field, [])))
                if self._to_set(doc.get(filter_on_field, [])).intersection(list_ids):
                    updated_results[_id] = r
            self.results = updated_results
            log.debug('{} {}'.format(self.cont_name, len(self.results)))
        else:
            self.results = self._exec_query(self.query)

    def _to_set(self, value_or_list):
        if type(value_or_list) == list:
            return set(value_or_list)
        else:
            return set([value_or_list])

    def collect(self):
        if self.is_target and self.results is None:
            self.results = self._exec_query(query={"match_all": {}})
        final_results = {self.cont_name: self.results.values()} if self.is_target else {}
        for t in self.child_targets:
            results = t.get_results(self.cont_name, self.results)
            final_results[t.name] = final_results.get(t.name, []) + results.values()
        return final_results


class TargetProperty(object):

    def __init__(self, name, query):
        self.name = name
        self.query = query

    def _get_results(self, parent_name, parent_results):
        if self.query is None:
            self.query = {"match_all": {}}
        self.query = add_filter(self.query, 'container_name', parent_name)
        if parent_results is not None:
            parent_ids = parent_results.keys()
            self.query = add_filter_from_list(self.query, 'container._id', parent_ids)
        return self._exec_query(self.query)

    def get_results(self, parent_name, parent_results):
        return self._get_results(parent_name, parent_results)

    def _exec_query(self, query):
        # pylint: disable=unexpected-keyword-arg
        # pylint disable can be removed after PyCQA/pylint#258 is fixed
        q = es_query(query, self.name, _min_score)
        results = config.es.search(
            index='scitran',
            doc_type=self.name,
            body=q,
            size=10000
        )['hits']['hits']
        return {r['_id']: r for r in results}


class TargetInAnalysis(TargetProperty):

    def __init__(self, name, query, analyses_query):
        super(TargetInAnalysis, self).__init__(name, query)
        self.target_analysys = TargetProperty('analyses', analyses_query)

    def get_results(self, parent_name, parent_results):
        analysis_list = self.target_analysys.get_results(parent_name, parent_results)
        return self._get_results('analyses', analysis_list)


class PreparedSearch(object):

    def __init__(self, target_paths, queries, all_data=False, user=None):
        self.containers = ['groups', 'projects', 'sessions', 'collections', 'acquisitions']
        self.queries = queries
        self.target_lists = {}
        for path in target_paths:
            targets = self._get_targets(path)
            self._merge_into(targets, self.target_lists)
        self.search_containers = {}
        last_relevant_container = None
        # In this loop we check for each container
        # 1) if there is a parent container affected by this search
        # 2) if there is a query on the container
        # 3) if the container is a target of the query
        # If one of the conditions is true, we include the container in the search
        # the index of the last relevant container is saved
        #
        # Please note that the containers are checked in reverse order so the index is
        # from the end of the container list.
        for i, cont_name in enumerate(self.containers[::-1]):
            query = self.queries.get(cont_name)
            targets = self.target_lists.get(cont_name, [])
            if query or targets or last_relevant_container is not None:
                if last_relevant_container is None:
                    last_relevant_container = i
                self.search_containers[cont_name] = SearchContainer(cont_name, query, targets, all_data, user)
        # In this line, we exclude from search all the non relevant containers (The one "after" the relevant container)
        # if the index of the last relevant container is 0 we include all the containers
        if last_relevant_container != 0:
            self.containers = self.containers[:- last_relevant_container]


    def _get_targets(self, path):
        path_parts = path.split('/')
        query = self.queries.get(path_parts[-1])
        if path_parts[-1] in ['files', 'notes', 'analyses']:
            if len(path_parts) >= 2 and path_parts[-2] == 'analyses':
                min_length = 2
                analyses_query = self.queries.get('analyses')
                target = TargetInAnalysis(path_parts[-1], query, analyses_query)
            else:
                min_length = 1
                target = TargetProperty(path_parts[-1], query)
            if len(path_parts) == min_length:
                return {
                    c: [copy.deepcopy(target)] for c in self.containers
                }
            else:
                return {path_parts[-min_length-1]: [target]}
        else:
            return {path_parts[-1]: [path_parts[-1]]}

    def _merge_into(self, source_dict, destination):
        for k, elements in source_dict.iteritems():
            destination[k] = destination.get(k, []) + elements

    def process_search(self):
        for cont_name in self.containers:
            container = self.search_containers[cont_name]
            partial_results = container.get_results()
            for child_name in querygraph[cont_name].get('children', []):
                if child_name in self.containers:
                    self.search_containers[child_name].receive(
                        cont_name, partial_results
                    )
        results = {}
        for cont_name in self.containers[::-1]:
            container = self.search_containers[cont_name]
            new_results = container.collect()
            self._merge_into(new_results, results)
            for parent_name in querygraph[cont_name].get('parents', []):
                self.search_containers[parent_name].receive(
                    cont_name, container.results, from_child=True
                )
        return results
