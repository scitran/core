from ..web import base
from .. import config
from ..auth import require_login, require_superuser
from ..dao import containerstorage, APINotFoundException, APIValidationException
#from ..validators import validate_data

log = config.log

class APIClassificationException(APIValidationException):
    def __init__(self, modality, errors):

        error_msg = 'Classification does not match format for modality {}. Unallowable key-value pairs: {}'.format(modality, errors)

        super(APIValidationException, self).__init__(error_msg)
        self.errors = errors

class ModalityHandler(base.RequestHandler):

    def __init__(self, request=None, response=None):
        super(ModalityHandler, self).__init__(request, response)
        self.storage = containerstorage.ContainerStorage('modalities', use_object_id=False)

    @require_login
    def get(self, modality_name):
        return self.storage.get_container(modality_name)

    @require_login
    def get_all(self):
        return self.storage.get_all_el(None, None, None)

    @require_superuser
    def post(self):
        payload = self.request.json_body
        # Clean this up when validate_data method is fixed to use new schemas
        # POST unnecessary, used to avoid run-time modification of schema
        #validate_data(payload, 'modality.json', 'input', 'POST', optional=True)

        result = self.storage.create_el(payload)
        if result.acknowledged:
            return {'_id': result.inserted_id}
        else:
            self.abort(400, 'Modality not inserted')

    @require_superuser
    def put(self, modality_name):
        payload = self.request.json_body
        # Clean this up when validate_data method is fixed to use new schemas
        # POST unnecessary, used to avoid run-time modification of schema
        #validate_data(payload, 'modality.json', 'input', 'POST', optional=True)

        result = self.storage.update_el(modality_name, payload)
        if result.matched_count == 1:
            return {'modified': result.modified_count}
        else:
            raise APINotFoundException('Modality with name {} not found, modality not updated'.format(modality_name))

    @require_superuser
    def delete(self, modality_name):
        result = self.storage.delete_el(modality_name)
        if result.deleted_count == 1:
            return {'deleted': result.deleted_count}
        else:
            raise APINotFoundException('Modality with name {} not found, modality not deleted'.format(modality_name))

    @staticmethod
    def check_and_format_classification(modality_name, classification_map):
        """
        Given a modality name and a proposed classification map,
        ensure:
          - that a modality exists with that name and has a classification
            map
          - all keys in the classification_map exist in the
            `classifications` map on the modality object
          - all the values in the arrays in the classification_map
            exist in the modality's classifications map

        And then return a map with the keys and values properly formatted

        For example:
            Modality = {
                "_id" = "Example_modality",
                "classifications": {
                    "Example1": ["Blue", "Green"]
                    "Example2": ["one", "two"]
                }
            }

        Returns properly formatted classification map:
            classification_map = {
                "Example1": ["Blue"],
                "custom":   ["anything"]
            }

        Raises APIClassificationException:
            classification_map = {
                "Example1": ["Red"], # "Red" is not allowed
                "Example2": ["one", "two"]
            }
        """
        try:
            modality = containerstorage.ContainerStorage('modalities', use_object_id=False).get_container(modality_name)
        except APINotFoundException as e:
            if classification_map.keys() == ['custom']:
                # for unknown modalities allow only list of custom values
                return classification_map
            else:
                raise e

        classifications = modality.get('classifications', {})

        formatted_map = {} # the formatted map that will be returned
        bad_kvs = [] # a list of errors to report, formatted like ['k:v', 'k:v', 'k:v']

        for k,array in classification_map.iteritems():
            if k == 'custom':
                # any unique value is allowed in custom list
                formatted_map[k] = array

            else:
                for v in array:

                    allowed, fk, fv = case_insensitive_search(classifications, k, v)

                    if allowed:
                        if fk in formatted_map:
                            formatted_map[fk].append(fv)
                        else:
                            formatted_map[fk] = [fv]

                    else:
                        bad_kvs.append(k+':'+v)

        if bad_kvs:
            raise APIClassificationException(modality_name, bad_kvs)

        return formatted_map

    def case_insensitive_search(classifications, proposed_key, proposed_value):
        """
        Looks for value in given classification map, returning:

        1) found     - a boolean that is true if the proposed_value was found, false if not
        2) key_name  - the key name of the classification list where it was found
        3) value     - the formatted string that should be saved to the file's classification

        NOTE: If the proposed_value was not found, key_name and value will be None

        This function is used mainly to preserve the existing stylization of the strings stored on
        the modalities set classifications.
        """

        for k in classifications.keys():
            if k.lower() == proposed_key.lower():
                for v in classifications[k]:
                    if v.lower() == proposed_value.lower():

                        # Both key and value were found
                        return True, k, v

                # Key was found but not value
                return False, None, None

        # key was not found
        return False, None, None




