#!/usr/bin/env python

import elasticsearch
import json
import time
import logging

from api import config
from api.web import encoder

es = config.es

# SHHH
logging.getLogger('urllib3').setLevel(logging.WARNING)

FIELDS_INDEX = 'data_explorer_fields'
DE_INDEX = 'data_explorer'


ANALYSIS = {
    "analyzer": {
        "my_analyzer": {
            "tokenizer": "my_tokenizer"
        }
    },
    "tokenizer": {
        "my_tokenizer": {
            "type": "ngram",
            "min_gram": 2,
            "max_gram": 100,
            "token_chars": [
                "letter",
                "digit"
            ]
        }
    }
}

DYNAMIC_TEMPLATES = [
    {
        'string_fields' : {
            'match': '*',
            'match_mapping_type' : 'string',
            'mapping' : {
                'type': 'text',
                'analyzer': 'my_analyzer',
                "fields": {
                    "raw": {
                        "type": "keyword",
                        "index": "not_analyzed",
                        "ignore_above": 256
                    }
                }
            }
        }
    }
]

def get_field_type(field_type):
    if field_type in ['text', 'keyword']:
        return 'string'
    elif field_type in ['long', 'integer', 'short', 'byte']:
        return 'integer'
    elif field_type in ['double', 'float']:
        return 'float'
    elif field_type in ['date', 'boolean', 'object']:
        return field_type
    else:
        logging.warning('Didnt recognize this field type {}, setting as string'.format(field_type))


def handle_properties(properties, current_field_name):

    for field_name, field in properties.iteritems():

        logging.warning('my field name is {}'.format(field_name))

        # Ignore some fields
        if field_name in ['_all', 'dynamic_templates', 'analysis_reference', 'file_reference', 'parent', 'container_type', 'origin', 'permissions', '_id']:
            continue

        elif 'properties' in field:
            new_curr_field = current_field_name+'.'+field_name if current_field_name != '' else field_name
            logging.warning('found a properties field, going to call it {}'.format(new_curr_field))
            handle_properties(field['properties'], new_curr_field)

        else:
            field_type = get_field_type(field['type'])
            if field_type == 'object':
                # empty objects don't get added
                continue

            field_name = current_field_name+'.'+field_name if current_field_name != '' else field_name

            doc = {
                'name':                 field_name,
                'type':                 field_type
            }

            doc_s = json.dumps(doc)
            logging.warning('inserting {}'.format(doc))
            es.index(index=FIELDS_INDEX, id=field_name, doc_type='flywheel_field', body=doc_s)



if __name__ == '__main__':

    if es.indices.exists(FIELDS_INDEX):
        print 'Removing existing data explorer fields index...'
        res = es.indices.delete(index=FIELDS_INDEX)
        print 'response: {}'.format(res)

    # mappings = create_mappings()

    request = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
            'analysis' : ANALYSIS
        },
        'mappings': {
            '_default_' : {
                '_all' : {'enabled' : True},
                'dynamic_templates': DYNAMIC_TEMPLATES
            },
            'flywheel': {}
        }
    }

    print 'creating {} index ...'.format(FIELDS_INDEX)
    res = es.indices.create(index=FIELDS_INDEX, body=request)
    print 'response: {}'.format(res)


    try:
        mappings = es.indices.get_mapping(index=DE_INDEX, doc_type='flywheel')
    except:
        logging.error('Could not access mappings, exiting ...')
        sys.exit(1)

    fw_mappings = mappings[DE_INDEX]['mappings']['flywheel']['properties']

    handle_properties(fw_mappings, '')
