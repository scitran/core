#!/usr/bin/env python

import ast
import copy
import dateutil.parser
import dicom
import elasticsearch
import json
import logging

from api import config
from api.web import encoder

es = config.es
db = config.db

# SHHH
logging.getLogger('urllib3').setLevel(logging.WARNING)

DE_INDEX = 'data_explorer'

ANALYSIS = {
            'analyzer' : {
                'str_search_analyzer' : {
                    'tokenizer' : 'keyword',
                    'filter' : ['lowercase']
                },

                'str_index_analyzer' : {
                    'tokenizer' : 'keyword',
                    'filter' : ['lowercase', 'substring']
                }
            },
            'filter' : {
                'substring': {
                    'type': 'nGram',
                    'min_gram': 2,
                    'max_gram': 50,
                    'token_chars': []
                }
            }
        }

DYNAMIC_TEMPLATES = [
    {
            '_id': {
                'match': '_id',
                'match_mapping_type' : 'string',
                'mapping': {
                   'type': 'keyword',
                   'index': 'not_analyzed'
                }
            }
        },
    {
            'long_fields' : {
                'match_mapping_type' : 'long',
                'mapping' : {
                    'ignore_malformed': True
                }
            }
        },
    {
            'integer_fields' : {
                'match_mapping_type' : 'integer',
                'mapping' : {
                    'ignore_malformed': True
                }
            }
        },
    {
            'double_fields' : {
                'match_mapping_type' : 'double',
                'mapping' : {
                    'ignore_malformed': True
                }
            }
        },
    {
            'float_fields' : {
                'match_mapping_type' : 'float',
                'mapping' : {
                    'ignore_malformed': True
                }
            }
        },
    {
            'short_fields' : {
                'match_mapping_type' : 'short',
                'mapping' : {
                    'ignore_malformed': True
                }
            }
        },
    {
            'byte_fields' : {
                'match_mapping_type' : 'byte',
                'mapping' : {
                    'ignore_malformed': True
                }
            }
        },
        {
            'hash': {
                'match': 'hash',
                'match_mapping_type' : 'string',
                'mapping': {
                   'type': 'text',
                   'index': 'not_analyzed'
                }
            }
        },
        {
            'string_fields' : {
                'match': '*',
                'match_mapping_type' : 'string',
                'mapping' : {
                    'type': 'text',
                    'analyzer': 'str_search_analyzer',
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

def datetime(str_datetime):
    pass

def age(str_age):
    pass

BLACKLIST_KEYS = ['template', 'roles', 'permissions', 'analyses', 'files', 'collections']


SKIPPED = []
SKIPPED2ELECTRICBOOGALOO = ['PixelSpacing', 'ImageOrientationPatient', 'PatientAge', 'ImagePositionPatient']

# TODO: Choose integer/long and float/double where appropriate
VR_TYPES = {
        'AE': ['string', str],
        'AS': ['long', int],
        'AT': ['long', int],
        'CS': ['string', str],
        'DA': ['date', datetime],
        'DS': ['float', float],
        'DT': ['date', datetime],
        'FD': ['float', float],
        'FL': ['float', float],
        'IS': ['long', int],
        'LO': ['string', str],
        'LT': ['string', str],
        'NONE': None,
        'OB': None,
        'OB or OW': None,
        'OF': None,
        'OW': None,
        'PN': ['string', str],
        'SH': ['string', str],
        'SL': ['long', int],
        'SQ': None,
        'SS': ['long', int],
        'ST': ['string', str],
        'TM': ['time', datetime],
        'UI': ['string', str],
        'UL': ['long', int],
        'US': ['long', int],
        'US or OW': ['long', int],
        'US or SS': ['long', int],
        'US or SS or OW': ['long', int],
        'UT': ['string', str]
    }

def create_mappings():
    public_dict = dicom._dicom_dict.DicomDictionary
    field_mappings = {}
    for k,v in public_dict.iteritems():
        vr_type = v[0]
        field_name = v[4]
        vr_mapping = VR_TYPES.get(vr_type)
        if vr_mapping:
            field_type = vr_mapping[0]
            if field_type == 'string' and vr_type not in ['UT', 'LT', 'ST']:
                field_mappings[field_name+'_term'] = {'type': 'string', 'index': 'not_analyzed'}
            field_mappings[field_name] = {'type': field_type}
            if field_type == 'time':
                # Actually store as date, format as time:
                field_mappings[field_name] = {'type': 'date', 'format': 'time'}
        else:
            pass
            #logging.warn('Skipping field {} of VR type {}'.format(field_name, vr_type))

    return field_mappings

def cast_date(dcm_date):
    """
    Cast DICOM date string (YYYYMMDD) into ElasticSearch pre-defined strict_date format (yyyy-MM-dd)
    """
    return dcm_date[:4] + '-' + dcm_date[4:6] + '-' + dcm_date[6:]

def cast_time(dcm_time):
    """
    Cast DICOM time string (HHMMSS.FRAC)
    into ElasticSearch pre-defined strict_time format (HH:mm:ss.SSSZZ)
    """
    # TODO: this fxn needs to be tested on real data
    if len(dcm_time) < 6:
        return None
    hours = dcm_time[:2]
    minutes = dcm_time[2:4]
    seconds = dcm_time[4:6]
    if len(dcm_time) > 7:
        fraction_str = dcm_time[7:]
        fraction = float(dcm_time[7:])/10^(len(fraction_str))
        fraction = int(fraction*1000)
    else:
        fraction = 0
    return '%s:%s:%s.%03d00' % (hours, minutes, seconds, fraction)

def cast_datetime(dcm_datetime):
    """
    Cast DICOM datetime string (YYYYMMDDHHMMSS.FFFFFF)
    into ElasticSearch pre-defined basic_date_time format (yyyyMMdd'T'HHmmss.SSSZ)
    """
    # TODO: this fxn needs to be tested on real data
    year = dcm_datetime[:4]
    month = dcm_datetime[4:6]
    day = dcm_datetime[6:8]
    if len(dcm_datetime) > 8:
        hours = dcm_datetime[8:10]
        minutes = dcm_datetime[10:12]
        seconds = dcm_datetime[12:14]
    else:
        hours = '00'
        minutes = '00'
        seconds = '00'
    if len(dcm_datetime) > 15:
        fraction_str = dcm_datetime[15:]
        fraction = float(dcm_datetime[15:])/10^(len(fraction_str))
        fraction = int(fraction*1000)
    else:
        fraction = 0
    return '%s%s%sT%s%s%s.%03d0' % (year, month, day, hours, minutes, seconds, fraction)

def cast_age(dcm_age):
    """ Cast DICOM age string into seconds"""
    # TODO: this fxn needs to be tested on real data
    unit = dcm_age[-1]
    if unit not in ['D', 'W', 'M', 'Y']:
        return None
    multipliers = dict(D=60*60*24,
                       W=60*60*24*7,
                       M=60*60*24*30,
                       Y=60*60*24*365)
    value = int(dcm_age[:-1])
    seconds = multipliers[unit]*value
    return seconds

def value_is_array(value):
    if type(value) != unicode:
        return False
    if len(value) < 2:
        return False
    if value[0] == '[' and value[-1] == ']':
        return True
    return False

def cast_array_from_string(string):
    array = None
    try:
        array = ast.literal_eval(string)
    except:
        config.log.warn('Tried to cast string {} as array, failed.'.format(string))

    if array:
        new_array = []
        for element in array:
            try:
                element = int(element)
            except:
                try:
                    element = float(element)
                except:
                    pass
            new_array.append(element)
        return new_array
    else:
        return string

def remove_blacklisted_keys(obj):
    for key in BLACKLIST_KEYS:
        obj.pop(key, None)

def handle_files(parent, parent_type, files, dicom_mappings, permissions, doc):
    doc['container_type'] = 'file'
    for f in files:
        # f.pop('info', None)
        doc['file'] = f
        # doc = {
        #     'file': f,
        #     'permissions': permissions
        # }
        # if f.get('type', '') == 'dicom' and f.get('info'):
        #     dicom_data = f.pop('info')
        #     term_fields = {}
        #     modified_data = {}
        #     for skipped in SKIPPED:
        #         dicom_data.pop(skipped, None)
        #     for k,v in dicom_data.iteritems():

        #         try:

        #             # Arrays are saved as strings in
        #             if value_is_array(v):
        #                 config.log.debug('calling array for {} and value {}'.format(k, v))
        #                 v = cast_array_from_string(v)
        #             if 'datetime' in k.lower():
        #                 config.log.debug('called datetime for {} and value {}'.format(k, v))
        #                 v = cast_datetime(str(v))
        #             elif 'date' in k.lower():
        #                 config.log.debug('called date for {} and value {}'.format(k, v))
        #                 v = cast_date(str(v))
        #             # elif 'time' in k.lower():
        #             #     # config.log.debug('called time for {} and value {}'.format(k, v))
        #             #     # v = cast_time(str(v))
        #             elif 'Age' in k:
        #                 config.log.debug('called age for {} and value {}'.format(k, v))
        #                 v = cast_age(str(v))
        #         except:
        #             pass

        #         term_field_name = k+'_term'
        #         if term_field_name in dicom_mappings and type(v) in [unicode, str]:
        #             term_fields[k+'_term'] = str(v)
        #         modified_data[k] = v

        #     modified_data.update(term_fields)
        #     doc['dicom_header'] = modified_data

        generated_id = str(parent['_id']) + '_' + f['name']

        doc['parent'] = {
            '_id': parent['_id'],
            'type': parent_type
        }

        doc_s = json.dumps(doc, default=encoder.custom_json_serializer)
        try:
            # es.index(index=DE_INDEX, id=generated_id, parent=str(parent['_id']), doc_type='file', body=doc)
            es.index(index=DE_INDEX, id=generated_id, doc_type='flywheel', body=doc_s)
        except:
            return


if __name__ == '__main__':

    if es.indices.exists(DE_INDEX):
        print 'Removing existing data explorer index...'
        res = es.indices.delete(index=DE_INDEX)
        print 'response: {}'.format(res)

    # mappings = create_mappings()

    request = {
        'settings': {
            "index.mapping.total_fields.limit": 4000,
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

    print 'creating {} index ...'.format(DE_INDEX)
    res = es.indices.create(index=DE_INDEX, body=request)
    print 'response: {}'.format(res)

    # mappings = es.indices.get_mapping(index=DE_INDEX, doc_type='flywheel')

    # dicom_mappings = mappings[DE_INDEX]['mappings']['file']['properties']['dicom_header']['properties']
    dicom_mappings = None

    permissions = []

    groups = db.groups.find({})
    print 'STARTING THE GROUPS'
    print ''
    print ''
    print ''
    count = 1
    group_count_total = groups.count()
    for g in groups:
        print 'Loading group {} ({} of {})'.format(g['name'], count, group_count_total)
        count += 1

        remove_blacklisted_keys(g)

        projects = db.projects.find({'group': g['_id']})
        for p in projects:

            files = p.pop('files', [])
            # Set permissions for documents
            permissions = p.pop('permissions', [])
            remove_blacklisted_keys(p)

            doc = {
                'project':              p,
                'group':                g,
                'permissions':          permissions,
                'container_type':       'project'

            }

            doc_s = json.dumps(doc, default=encoder.custom_json_serializer)
            es.index(index=DE_INDEX, id=str(p['_id']), doc_type='flywheel', body=doc_s)

            handle_files(p, 'project', files, dicom_mappings, permissions, doc)


            sessions = db.sessions.find({'project': p['_id']})
            for s in sessions:
                subject = s.pop('subject', {})

                analyses = s.pop('analyses', [])
                files = s.pop('files', [])
                remove_blacklisted_keys(s)

                doc = {
                    'project':              p,
                    'group':                g,
                    'session':              s,
                    'subject':              subject,
                    'permissions':          permissions,
                    'container_type':       'session'

                }

                doc_s = json.dumps(doc, default=encoder.custom_json_serializer)
                es.index(index=DE_INDEX, id=str(s['_id']), doc_type='flywheel', body=doc_s)

                handle_files(s, 'session', files, dicom_mappings, permissions, doc)

                for an in analyses:
                    files = an.pop('files', [])
                    doc = {
                        'analysis':             an,
                        'session':              s,
                        'subject':              subject,
                        'project':              p,
                        'group':                g,
                        'permissions':          permissions,
                        'container_type':       'analysis'

                    }

                    doc_s = json.dumps(doc, default=encoder.custom_json_serializer)
                    es.index(index=DE_INDEX, id=str(an['_id']), doc_type='flywheel', body=doc_s)

                    files = [f for f in files if f.get('output')]

                    handle_files(an, 'analysis', files, dicom_mappings, permissions, doc)



                acquisitions = db.acquisitions.find({'session': s['_id']})
                for a in acquisitions:
                    a.pop('info', None)
                    files = a.pop('files', [])
                    remove_blacklisted_keys(a)

                    doc = {
                        'acquisition':          a,
                        'session':              s,
                        'subject':              subject,
                        'project':              p,
                        'group':                g,
                        'permissions':          permissions,
                        'container_type':       'acquisition'

                    }

                    doc_s = json.dumps(doc, default=encoder.custom_json_serializer)
                    es.index(index=DE_INDEX, id=str(a['_id']), doc_type='flywheel', body=doc_s)


                    handle_files(a, 'acquisition', files, dicom_mappings, permissions, doc)



