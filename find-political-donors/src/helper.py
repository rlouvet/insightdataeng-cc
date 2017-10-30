#!/usr/bin/env python
# File with helper functions
import collections
import datetime

def valid_date(date_text):
    try:
        datetime.datetime.strptime(date_text, '%m%d%Y')
    except ValueError:
        return False
    return True

def line_count(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def keys_exists(element, *keys):
    '''
    Check if *keys (nested) exists in `element` (dict).
    '''
    if (type(element) is not dict) and (type(element) is not collections.OrderedDict):
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True