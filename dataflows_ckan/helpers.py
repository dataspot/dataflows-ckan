import json
import logging
import os

import requests

log = logging.getLogger(__name__)


def make_ckan_request(url, method='GET', headers=None, api_key=None, **kwargs):
    '''Make a CKAN API request to `url` and return the json response. **kwargs
    are passed to requests.request()'''

    if headers is None:
        headers = {}

    if api_key:
        if api_key.startswith('env:'):
            api_key = os.environ.get(api_key[4:])
        headers.update({'Authorization': api_key})

    response = requests.request(
        method=method, url=url, headers=headers, allow_redirects=True, **kwargs
    )

    try:
        return response.json()
    except json.decoder.JSONDecodeError:
        log.error('Expected JSON in response from: {}'.format(url))
        raise


def get_ckan_error(response):
    '''Return the error from a ckan json response, or None.'''
    ckan_error = None
    if not response['success'] and response['error']:
        ckan_error = response['error']
    return ckan_error
