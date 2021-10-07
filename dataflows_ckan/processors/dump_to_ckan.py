import copy
import logging

from ckan_datapackage_tools import converter
from dataflows.processors.dumpers.file_dumper import FileDumper
from datapackage import DataPackage
from tableschema_ckan_datastore import Storage
from tabulator import Stream

from ..helpers import get_ckan_error, make_ckan_request

log = logging.getLogger(__name__)


CKAN_PACKAGE_CORE_PROPERTIES = {
    'title': '',
    'version': '',
    'state': 'active',
    'url': '',
    'notes': '',
    'license_id': '',
    'author': '',
    'author_email': '',
    'maintainer': '',
    'maintainer_email': '',
    'owner_org': None,
    'private': False,
}

CKAN_RESOURCE_CORE_PROPERTIES = {
    'package_id': None,
    'url': None,
    'url_type': 'upload',
    'name': None,
    'hash': None,
}


class CkanDumper(FileDumper):
    def __init__(
        self,
        host,
        api_key,
        owner_org,
        overwrite_existing_data=True,
        push_to_datastore=False,
        push_to_datastore_method='insert',
        **options,
    ):
        super().__init__(options)
        self.api_path = '/api/3/action'
        self.host = host
        self.owner_org = owner_org
        self.base_endpoint = f'{self.host}{self.api_path}'
        self.package_create_endpoint = f'{self.base_endpoint}/package_create'
        self.package_update_endpoint = f'{self.base_endpoint}/package_update'
        self.resource_create_endpoint = f'{self.base_endpoint}/resource_create'
        self.overwrite_existing_data = overwrite_existing_data
        self.api_key = api_key
        self.ckan_dataset = copy.deepcopy(CKAN_PACKAGE_CORE_PROPERTIES)
        self.ckan_dataset['owner_org'] = self.owner_org
        self.push_to_datastore = push_to_datastore
        self.push_to_datastore_method = push_to_datastore_method

    def get_iterator(self, datastream):
        # I don't see a clean way to ensure creating a CKAN package before resources
        # as iterating over files does not give the datapackage.json first (which would provide one convention-based way).
        # So, this is the 'cleanest' method I can override AFAIK (less clean: self._process) where I can access
        # self.datapackage to get the data I need to create a CKAN package before processing resources.
        self.write_ckan_dataset()
        return super().get_iterator(datastream)

    def write_file_to_output(self, filename, path):
        # this is a hack so we don't have to re-implement all of
        # rows_processor just to use this method.
        res_path = path.lstrip('./')
        is_datapackage_descriptor = res_path == 'datapackage.json'
        if is_datapackage_descriptor:
            descriptor = self.datapackage.descriptor
        else:
            matches = [r for r in self.datapackage.resources if r.descriptor['path'] == res_path]
            assert len(matches) == 1
            descriptor = matches[0].descriptor
        # end hack

        ckan_resource = copy.deepcopy(CKAN_RESOURCE_CORE_PROPERTIES)
        ckan_resource['package_id'] = self.ckan_dataset['id']
        ckan_resource['name'] = descriptor['name']
        ckan_resource['url'] = res_path
        # TODO - only packages have hash in the datastream?
        ckan_resource['hash'] = descriptor.get('hash', '')
        ckan_resource['encoding'] = 'utf-8' if descriptor else descriptor['encoding']
        ckan_resource['format'] = 'json' if is_datapackage_descriptor else descriptor['format']

        ckan_files = {'upload': (res_path, open(filename, 'rb'))}
        response = self.create_or_update_ckan_data(
            'resource', data=ckan_resource, files=ckan_files
        )
        if self.push_to_datastore and not is_datapackage_descriptor:
            self.push_resource_to_datastore(
                filename,
                response['id'],
                descriptor['schema'],
            )
        return response

    def push_resource_to_datastore(self, filename, ckan_resource_id, resource_schema):
        storage = Storage(
            base_url=self.base_endpoint,
            dataset_id=self.ckan_dataset['id'],
            api_key=self.api_key,
        )
        storage.create(ckan_resource_id, resource_schema)
        storage.write(
            ckan_resource_id,
            Stream(filename, format='csv').open(),
            method=self.push_to_datastore_method,
        )
        return True

    def write_ckan_dataset(self):
        self.ckan_dataset.update(converter.datapackage_to_dataset(self.datapackage))
        # we deal with resource metadata mapping later
        # based on the ported implementation so following here
        del self.ckan_dataset['resources']
        response = self.create_or_update_ckan_data(
            'package',
            json=self.ckan_dataset,
            method='POST',
        )
        # we will get back the ID
        self.ckan_dataset = response['result']
        return self.ckan_dataset

    def create_or_update_ckan_data(self, entity, method='POST', json=None, data=None, files=None):
        response = make_ckan_request(
            getattr(self, f'{entity}_create_endpoint'),
            method=method,
            json=json,
            data=data,
            files=None,
            api_key=self.api_key,
        )
        error = get_ckan_error(response)
        # in ported code, there was another check for type of error based on the message
        # but it break with i18n so just assuming every error lets us overwrite.
        if error and self.overwrite_existing_data is True:
            response = make_ckan_request(
                getattr(self, f'{entity}_update_endpoint'),
                method=method,
                json=json,
                data=data,
                files=None,
                api_key=self.api_key,
            )
            error = get_ckan_error(response)
        if error:
            raise Exception(f'{json.dumps(error)}')
        return response


if __name__ == '__main__':
    CkanDumper()()
