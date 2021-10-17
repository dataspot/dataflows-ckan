import copy
import logging
import json as json_module

from ckan_datapackage_tools import converter
from dataflows.base.resource_wrapper import ResourceWrapper
from dataflows.processors.dumpers.file_dumper import FileDumper
from tableschema_ckan_datastore import Storage

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
DESCRIPTOR_RESOURCE_NAME = 'datapackage_json'

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
        self.package_show_endpoint = f'{self.base_endpoint}/package_show'
        self.overwrite_existing_data = overwrite_existing_data
        self.api_key = api_key
        self.ckan_dataset = copy.deepcopy(CKAN_PACKAGE_CORE_PROPERTIES)
        self.ckan_dataset['owner_org'] = self.owner_org
        self.push_to_datastore = push_to_datastore
        self.push_to_datastore_method = push_to_datastore_method

    def process_datapackage(self, datapackage):
        datapackage = super().process_datapackage(datapackage)
        self.write_ckan_dataset(datapackage)
        return datapackage

    def write_file_to_output(self, filename, path):
        # this is a hack so we don't have to re-implement all of
        # rows_processor just to use this method.
        res_path = path.lstrip('./')
        is_datapackage_descriptor = res_path == 'datapackage.json'
        if is_datapackage_descriptor:
            descriptor = self.datapackage.descriptor
            resource_name = DESCRIPTOR_RESOURCE_NAME
        else:
            matches = [r for r in self.datapackage.resources if r.descriptor['path'] == res_path]
            assert len(matches) == 1
            descriptor = matches[0].descriptor
            resource_name = descriptor['name']
        # end hack

        ckan_resource = copy.deepcopy(CKAN_RESOURCE_CORE_PROPERTIES)
        resource_id = [r['id'] for r in self.ckan_dataset['resources'] if r['name'] == resource_name]
        if len(resource_id) == 1:
            ckan_resource['id'] = resource_id[0]
        ckan_resource['package_id'] = self.ckan_dataset['id']
        ckan_resource['name'] = resource_name
        ckan_resource['url'] = res_path
        # TODO - only packages have hash in the datastream?
        ckan_resource['hash'] = descriptor.get('hash', '')
        ckan_resource['encoding'] = 'utf-8' if descriptor else descriptor['encoding']
        ckan_resource['format'] = 'json' if is_datapackage_descriptor else descriptor['format']

        ckan_files = {'upload': (res_path, open(filename, 'rb'))}
        self.create_or_update_ckan_data(
            'resource', data=ckan_resource, files=ckan_files
        )

    def rows_processor(self, resource: ResourceWrapper, writer, temp_file):
        stream = super().rows_processor(resource, writer, temp_file)
        if self.push_to_datastore:
            schema = resource.res.schema.descriptor
            resource_id = [r['id'] for r in self.ckan_dataset['resources'] if r['name'] == resource.res.name]
            if len(resource_id) == 1:
                yield from self.push_resource_to_datastore(stream, resource_id[0], schema)
                return
        yield from stream

    def push_resource_to_datastore(self, rows, ckan_resource_id, resource_schema):
        storage = Storage(
            base_url=self.base_endpoint,
            dataset_id=self.ckan_dataset['id'],
            api_key=self.api_key,
        )
        storage.create(ckan_resource_id, resource_schema)
        yield from storage.write(
            ckan_resource_id,
            rows,
            method=self.push_to_datastore_method,
            as_generator=True,
        )

    def write_ckan_dataset(self, datapackage):
        self.ckan_dataset.update(self.get_ckan_package(datapackage))
        dataset = converter.datapackage_to_dataset(datapackage)
        # We don't want to overwrite the existing dataset resources
        dataset.pop('resources', None)
        self.ckan_dataset.update(dataset)
        response = self.create_or_update_ckan_data(
            'package',
            json=self.ckan_dataset,
            method='POST',
        )
        # we will get back the ID
        self.ckan_dataset = response['result']
        existing_names = [r['name'] for r in self.ckan_dataset['resources']]
        for resource in datapackage.resources:
            name = resource.name
            if name not in existing_names:
                response = self.create_or_update_ckan_data(
                    'resource',
                    json=dict(package_id=self.ckan_dataset['id'], name=name),
                    method='POST',
                )
                self.ckan_dataset['resources'].append(response['result'])
        return self.ckan_dataset

    def create_or_update_ckan_data(self, entity, method='POST', json=None, data=None, files=None):
        response = make_ckan_request(
            getattr(self, f'{entity}_create_endpoint'),
            method=method,
            json=json,
            data=data,
            files=files,
            api_key=self.api_key,
        )
        error = get_ckan_error(response)
        # in ported code, there was another check for type of error based on the message
        # but it break with i18n so just assuming every error lets us overwrite.
        update_error = None
        if error and self.overwrite_existing_data is True:
            response = make_ckan_request(
                getattr(self, f'{entity}_update_endpoint'),
                method=method,
                json=json,
                data=data,
                files=files,
                api_key=self.api_key,
            )
            update_error = get_ckan_error(response)
        if update_error is not None:
            # the more intersting error is the create error
            raise Exception(f'{json_module.dumps(error)}')
        return response

    def get_ckan_package(self, datapackage):
        response = make_ckan_request(
            self.package_show_endpoint,
            method='GET',
            params=dict(id=datapackage.descriptor['name']),
            api_key=self.api_key,
        )
        error = get_ckan_error(response)
        if error:
            return dict()
        return response['result']

if __name__ == '__main__':
    CkanDumper()()
