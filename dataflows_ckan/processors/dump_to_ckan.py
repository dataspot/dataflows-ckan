import hashlib
import json
import logging
import os

from ckan_datapackage_tools import converter
from dataflows import schema_validator
from dataflows.processors.dumpers.file_dumper import FileDumper
from datapackage import DataPackage
from tableschema_ckan_datastore import Storage
from tabulator import Stream

from ..helpers import get_ckan_error, make_ckan_request

log = logging.getLogger(__name__)


class CkanDumper(FileDumper):
    def initialize(
        self,
        host,
        api_key,
        push_resources_to_datastore=False,
        push_to_datastore_method='insert',
        dataset_properties={},
    ):
        super().initialize()

        self.api_path = '/api/3/action'
        self.host = host
        self.base_endpoint = f'{self.host}{self.api_path}'
        self.package_create_endpoint = f'{self.base_endpoint}/package_create'
        self.package_update_endpoint = f'{self.base_endpoint}/package_update'
        self.resource_create_endpoint = f'{self.base_endpoint}/resource_create'

        self.api_key = api_key
        self.push_resources_to_datastore = push_resources_to_datastore
        self.push_to_datastore_method = push_to_datastore_method

        self.dataset_properties = dataset_properties
        self.dataset_resources = []
        self.dataset_id = None

        datastore_methods = ['insert', 'upsert', 'update']

        if self.push_to_datastore_method not in datastore_methods:
            raise RuntimeError(
                f'push_to_datastore_method must be one of {", ".join(datastore_methods)}'
            )

    def handle_resources(self, datapackage, resource_iterator, parameters, stats):
        '''
        This method handles most of the control flow for creating
        dataset/resources on ckan.

        First we handle the dataset creation, then, if successful, we
        create each resource.
        '''

        # Calculate datapackage hash
        if self.datapackage_hash:
            datapackage_hash = hashlib.md5(
                json.dumps(datapackage, sort_keys=True, ensure_ascii=True).encode('ascii')
            ).hexdigest()
            self.set_attr(datapackage, self.datapackage_hash, datapackage_hash)

        # Handle the datapackage first!
        self.handle_datapackage(datapackage, parameters, stats)

        # Handle non-streaming resources
        for resource in datapackage['resources']:
            if not resource.get('dpp:streaming', False):
                resource_metadata = {
                    'package_id': self.__dataset_id,
                    'url': resource['dpp:streamedFrom'],
                    'name': resource['name'],
                }
                if 'format' in resource:
                    resource_metadata.update({'format': resource['format']})
                request_params = {'json': resource_metadata}

                self._create_ckan_resource(request_params)

        # Handle each resource in resource_iterator
        for resource in resource_iterator:
            resource_spec = resource.spec
            ret = self.handle_resource(
                schema_validator(resource), resource_spec, parameters, datapackage
            )
            ret = self.row_counter(datapackage, resource_spec, ret)
            yield ret

        stats['count_of_rows'] = self.get_attr(datapackage, self.datapackage_rowcount)
        stats['bytes'] = self.get_attr(datapackage, self.datapackage_bytes)
        stats['hash'] = self.get_attr(datapackage, self.datapackage_hash)
        stats['dataset_name'] = datapackage['name']

    def handle_datapackage(self, datapackage, parameters, stats):
        '''Create or update a ckan dataset from datapackage and parameters'''

        # core dataset properties
        dataset = {
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

        dp = DataPackage(datapackage)
        dataset.update(converter.datapackage_to_dataset(dp))

        self.dataset_resources = dataset.get('resources', [])
        if self.dataset_resources:
            del dataset['resources']

        # Merge dataset-properties from parameters into dataset.
        dataset.update(self.dataset_properties) if self.dataset_properties else None

        response = make_ckan_request(
            self.package_create_endpoint, method='POST', json=dataset, api_key=self.api_key
        )

        ckan_error = get_ckan_error(response)
        if (
            ckan_error
            and parameters.get('overwrite_existing')
            and 'That URL is already in use.' in ckan_error.get('name', [])
        ):

            log.info('CKAN dataset with url already exists. ' 'Attempting package_update.')
            response = make_ckan_request(
                self.package_update_endpoint,
                method='POST',
                json=dataset,
                api_key=self.api_key,
            )
            ckan_error = get_ckan_error(response)

        if ckan_error:
            log.exception('CKAN returned an error: ' + json.dumps(ckan_error))
            raise Exception

        if response['success']:
            self.dataset_id = response['result']['id']

    def rows_processor(self, resource, spec, temp_file, writer, fields, datapackage):
        file_formatter = self.file_formatters[spec['name']]
        for row in resource:
            file_formatter.write_row(writer, row, fields)
            yield row
        file_formatter.finalize_file(writer)

        # File size:
        filesize = temp_file.tell()
        self.inc_attr(datapackage, self.datapackage_bytes, filesize)
        self.inc_attr(spec, self.resource_bytes, filesize)

        # File Hash:
        if self.resource_hash:
            temp_file.seek(0)
            hasher = hashlib.md5()
            data = 'x'
            while len(data) > 0:
                data = temp_file.read(1024)
                hasher.update(data.encode('utf8'))
            self.set_attr(spec, self.resource_hash, hasher.hexdigest())

        # Finalise
        filename = temp_file.name
        temp_file.close()

        resource_metadata = {
            'package_id': self.dataset_id,
            'url': 'url',
            'url_type': 'upload',
            'name': spec['name'],
            'hash': spec['hash'],
        }
        resource_metadata.update({'encoding': spec['encoding']}) if 'encoding' in spec else None
        resource_metadata.update({'format': spec['format']}) if 'format' in spec else None
        ckan_filename = os.path.basename(spec['path'])
        resource_files = {'upload': (ckan_filename, open(temp_file.name, 'rb'))}
        request_params = {'data': resource_metadata, 'files': resource_files}
        try:
            # Create the CKAN resource
            create_result = self._create_ckan_resource(request_params)
            if self.push_to_datastore:
                # Create the DataStore resource
                storage = Storage(
                    base_url=self.base_endpoint,
                    dataset_id=self.dataset_id,
                    api_key=self.api_key,
                )
                resource_id = create_result['id']
                storage.create(resource_id, spec['schema'])
                storage.write(
                    resource_id,
                    Stream(temp_file.name, format='csv').open(),
                    method=self.push_to_datastore_method,
                )
        except Exception as e:
            raise e
        finally:
            os.unlink(filename)

    def _create_ckan_resource(self, request_params):
        create_response = make_ckan_request(
            self.resource_create_endpoint,
            api_key=self.api_key,
            method='POST',
            **request_params,
        )

        ckan_error = get_ckan_error(create_response)
        if ckan_error:
            log.exception(
                'CKAN returned an error when creating ' 'a resource: ' + json.dumps(ckan_error)
            )
            raise Exception
        return create_response['result']


if __name__ == '__main__':
    CkanDumper()()
