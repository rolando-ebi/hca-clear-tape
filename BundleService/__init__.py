import requests

from Config import env


class BundleService:

    def __init__(self, _env=env.DEV):
        self.env = _env

    def iterate_indexed_files(self, bundle_uuid):
        bundle = self.get_bundle(bundle_uuid)
        for file in bundle['files']:
            if (file['indexed']):
                file_url = '%s/files/%s' % (self.env.dss_api_url, file['uuid'])
                response = requests.get(file_url, {'replica': 'aws'})
                yield response.json()


    def get_bundle(self, uuid):
        bundle_url = '%s/bundles/%s' % (self.env.dss_api_url, uuid)
        response = requests.get(bundle_url, {'replica': 'aws'})
        return response.json()['bundle']

    def get_bundles(self, uuids):
        return map(lambda uuid: self.get_bundle(uuid), uuids)


    def get_name_size_hash_triples(self, bundle_json):
        '''
        returns triple (filename, filesize, md5_hash) for each fastq file in a bundle
        :param bundle_json:
        :return:
        '''
        return [{'name': file['name'], 'size': file['size'], 'hash': file['s3_etag']} for file in filter(is_fastq_file, bundle_json['files'])]


def is_data_file(file_json):
    return "dcp-type=data" in file_json['content-type']


def is_fastq_file(file_json):
    return is_data_file(file_json) and "fastq" in file_json['name']
