import requests
import time
import json

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
        if 200 <= response.status_code < 300:
            return response.json()['bundle']
        else:
            raise(Exception("Error: status code %s, url %s, body: %s" % (str(response.status_code), response.url, json.dumps(response.json()))))

    def get_bundle_with_retries(self, uuid, attempt_num, max_attempts):
        if attempt_num == max_attempts:
            raise(Exception(str(max_attempts) + " attempts failed to fetch bundle with uuid " + uuid + "\n"))
        else:
            try:
                return self.get_bundle(uuid)
            except Exception as e:
                print(str(e))
                time.sleep(1)
                return self.get_bundle_with_retries(uuid, attempt_num + 1, max_attempts)



    def get_bundles(self, uuids):
        return map(lambda uuid: self.get_bundle(uuid), uuids)


    def get_name_size_hash_triples(self, bundle_json):
        '''
        returns triple (filename, filesize, md5_hash) for each fastq file in a bundle
        :param bundle_json:
        :return:
        '''
        return [self.bundle_file_to_file_size_hash_dict(file) for file in filter(is_fastq_file, bundle_json['files'])]

    def bundle_file_to_file_size_hash_dict(self, file_dict):
        name = file_dict['name']
        size = file_dict['size']
        s3etag = file_dict['s3_etag']
        sha1 = file_dict['sha1']
        sha256 = file_dict['sha256']
        crc32 = file_dict['crc32c']

        return {'name': name, 'size': size, 's3_etag': s3etag, 'sha256': sha256, 'sha1': sha1, 'crc32c': crc32}


def is_data_file(file_json):
    return "dcp-type=data" in file_json['content-type']


def is_fastq_file(file_json):
    return is_data_file(file_json) and "fastq" in file_json['name']
