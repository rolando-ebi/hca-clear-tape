import os
import requests
import json

API_URL_DEFAULT = 'https://dss.dev.data.humancellatlas.org/v1'
API_URL_ENV_KEY = 'CT_DSS_API'

class BundleService:

    def __init__(self):
        self.base_url = API_URL_DEFAULT
        if API_URL_ENV_KEY in os.environ:
            self.base_url = os.environ[API_URL_ENV_KEY]

    def get_files(self, bundle_uuid):
        bundle = self.get_bundle(bundle_uuid)
        return [file['uuid'] for file in bundle['files']]

    def get_bundle(self, uuid):
        bundle_url = '%s/bundles/%s' % (self.base_url, uuid)
        response = requests.get(bundle_url, {'replica': 'aws'})
        return json.loads(response.text)['bundle']