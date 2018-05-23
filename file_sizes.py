from BundleService import BundleService
from Config import env

import sys
import traceback
import json


def parse_bundles_from_file(bundle_list_file_name):
    with open(bundle_list_file_name, encoding='utf-8') as bundle_list_file:
        return [str(bundle_uuid.rstrip()) for bundle_uuid in bundle_list_file.readlines()]


def bundles_details_dicts_to_tsv(bundles_details):
    for (bundle_uuid, files_details) in bundles_details.items():
        for file_details in files_details:
            yield bundle_uuid + "\t" + file_details['name'] + "\t" + str(file_details['size']) + "\t" + file_details['hash'] + "\n"

def run():
    bundle_list_file = sys.argv[1]
    bundle_uuids = parse_bundles_from_file(bundle_list_file)

    prod_config = env.PROD
    bundle_service = BundleService(prod_config)

    # map of bundle_uuid -> list of (filename, filesize, filehash) triples
    file_sizes_dict = dict()
    overall_size_count = 0

    for uuid in bundle_uuids:
        bundle_json = bundle_service.get_bundle(uuid)
        files_details_for_bundle = bundle_service.get_name_size_hash_triples(bundle_json)

        file_sizes_dict[uuid] = files_details_for_bundle

        for file_details in files_details_for_bundle:
            file_size = file_details['size']
            overall_size_count += file_size

    with open('bundles_details.json', 'w') as outfile_json:
        json.dump(file_sizes_dict, outfile_json)

    with open('bundles_details.tsv', 'w') as outfile_tsv:
        for tsv_line in bundles_details_dicts_to_tsv(file_sizes_dict):
            outfile_tsv.write(tsv_line)

    print("Total file size in bytes: " + str(overall_size_count) + "\n")


if __name__ == '__main__':
    try:
        run()
    except Exception as exception:
        traceback.print_exc()
        sys.exit(1)