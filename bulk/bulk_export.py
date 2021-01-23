import boto3
import os
import xml.etree.ElementTree as ET
import daiquiri
import logging
from botocore.errorfactory import ClientError
import tarfile


daiquiri.setup(level=logging.INFO)
log = daiquiri.getLogger(__name__)

SOURCE_BUCKET = "arxiv"
DESTINATION_BUCKET = "arxivist"


def key_exists(bucket, key):

    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return False

    return True

def put_file(bucket, key, source_bytes):
    if key_exists(bucket, key):
        log.info('Key %s already exists, skipping', key)
    else:
        log.info('Copying %s to bucket %s', key, bucket)
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket)
        object = bucket.Object(key)
        object.put(Body=source_bytes)

def get_file(bucket, key, output_dir, requester_pays=False):
    s3_resource = boto3.resource('s3')
    bucket_resource = s3_resource.Bucket(bucket)
    filename = os.path.basename(key)
    if output_dir is None:
        output_dir = ""
    if output_dir != "" and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file = os.path.join(output_dir, filename)
    if requester_pays:
        extraArgs = {'RequestPayer': 'requester'}
    else:
        extraArgs = None
    if os.path.exists(output_file):
        log.info("File %s already exists, skipping", output_file)
    else:
        log.info('Downloading key %s from bucket %s', key, bucket)
        bucket_resource.download_file(key, output_file, ExtraArgs=extraArgs)
    return output_file


def copy_file(source_bucket, destination_bucket, key):
    s3_resource = boto3.resource('s3')
    copy_source = {
        'Bucket': source_bucket,
        'Key': key
    }
    bucket = s3_resource.Bucket(destination_bucket)
    if key_exists(destination_bucket, key):
        log.info('Key %s already exists, skipping', key)
    else:
        log.info('Copying %s', key)
        bucket.copy(copy_source, key, ExtraArgs={'RequestPayer': 'requester'})


def get_files_from_manifest(manifest_file):
    for event, elem in ET.iterparse(manifest_file):
        if event == 'end':
            if elem.tag == 'filename':
                fname = elem.text
                copy_file(fname, SOURCE_BUCKET, DESTINATION_BUCKET)

def untar_s3_prefix(bucket, prefix):
    s3_client = boto3.client('s3')

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        for obj in page['Contents']:
            if obj['Key'].endswith('.tar'):
                tar_filename = get_file(bucket, obj['Key'], 'tar-temp', requester_pays=False)
                with open(tar_filename, 'rb') as tar_handle:
                    with tarfile.open(name=None, mode="r:*", fileobj=tar_handle) as tarball:
                        for tar_file in tarball:
                            if not tar_file.isfile():
                                continue
                            file_data = tarball.extractfile(tar_file)
                            key = os.path.join('pdf', tar_file.name)
                            put_file(DESTINATION_BUCKET, key, file_data.read())

def main():
    #Get manifests
    #get_file('pdf/arXiv_pdf_manifest.xml', "manifests", requester_pays=True)

    #Copy all files from one bucket to another
    #get_files_from_manifest('manifests/arXiv_pdf_manifest.xml')

    untar_s3_prefix(DESTINATION_BUCKET, 'pdf')




if __name__ == "__main__":
    main()