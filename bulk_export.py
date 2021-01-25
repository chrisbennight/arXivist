import boto3
import os
import xml.etree.ElementTree as ET
import logging
from botocore.errorfactory import ClientError
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

SOURCE_BUCKET = "arxiv"
DESTINATION_BUCKET = "arxivist"
UNTAR_QUEUE = "arxivist_untar.fifo"
REGION = "us-east-1"


def key_exists(bucket, key):
    s3_client = boto3.client('s3', region_name=REGION)
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return False

    return True


def get_file(bucket, key, output_dir, requester_pays=False):
    s3_resource = boto3.resource('s3', region_name=REGION)
    bucket_resource = s3_resource.Bucket(bucket)
    filename = os.path.basename(key)
    if output_dir is None:
        output_dir = ""
    if output_dir != "" and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file = os.path.join(output_dir, filename)
    if requester_pays:
        extra_args = {'RequestPayer': 'requester'}
    else:
        extra_args = None
    if os.path.exists(output_file):
        logger.info("File %s already exists, skipping", output_file)
    else:
        logger.info('Downloading key %s from bucket %s', key, bucket)
        bucket_resource.download_file(key, output_file, ExtraArgs=extra_args)
    return output_file


def copy_file(source_bucket, destination_bucket, key):
    s3_resource = boto3.resource('s3', region_name=REGION)
    sqs_resource = boto3.resource('sqs', region_name=REGION)
    untar_queue = sqs_resource.get_queue_by_name(QueueName=UNTAR_QUEUE)
    copy_source = {
        'Bucket': source_bucket,
        'Key': key
    }
    bucket = s3_resource.Bucket(destination_bucket)
    if key_exists(destination_bucket, key):
        logger.info('Key %s already exists, skipping', key)
    else:
        logger.info('Copying %s', key)
        bucket.copy(copy_source, key, ExtraArgs={'RequestPayer': 'requester'})

        status_key = "status/%s.processed" % key

        if not key_exists(destination_bucket, status_key):
            untar_queue.send_message(
                MessageBody=json.dumps({'key': key, 'bucket': destination_bucket}),
                MessageGroupId=key,
                MessageDeduplicationId=key
            )
            logger.info('Queued key %s in bucket %s', key, destination_bucket)
        else:
            logger.info("File %s already processed in bucket %s", key, destination_bucket)


def get_files_from_manifest(manifest_file):
    for event, elem in ET.iterparse(manifest_file):
        if event == 'end':
            if elem.tag == 'filename':
                file_key = elem.text
                copy_file(SOURCE_BUCKET, DESTINATION_BUCKET, file_key)


def populate_sqs_s3_prefix(bucket, prefix):
    s3_client = boto3.client('s3', region_name=REGION)
    sqs_resource = boto3.resource('sqs', region_name=REGION)
    untar_queue = sqs_resource.get_queue_by_name(QueueName=UNTAR_QUEUE)

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        for obj in page['Contents']:
            if obj['Key'].endswith('.tar'):
                status_key = "status/%s.processed" % obj['Key']
                if not key_exists(bucket, status_key):
                    untar_queue.send_message(
                        MessageBody=json.dumps({'key': obj['Key'], 'bucket': bucket}),
                        MessageGroupId=obj['Key'],
                        MessageDeduplicationId=obj['Key']
                    )
                    logger.info('Queued key %s in bucket %s', obj['Key'], bucket)
                else:
                    logger.info("File %s already processed in bucket %s", obj["Key"], bucket)


def main():
    # Get manifests
    get_file(SOURCE_BUCKET, 'pdf/arXiv_pdf_manifest.xml', "manifests", requester_pays=True)

    # Copy all files from one bucket to another
    get_files_from_manifest('manifests/arXiv_pdf_manifest.xml')

    # Only needed for testing
    #populate_sqs_s3_prefix(DESTINATION_BUCKET, 'pdf')


if __name__ == "__main__":
    main()
