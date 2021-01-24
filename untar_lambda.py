import logging
import tarfile
import boto3
from botocore.errorfactory import ClientError
import os
import json
import datetime
from io import BytesIO

if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger()


def key_exists(bucket, key):
    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return False
    return True


def get_filestream(bucket, key):
    s3_resource = boto3.resource('s3')
    bucket_resource = s3_resource.Bucket(bucket)
    s3_object = bucket_resource.Object(key)
    s3_object_response = s3_object.get()
    s3_file_bytes = BytesIO(s3_object_response['Body'].read())
    return s3_file_bytes


def put_file(bucket, key, source_bytes):
    if key_exists(bucket, key):
        logger.info('Key %s already exists, skipping', key)
    else:
        logger.info('Copying %s to bucket %s', key, bucket)
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket)
        s3_object = bucket.Object(key)
        s3_object.put(Body=source_bytes)


def lambda_handler(event, context):
    for record in event['Records']:
        try:
            message = json.loads(record["body"])
        except:
            logger.error("Message was not valid, message was: %s", record["body"])
            return

        bucket = message['bucket']
        key = message['key']

        status_key = "status/%s.processed" % key
        if not key_exists(bucket, status_key):
            logger.info('Untaring file %s in bucket %s', key, bucket)
            tar_filestream = get_filestream(bucket, key)
            with tarfile.open(name=None, mode="r:*", fileobj=tar_filestream) as tarball:
                for tar_file in tarball:
                    if not tar_file.isfile():
                        continue
                    file_data = tarball.extractfile(tar_file)
                    key = os.path.join('extracted/pdf', tar_file.name)
                    put_file(bucket, key, file_data.read())
            put_file(bucket, status_key, datetime.datetime.utcnow().isoformat().encode("utf-8"))
            tar_filestream.close()
        else:
            logger.info('Tar file %s already processed, skipping', key)


if __name__ == "__main__":
    event = {"Records": [{'body': json.dumps({"key": 'pdf/arXiv_pdf_1312_003.tar', 'bucket': 'arxivist'})}]}
    import time
    t1 = time.time()
    lambda_handler(event, None)
    logger.info("Total time: %0.1f (s)", time.time() - t1)
