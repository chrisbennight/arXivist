import boto3
import logging
from botocore.errorfactory import ClientError
import urllib.request
import urllib.parse
import time
from io import StringIO
from xml.etree import ElementTree as ET
import json

ARXIV_ENDPOINT = 'http://export.arxiv.org/oai2'
REGION = 'us-east-1'
DYNAMO_TABLE = "arxivist_status"
S3_BUCKET = 'arxivist'
S3_PREFIX = 'delivered'
DATEFORMAT = '%Y-%m-%d %H:%M:%S'

if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger()

LAST_REQUEST = time.time()


def key_exists(bucket, key):
    s3_client = boto3.client('s3', region_name=REGION)
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return False
    return True


def put_file(bucket, key, source_bytes):
    if key_exists(bucket, key):
        logger.info('Key %s in bucket %s already exists, skipping', key, bucket)
    else:
        logger.info('Writing data to key %s in bucket %s', key, bucket)
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket)
        s3_object = bucket.Object(key)
        s3_object.put(Body=source_bytes)


def get_url_bytes(url):
    response = urllib.request.urlopen(url)
    return response.read()


def pdf_to_bucket(bucket, record_id, pdf_key_for_record):
    global LAST_REQUEST

    url = f'https://export.arxiv.org/pdf/{record_id}'

    # no more than 1 request every three seconds
    time_delta = 3 - (time.time() - LAST_REQUEST)
    if time_delta > 0:
        time.sleep(time_delta)
    pdf_bytes = get_url_bytes(url)
    LAST_REQUEST = time.time()

    if pdf_bytes[0:4] != '%PDF'.encode('utf-8'):
        logger.error('File %s was not a valid pdf', url)
        try:
            if 'PDF unavailable for' in pdf_bytes.decode('utf-8'):
                logger.info('PDF unavailable: %s - skipping', url)
                return
        except:
            pass
        raise ValueError('File %s was not a valid pdf' % url)
    logger.info('Writing %s to s3://%s/%s', url, bucket, pdf_key_for_record)
    put_file(bucket, pdf_key_for_record, pdf_bytes)


def process_record(record):
    record_id = record['metadata']['arXivRaw']['id']

    if "." not in record_id:
        logger.warning(record_id)

    if len(record_id.split(".")) != 2:
        logger.warning(record_id)

    if "-" in record_id:
        logger.warning(record_id)

    record_id_parts = record_id.split(".")
    datepart = record_id_parts[0]

    pdf_key_for_record = f"extracted/pdf/{datepart}/{record_id}.pdf"

    if not key_exists(S3_BUCKET, pdf_key_for_record):
        pdf_to_bucket(S3_BUCKET, record_id, pdf_key_for_record)
    else:
        logger.info('PDF %s found, skipping', pdf_key_for_record)

    metadata_key_for_record = f"extracted/metadata/{datepart}/{record_id}.json"

    put_file(S3_BUCKET, metadata_key_for_record, json.dumps(record).encode('utf-8'))


def element_to_dict(xml_element):
    xml_dict = {}
    for child in list(xml_element):
        if len(list(child)) > 0:
            xml_dict[child.tag] = element_to_dict(child)
        else:
            xml_dict[child.tag] = child.text or ''

    return xml_dict


def lambda_handler(event, context):
    dynamo_client = boto3.client('dynamodb', region_name=REGION)
    response = dynamo_client.get_item(TableName=DYNAMO_TABLE, Key={'setting_name': {'S': 'last_record'}})
    if 'Item' in response:
        item = response['Item']
        last_record_date = item['setting_value']["S"]
    else:
        last_record_date = None

    resumption_token = None
    response_datestamp = None

    base_endpoint = ARXIV_ENDPOINT + '?verb=ListRecords'
    while resumption_token != "":
        if resumption_token is None:
            new_endpoint = base_endpoint + "&metadataPrefix=arXivRaw"
            if last_record_date is not None:
                new_endpoint += "&from=%s" % last_record_date
            xml_bytes = get_url_bytes(new_endpoint)
        else:
            token_encoded = urllib.parse.urlencode({'resumptionToken': resumption_token})
            xml_bytes = get_url_bytes(base_endpoint + '&' + token_encoded)

        it = ET.iterparse(StringIO(xml_bytes.decode('utf-8')))

        # get rid of namespaces
        for _, el in it:
            _, _, el.tag = el.tag.rpartition('}')

        dom_root = it.root

        response_datestamp = dom_root.findall("responseDate")[0].text.split("T")[0]

        for record in dom_root.findall("ListRecords/record"):
            record_dict = element_to_dict(record)
            process_record(record_dict)

        resumption_token = dom_root.findall("ListRecords/resumptionToken")[0].text
        logger.info(resumption_token)

    if response_datestamp is not None:
        logger.info('Setting new checkpoint to %s', response_datestamp)
        resp = dynamo_client.update_item(
            TableName=DYNAMO_TABLE,
            Key={'setting_name': {'S': 'last_record'}},
            AttributeUpdates={
                'setting_value': {'Value': {'S': response_datestamp}}
            }
        )


if __name__ == "__main__":
    lambda_handler(None, None)
