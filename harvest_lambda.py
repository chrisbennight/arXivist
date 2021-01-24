from sickle import Sickle
import boto3
import datetime
import logging

ARXIV_ENDPOINT = 'http://export.arxiv.org/oai2'
REGION = 'us-east-1'
DYNAMO_TABLE = "arxivist_status"
S3_BUCKET = 'arxivist'
S3_PREFIX = 'delivered'
DATEFORMAT = '%Y-%m-%d %H:%M:%S'




def lambda_handler(event, context):

    dynamo_client = boto3.client('dynamodb', region_name=REGION)
    response = dynamo_client.get_item(TableName=DYNAMO_TABLE, Key={'setting_name': {'S': 'last_record'}})
    if 'Item' in response:
        item = response['Item']
        last_record_date = item['setting_value']["S"]
    else:
        last_record_date = '2008-11-26'

    s = Sickle('https://export.arxiv.org/oai2', max_retries=2)

    records = s.ListRecords(metadataPrefix='arXivRaw', ignore_deleted=True, **{'from': last_record_date})

    for record in records:
        xml_text = record.raw
        record_id = record.metadata['id'][0]
        record_category = record.metadata['categories'][0]
        last_record_date = record.header.datestamp



        resp = dynamo_client.update_item(
            TableName=DYNAMO_TABLE,
            Key={'setting_name': {'S': 'last_record'}},
            AttributeUpdates={
                'setting_value': {'Value': {'S': last_record_date}}
            }
        )
        print(resp)


if __name__ == "__main__":
  lambda_handler(None, None)