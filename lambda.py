from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
import boto3
import datetime


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
        last_record_date = item['setting_value']
    else:
        last_record_date = datetime.datetime.strptime('2007-05-23 00:00:00', '%Y-%m-%d %H:%M:%S')

    registry = MetadataRegistry()
    registry.registerReader('oai_dc', oai_dc_reader)
    oai_client = Client(ARXIV_ENDPOINT, registry)
    oai_client.updateGranularity()

    for record in oai_client.listRecords(metadataPrefix='oai_dc',  from_=last_record_date):
      print(record[0].datestamp(), end=' ')
      print(record[1]['title'][0])


if __name__ == "__main__":
  lambda_handler(None, None)