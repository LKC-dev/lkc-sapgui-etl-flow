from datetime import datetime
import json
from sapgui_flow.secrets.get_token import get_secret
from io import StringIO
import boto3
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("my_logger")

def pushToDataLake(datasource, name, data):
    logger.info(f'{name} - Started loading to S3')
    bucket = 'v4company-data-lake'
    csv_buffer = StringIO()
    data.to_csv(csv_buffer,encoding="utf8",index=False)
    s3_resource = boto3.resource('s3',
                                    aws_access_key_id=json.loads(get_secret("prod/SAAWS"))['Access Key Id'],
                                    aws_secret_access_key=json.loads(get_secret("prod/SAAWS"))['Secret Access Key'])
    s3_resource.Object(bucket,
                    datasource + "/" + str(time.strftime('%Y/%m/%d/')) + name).put(Body=csv_buffer.getvalue())
    logger.info("Data uploaded sucessfuly in V4 Data Lake -> " + name)
    logger.info(time.ctime())