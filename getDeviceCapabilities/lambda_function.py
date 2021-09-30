import json, os, logging, asyncio, time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

'''
lambda_handler function

@input parameter:
    - None

@return:
    - List: Capable languages
        0: EN
        1: KR
        2: JP
'''
def lambda_handler(event, context):
    languageCaps = [0, 1, 2]
    
    return {
        'statusCode': 200,
        'body': {
            "langCap": languageCaps,
        }
    }
