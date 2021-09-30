import json, os, logging, asyncio, time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    #emailFromToken = event.get('emailFake', None)
    
    db = boto3.resource('dynamodb')
    table = db.Table(os.environ['TABLE_NAME'])
    
    # try 1: Read DB
    try:
        res = table.get_item(Key={
            'email' : emailFromToken,
        })
        
        # Return: User not found
        if 'Item' not in res.keys():
            logger.info("User (" + emailFromToken + ") has not been found" )
            return {
                'statusCode' : 200,
                'body' : dict()
            }
        
        logger.info("Operation successful. Returning information.")
        
        returnDict = dict()
        for item in res['Item'].keys():
            if item != "email":
                returnDict[item] = res['Item'][item]
        
        return {
            'statusCode' : 200,
            'body' : returnDict,
        }
        
    # try 1: Read DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message' : "Internal Server Error"
        }))
