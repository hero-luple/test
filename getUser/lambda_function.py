import json, os, logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

'''
lambda_handler function

@input parameter:
    - emailFromToken: get user's e-mail address from Auth Token
             PK of database
@return:
    - dict: status code, [body], error

Description: GET operation for /user api
'''
def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    #emailFromToken = event.get('emailFake', None)
    
    db = boto3.resource('dynamodb')
    table = db.Table(os.environ['TABLE_NAME'])
    # table = db.Table('asd')
    
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
                'body' : dict(),
            }
        
        logger.info("Operation successful. Returning information.")
        
        returnDict = dict()
        for item in res['Item'].keys():
            if item != "email":
                logger.info(item)
                returnDict[item] = res['Item'][item]
        
        return {
            'statusCode' : 200,
            'body' : returnDict,
        }
        
    # try 1: Read DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception({
            'statusCode' : 500,
            'message' : "Internal Server Error",
        })