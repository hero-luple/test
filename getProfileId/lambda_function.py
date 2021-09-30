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
    - dict: profile json object

Description: GET operation for /user/profile api
'''
def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    
    db = boto3.resource('dynamodb')
    table = db.Table(os.environ['TABLE_NAME'])
    
    # try 1: Read DB
    try:
        res = table.get_item(Key={
            'email' : emailFromToken,
        })
        
        # Return: User not found
        if 'Item' not in res.keys():
            logger.error("User (" + emailFromToken + ")not found.")
            raise Exception(json.dumps({
                'statusCode' : 400,
                'message': "User Not Found"
            }))
        
        # Return: No profile exists
        if 'profile' not in res['Item'].keys():
            logger.error("Found user (" + emailFromToken + "), but no profile exists.")
            raise Exception(json.dumps({
                'statusCode' : 400,
                'message': "Bad Request"
            }))
        
        profile = res['Item']['profile']
        
        # Return: Operation Success
        logger.info("Operation success.")
        return {
            'statusCode' : 200,
            'body' : profile
        }
        
    # try 1: Read DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message' : "Internal Server Error",
        }))