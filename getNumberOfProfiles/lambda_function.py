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
    - dict: status code, [count of profiles, profiles list]

Description: GET operation for /user/numberOfProfiles api
'''
def lambda_handler(event, context):
    # emailFromToken = event.get('email', None)
    emailFromToken = event.get('emailFake', None)
    
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
            return {
                'statusCode' : 200,
                'message' : "User Not Found"
            }
        
        # Return: No profile exists
        if 'profiles' not in res['Item'].keys():
            logger.error("Found user (" + emailFromToken + "), but no profile exists.")
            return {
                'statusCode' : 200,
                'message' : None,
                'body' : {
                    "countOfProfiles" : 0,
                    "profileId" : [],
                }
            }
        
        # Collect profile['id'] for return
        profileIds = []
        for item in res['Item']['profiles']:
            profileIds.append(item['id'])
        
        # Return: Operation Success
        logger.info("Operation success.")
        return {
            'statusCode' : 200,
            'message' : None,
            'body' : {
                "countOfProfiles": len(res['Item']['profiles']),
                "profileId": profileIds,
            }
        }
    # try 1: Read DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception({
            'statusCode' : 500,
            'message' : e.response['Error']['Message'],
        })