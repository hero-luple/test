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
    - dict: status code

Description: DELETE operation for /user/profile api
'''
def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    profileId = event.get('profileId', None)
    
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
        
        # Return: profiles section not found
        if 'profile' not in res['Item'].keys():
            logger.error("profiles section does not exist.")
            raise Exception(json.dumps({
                'statusCode' : 400,
                'message': "Bad Request"
            }))
        
        # try 3: Update DB
        try:
            updateVals = dict()
            updateVals[":i"] = dict()
            
            res = table.update_item(Key={
                'email' : emailFromToken,
            },
            # UpdateExpression="remove profile")
            UpdateExpression="set profile = :i",
            ExpressionAttributeValues=updateVals)
            
            # Return: Operation success
            return {
                'statusCode' : 200
            }
            
        # try 3: Update DB
        except ClientError as e:
            logger.error(e.response['Error']['Message'])
            raise Exception(json.dumps({
                'statusCode' : 500,
                'message': "Internal Server Error"
            }))
            
    # try 1: Read DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message' : "Internal Server Error"
        }))
        
def deleteProfiles(table, emailFromToken, profileId, profileIds):
    if profileId == '*':
        # try 2: Update DB
        try:
            res = table.update_item(Key={
                'email' : emailFromToken,
            },
            UpdateExpression="remove profiles")
            
            res = table.update_item(Key={
                'email' : emailFromToken,
            },
            UpdateExpression="set profiles = :q",
            ExpressionAttributeValues={
                ":q": []
            })
            
            # Return: Operation success
            return {
                'statusCode' : 200
            }
            
        # try 2: Update DB
        except ClientError as e:
            logger.error(e.response['Error']['Message'])
            raise Exception(json.dumps({
                'statusCode' : 500,
                'message': "Internal Server Error"
            }))
    else:
        # Return: Specific profile not found
        if profileId not in profileIds:
            logger.error("Specific profile does not exist.")
            raise Exception(json.dumps({
                'statusCode' : 400,
                'message' : "Bad Request"
            }))
        
        idx = profileIds.index(profileId)
        
        # try 3: Update DB
        try:
            res = table.update_item(Key={
                'email' : emailFromToken,
            },
            UpdateExpression="remove profiles[" + str(idx) + "]")
            
            # Return: Operation success
            return {
                'statusCode' : 200
            }
            
        # try 3: Update DB
        except ClientError as e:
            logger.error(e.response['Error']['Message'])
            raise Exception(json.dumps({
                'statusCode' : 500,
                'message': "Internal Server Error"
            }))