import json, os, logging, asyncio, time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

categories = ['lang', 'agreement']
validItems = ["termsAndCondition", "userInfoPrivacy", "userInfoCollectPrivacy", "gpsService", "marketingPolicy"]

def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    body = event.get('body-json', None)
    
    if 'lang' in body:
        if body['lang'] not in [0, 1, 2]:
            raise Exception(json.dumps({
                'statusCode': 400,
                'message': "Bad Request"
            }))
    
    checkNonExistCapability(body, "device")
    
    db = boto3.resource('dynamodb')
    table = db.Table(os.environ['TABLE_NAME'])
    
    # try 1: Read DB
    try:
        res = table.get_item(Key={
            'email' : emailFromToken,
        })
        
        # Return: User not found
        if 'Item' not in res.keys():
            logger.info("User (" + emailFromToken + ") has not been found. Creating new row." )
            ret = createSettings(table, emailFromToken, body)
        else:
            logger.info("Found user. Updating info." )
            ret = updateSettings(table, emailFromToken, body)
        
        logger.info("Operation successful. Returning information.")
        
        return ret
        
    # try 1: Read DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message' : "Internal Server Error"
        }))

def createSettings(table, emailFromToken, body):
    postItem = dict()
    postItem['email'] = emailFromToken
    postItem['lang'] = None
    postItem['agreement'] = dict()
    
    for item in body.keys():
        if item not in categories:
            raise Exception(json.dumps({
                'statusCode': 400,
                'message': 'Bad Request'
            }))
            
    if 'lang' in body:
        postItem['lang'] = body['lang']
    else:
        postItem['lang'] = None
    
    if 'agreement' in body:
        for item in body['agreement'].keys():
            if item not in validItems:
                raise Exception(json.dumps({
                    'statusCode': 400,
                    'message': 'Invalid Method'
                }))
            else:
                try:
                    postItem['agreement'][item] = body['agreement'][item]
                except:
                    continue
                
    table.put_item(
        Item=postItem
        )
    
    logger.info("Post settings successful.")
    
    return {
        'statusCode': 200,
    }
    
def updateSettings(table, emailFromToken, body):
    updateExpression = "set"
    updateVals = dict()
    for item in body.keys():
        if item == 'agreement':
            for subitem in body[item].keys():
                updateExpression += " " + str(item) + "." + str(subitem) + " = :" + str(subitem) + ","
                updateVals[":"+str(subitem)] = body[item][subitem]
        else:
            updateExpression += " " + str(item) + " = :" + str(item) + ","
            updateVals[":"+str(item)] = body[item]
    updateExpression = updateExpression[:-1]
    
    # try 1: Update DB
    try:
        res = table.update_item(Key={
            'email' : emailFromToken,
        },
        UpdateExpression=updateExpression,
        ExpressionAttributeValues=updateVals)
        
        return {
            'statusCode': 200,
        }
    
    # try 1: Update DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message': "Internal Server Error"
        }))
        
def checkNonExistCapability(body, mode):
    userItems = ["userName", "problems", "problem", "priority", "age", "gps", "latitude", "longitude", "sex", "profile", "wakeUpTime", "hh", "mm", "sleepTime", "effectiveDays"]
    profileItems = ["wakeUpTime", "hh", "mm", "sleepTime", "effectiveDays"]
    deviceItems = ["lang", "agreement", "termsAndCondition", "userInfoPrivacy", "userInfoCollectPrivacy", "gpsService", "marketingPolicy"]

    visited = []
    
    checkItemsDfs(body, visited, userItems, profileItems, deviceItems, mode)
    
    return True

def checkItemsDfs(body, visited, userItems, profileItems, deviceItems, mode):
    for key in body.keys():
        if key not in visited:
            logger.info(key)
            if mode == "user":
                if key not in userItems:
                    raise Exception(json.dumps({
                        'statusCode': 400,
                        'message': "Bad Request"
                    }))
            elif mode == "profile":
                if key not in profileItems:
                    raise Exception(json.dumps({
                        'statusCode': 400,
                        'message': "Bad Request"
                    }))
            elif mode == "device":
                if key not in deviceItems:
                    raise Exception(json.dumps({
                        'statusCode': 400,
                        'message': "Bad Request"
                    }))
                    
            visited.append(key)
            if type(body[key]) == dict:
                checkItemsDfs(body[key], visited, userItems, profileItems, deviceItems, mode)
            elif type(body[key]) == list:
                for item in body[key]:
                    if type(item) == dict:
                        checkItemsDfs(item, visited, userItems, profileItems, deviceItems, mode)
    return True