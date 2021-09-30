import os, logging, json
import datetime, time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

taskType = [0, 1, 2, 3, 4]

def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    
    taskId = event.get("taskId", None)
    body = event.get("body-json", None)

    items = ["taskType", "startTime", "elapsedTime"]
    if body is not None:
        checkNonExistCapability(body, items)
        
    expression = createExpression(body, items, taskId)

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["TABLE_NAME"])
        
    try:
        keyCondition = Key("email").eq(emailFromToken) & Key("taskId").eq(taskId)
        queryResponse = table.query(
                KeyConditionExpression=keyCondition
            )
        if queryResponse['Count'] == 0:
            logger.error("User (" + emailFromToken + ")'s task is not found.")
            raise Exception(json.dumps({
                'statusCode' : 400,
                'message' : "Bad Request"
            }))
        
        updateResponse = table.update_item(
            Key={
                "email": emailFromToken,
                "taskId": taskId
            },
            UpdateExpression=expression["expression"][:-1],
            ExpressionAttributeValues=expression["values"],
            ReturnValues="UPDATED_NEW"
        )
        logger.info("Operation successful. Return 200.")
        return {
            "statusCode": 200
        }
        
    except ClientError as e:
        logger.error(e.response["Error"]["Message"])
        raise Exception(json.dumps({
                "statusCode": 500,
                "message": "%s :%s" % (e.response["Error"]["Code"], e.response["Error"]["Message"])
            }))

def createExpression(body, items, taskId):
    expression = {"expression": "set ", "values" : {} }
    bodyParams = {}
    for item in items:
        param = body.get(item, None)
        if param is not None:
            bodyParams[item] = param
            expression["expression"] += "%s=:%s," % (item, item)
            expressionKey = ":"+item
            expression["values"][expressionKey] = param
    parameterCheck(bodyParams, taskId.split("-")[1])
    return expression
        
def parameterCheck(bodyParams, date):
    errorMsg = {}
    if bodyParams.get("startTime") is not None:
        todayTimestamp = convertUnixtime(date)
        tommorowTimestamp= todayTimestamp + 24 * 60 * 60
        if todayTimestamp > bodyParams["startTime"] or bodyParams["startTime"] >= tommorowTimestamp:
            errorMsg = {
                "statusCode": 400,
                "message": "Bad Request"
            }
    if bodyParams.get("taskType") is not None:
        if bodyParams["taskType"] not in taskType:
            errorMsg = {
                "statusCode": 400,
                "message": "Bad Request"
            }
    if bodyParams.get("elapsedTime") is not None:
        if bodyParams["elapsedTime"] < 0:
            errorMsg = {
                "statusCode": 400,
                "message": "Bad Request"
            }
    
    if errorMsg != {}:
        raise Exception(json.dumps({
            "statusCode" : errorMsg['statusCode'],
            "message" : errorMsg['message']
        }))

        
def convertUnixtime(dateTime):
    unixtime = int(datetime.datetime.strptime(dateTime, '%Y%m%d').timestamp())
    return unixtime
    
def checkNonExistCapability(body, items):
    visited = []
    checkItems(body, visited, items)
    
    return True

def checkItems(body, visited, taskItems):
    for key in body.keys():
        if key not in visited:
            logger.info(key)
            if key not in taskItems:
                raise Exception(json.dumps({
                    "statusCode": 400,
                    "message": "Bad Request"
                }))

            visited.append(key)
            checkItems(body, visited, taskItems)
        
    return True