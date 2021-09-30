import os, logging, json
import datetime, time
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

taskType = [0, 1, 2, 3, 4]

def lambda_handler(event, context):
    emailFromToken = event.get("email", None)
    body = event.get("body-json", None)
    items = ["taskType", "startTime", "elapsedTime"]
    if body is not None:
        checkNonExistCapability(body, items)
        

    params = parseBodyparams(items, body)
    
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["TABLE_NAME"])
    
    # taskId format: userName-startDate-currentServerTime
    taskId = createTaskId(emailFromToken, params["startTime"])
    
    inputItem = {
        "email" : emailFromToken,
        "taskId" : taskId
    }
    inputItem.update(params)
    
    try:
        table.put_item(
            Item = inputItem
        )
        logger.info("Post task successful.")
        return {
            "statusCode": 200
        }
        
    except ClientError as e:
        logger.error(e.response["Error"]["Message"])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message' : "Internal Server Error"
        }))
    
def createTaskId(email, startTime):
    userName = email.split("@")[0]
    startDate = convertDatetime(startTime)
    currentServerTime = int(time.time())
    return "%s-%d-%d" % (userName, startDate, currentServerTime)
    
def convertDatetime(unixtime):
    date = datetime.datetime.fromtimestamp(unixtime).strftime("%Y%m%d")
    return int(date)

def parseBodyparams(items, body):
    params = {}
    for item in items:
        param = body.get(item, None)
        if param is not None:
            params[item] = param
    parameterCheck(params)
    return params
    
def parameterCheck(bodyParams):
    errorMsg = {}
    if bodyParams.get("taskType") is not None:
        if bodyParams["taskType"] >= len(taskType):
            logger.info("error.")
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