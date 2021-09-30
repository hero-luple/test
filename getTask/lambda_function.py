import os, logging, json
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
taskTypeList = [0, 1, 2, 3, 4]
def lambda_handler(event, context):
    emailFromToken = event.get("email", None)
    all = event.get("all", None)
    taskId = event.get("taskId", None)
    taskType = event.get("taskType", None)
    since = event.get("since", None)
    
    parameterCheck(all, taskId, taskType, since)
    
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["TABLE_NAME"])
    
    try:
        keyCondition = Key("email").eq(emailFromToken)
        expression = None
        if not all:
            if taskId != "":
                keyCondition &= Key("taskId").eq(taskId)
            if taskType != None or since is not None:
                expression = createExpression(taskType, since)

        if expression is None:
            response = table.query(
                ProjectionExpression = "taskType, taskId, startTime, elapsedTime",
                KeyConditionExpression = keyCondition
            )
        else:
            response = table.query(
                ProjectionExpression = "taskType, taskId, startTime, elapsedTime",
                KeyConditionExpression = keyCondition,
                FilterExpression = expression["expression"][:-5],
                ExpressionAttributeValues = expression["values"]
            )
            
        logger.info("Operation successful. Returning information.")
        return {
            "statusCode": 200,
            "body": response["Items"]
        }
    except ClientError as e:
        logger.error(e.response)
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message' : "Internal Server Error"
        }))
        
def createExpression(taskType, since):
    expression = {
        "expression" : "",
        "values" : {}
    }
    if taskType is not None:
        expression["expression"] += "taskType=:taskType and "
        expression["values"][":taskType"] = taskType
    if since is not None:
        expression["expression"] += "startTime>=:since and "
        expression["values"][":since"] = since
    return expression
    
def parameterCheck(all, taskId, taskType, since):
    errorMsg = {}
    if taskType is not None and taskType not in taskTypeList:
        errorMsg = {
            "statusCode": 400,
            "message": "Bad Request"
        }
    if type(all) != type(True):
        errorMsg = {
            "statusCode": 400,
            "message": "Bad Request"
        }
    if not all:
        if taskType is None and since is None and taskId == "":
            errorMsg = {
                "statusCode": 400,
                "message": "Bad Request"
            }
    
    if errorMsg != {}:
        raise Exception(json.dumps({
            "statusCode" : errorMsg['statusCode'],
            "message" : errorMsg['message']
        }))