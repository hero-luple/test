import os, datetime, logging, json
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    body = event.get("body-json", None)
    date = event.get("didaryDate", None)
    dateCheck(date)
    
    items = ["timeToSleep", "numOfWakeUp", "differenceTime", "disturbance", "textMessage", "sleepScore"]
    limitArray = [4, 4, 5, 11, 255, 5.0]
    floatItems = ["sleepScore"]
    expression = createExpression(body, items, floatItems, limitArray)

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["TABLE_NAME"])
        
    try:
        keyCondition = Key("email").eq(emailFromToken) & Key("date").eq(int(date))
        if not isDataExist:
            logger.error("User (" + emailFromToken + ")'s sleepDiary is not found.")
            raise Exception(json.dumps({
                "statusCode" : 400,
                "message" : "Bad Request"
            }))
        
        updateResponse = table.update_item(
            Key={
                "email": emailFromToken,
                "diaryDate": int(date)
            },
            UpdateExpression = expression["expression"][:-1],
            ExpressionAttributeValues=expression["values"],
            ReturnValues="UPDATED_NEW"
        )
        logger.info(updateResponse)
        return {
            "statusCode": 200
        }
    except ClientError as e:
        logger.error(e.response["Error"]["Message"])
        raise Exception(json.dumps({
            "statusCode" : 500,
            "message" : "Internal Server Error"
        }))
    
def dateCheck(date):
    try:
        datetime.datetime.strptime(date, '%Y%m%d')
    except ValueError:
        errorMsg = {
            "statusCode": 400,
            "message": "Bad Request"
        }
        raise ValueError(errorMsg)
        
def createExpression(body, items, floatItems, limitArray):
    expression = {"expression" : "set ", "values" : {}}
    bodyParams = {}
    for item in items:
        param = body.get(item, None)
        if param is not None:
            bodyParams[item] = param
            expression["expression"] += "%s=:%s," % (item, item)
            expressionKey = ":"+item
            if item not in floatItems:
                expression["values"][expressionKey] = param
            else:
                expression["values"][expressionKey] = Decimal(str(param))
    parameterCheck(items, limitArray, bodyParams)
    return expression
    
def isDataExist(table, keyCondition):
    queryResponse = table.query(
            KeyConditionExpression=keyCondition
        )
    return queryResponse["Count"] != 0

def parameterCheck(items, limitArray, bodyParams):
    errorMsg = {}
    for idx, item in enumerate(items):
        if bodyParams.get(item) is not None:
            if item == "textMessage":
                if len(bodyParams[item]) > limitArray[idx]:
                    errorMsg = {
                        "statusCode": 400,
                        "message": "Bad Request"
                    }
            elif item == "disturbance":
                for disturbance in bodyParams[item]:
                    if 0 > disturbance or disturbance > limitArray[idx]:
                        errorMsg = {
                            "statusCode": 400,
                            "message": "Bad Request"
                        }
            else:
                if 0 > bodyParams[item] or bodyParams[item] > limitArray[idx]:
                    errorMsg = {
                        "statusCode": 400,
                        "message": "Bad Request"
                    }
    if errorMsg != {}:
        raise Exception(json.dumps({
            "statusCode" : errorMsg['statusCode'],
            "message" : errorMsg['message']
        }))