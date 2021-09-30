import os, datetime, logging, json
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    body = event.get("body-json", None)
    date = body["diaryDate"]
    
    dateCheck(date)
    
    items = ["timeToSleep", "numOfWakeUp", "differenceTime", "disturbance", "textMessage", "sleepScore"]
    limitArray = [4, 4, 5, 11, 255, 5.0]
    bodyParams = parseBodyparams(items, body, limitArray)
    bodyParams["sleepScore"] = Decimal(str(bodyParams["sleepScore"]))
                
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["TABLE_NAME"])
    
    inputItem = {
        "email" : emailFromToken,
        "diaryDate": int(date)
    }
    inputItem.update(bodyParams)
    
    try:
        table.put_item(
            Item = inputItem
        )
        logger.info("Post sleepDiary successful.")
        return {
            "statusCode": 200
        }
    except ClientError as e:
        logger.error(e.response["Error"]["Message"])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message' : "Internal Server Error"
        }))
            
def dateCheck(date):
    try:
        datetime.datetime.strptime(date, '%Y%m%d')
    except ValueError:
        raise Exception(json.dumps({
            "statusCode": 400,
            "message": "Invalid value error: 'date'(path) must be 'yyyyMMdd' date format, input 'date': %s" % date
        }))
        
def parseBodyparams(items, body, limitArray):
    params = {}
    for item in items:
        param = body.get(item, None)
        if param is not None:
            params[item] = param
    parameterCheck(items, limitArray, params)
    return params
            
def parameterCheck(items, limitArray, bodyParams):
    errorMsg = {}
    for idx, item in enumerate(items):
        if bodyParams.get(item) is not None:
            if item == "textMessage":
                if len(bodyParams[item]) > limitArray[idx]:
                    errorMsg = {
                        "statusCode": 400,
                        "message": "Invalid value error: 'textMessage'(body) must not exceed 255 characters."
                    }
            elif item == "disturbance":
                for disturbance in bodyParams[item]:
                    if 0 > disturbance or disturbance > limitArray[idx]:
                        errorMsg = {
                            "statusCode": 400,
                            "message": "Invalid value error: '%s'(body) must be 0 to %d int array" % (item, limitArray[idx])
                        }
            else:
                if 0 > bodyParams[item] or bodyParams[item] > limitArray[idx]:
                    errorMsg = {
                        "statusCode": 400,
                        "message": "Invalid value error: '%s'(body) must be 0 to %d" % (item, limitArray[idx])
                    }
    if errorMsg != {}:
        raise Exception(json.dumps({
            "statusCode" : errorMsg["statusCode"],
            "message" : errorMsg["message"]
        }))
    
    