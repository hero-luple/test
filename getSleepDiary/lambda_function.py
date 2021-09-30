import os, time, datetime, logging, json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    emailFromToken = event.get("email", None)
    since = event.get("since", None)
    parameterCheck(since)
    sinceFormat = convertDate(since)
    
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["TABLE_NAME"])
    
    try:
        keyCondition = Key("email").eq(emailFromToken) & Key("diaryDate").gte(sinceFormat)
        res = table.query(
                ProjectionExpression="diaryDate, sleepScore, textMessage, timeToSleep, differenceTime, disturbance",
                KeyConditionExpression=keyCondition
            )
            
        jsonArray = res["Items"]
        resultArray = []
        for sleepDiary in jsonArray:
            resultArray.append(sleepDiary)
        
        logger.info("Operation successful. Returning information.")
        return {
            "statusCode": 200,
            "body": resultArray
        }
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 400,
            'message' : "Bad Request"
        }))
        
def parameterCheck(since):
    if since > time.time():
        raise Exception(json.dumps({
            'statusCode' : 400,
            'message' : "Bad Request"
        }))
        
def convertDate(unixtime):
    return int(datetime.datetime.fromtimestamp(unixtime).strftime('%Y%m%d'))