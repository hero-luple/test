import os, datetime, logging
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    date = event.get("diaryDate", None)
    sortKeyArray = []
    
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["TABLE_NAME"])
    
    if date == "*":
        keyCondition = Key("email").eq(emailFromToken)
        sortKeyArray = getSortKeys(table, keyCondition)
        if len(sortKeyArray) == 0:
            logger.error("No data exists to delete.")
            raise Exception(json.dumps({
                'statusCode' : 400,
                'message' : "Bad Request"
             }))
    else:
        parameterCheck(date)
        sortKeyArray.append(date)

    try:
        for sortKey in sortKeyArray:
            deleteResponse = table.delete_item(
                Key = {
                    "email":emailFromToken,
                    "diaryDate" : int(sortKey)
                },
                ReturnValues = "ALL_OLD"
            )
        responseBody = { "statusCode": 200 }
        if "Attributes" in deleteResponse.keys():
            logger.info("%d item(s) delete operation successful. Return 200." % len(sortKeyArray))
        else:
            logger.error("No data exists to delete.")
            raise Exception(json.dumps({
                'statusCode' : 400,
                'message' : "Bad Request"
             }))
        return responseBody
            
    except ClientError as e:
        logger.error(e.response["Error"]["Message"])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message' : "Internal Server Error"
        }))
        
def getSortKeys(table, keyCondition):
    sortKeyArray = []
    queryResponse = table.query(
        KeyConditionExpression=keyCondition
    )
    for sleepDiary in queryResponse["Items"]:
        sortKeyArray.append(sleepDiary["date"])
    return sortKeyArray
    
def parameterCheck(date):
    try:
        datetime.datetime.strptime(date, '%Y%m%d')
    except ValueError:
        raise Exception(json.dumps({
            'statusCode' : 400,
            'message' : "Bad Request"
        }))