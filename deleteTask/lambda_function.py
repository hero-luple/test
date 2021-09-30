import os, logging, json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    emailFromToken = event.get("email", None)
    emailFromToken = "hero@luple.co.kr"
    taskId = event.get("taskId", None)
    sortKeyArray = []
    
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["TABLE_NAME"])
    
    if taskId == "*":
        keyCondition = Key("email").eq(emailFromToken)
        queryResponse = table.query(
                KeyConditionExpression=keyCondition
            )
        if queryResponse["Count"] == 0:
            logger.error("No data exists to delete.")
            raise Exception(json.dumps({
                "statusCode": 400,
                "message": "No data exists to delete"
            }))
        else:
            for task in queryResponse["Items"]:
                sortKeyArray.append(task["taskId"])
    else:
        sortKeyArray.append(taskId)
        
    try:
        for sortKey in sortKeyArray:
            deleteResponse = table.delete_item(
                Key = {
                    "email":emailFromToken,
                    "taskId" : sortKey
                },
                ReturnValues = "ALL_OLD"
            )
        responseBody = { "statusCode": 200 }
        if "Attributes" in deleteResponse.keys():
            logger.info("%d item(s) delete operation successful. Return 200." % len(sortKeyArray))
        else:
            logger.error("No data exists to delete.")
            raise Exception(json.dumps({
                "statusCode": 400,
                "message": "No data exists to delete"
            }))
        
        return responseBody
        
    except ClientError as e:
        logger.error(e.response["Error"]["Message"])
        raise Exception(json.dumps({
            "statusCode": 500,
            "message": "%s :%s" % (e.response["Error"]["Code"], e.response["Error"]["Message"])
        }))