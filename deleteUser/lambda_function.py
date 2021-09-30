import json, os, logging, asyncio, time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SLEEP_TABLE = "dynamodb.Table(name='Sleep-Dev')"
RECORDS_TABLE = "dynamodb.Table(name='Records-Dev')"

'''
lambda_handler function

@input parameter:
    - emailFromToken: get user's e-mail address from Auth Token
             PK of database
    - userNameFromToken: get name of userName from Auth Token
@return:
    - dict: status code

'''
def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    userNameFromToken = event.get('userName', None)
    
    logger.info("Lambda Handler for [/user] DELETE has been called.")
    logger.info("Process DELETE Method.")
    
    db = boto3.resource('dynamodb')
    userTable = db.Table(os.environ['USER_TABLE'])
    sleepTable = db.Table(os.environ['SLEEP_TABLE'])
    settingsTable = db.Table(os.environ['SETTINGS_TABLE'])
    recordsTable = db.Table(os.environ['RECORDS_TABLE'])
    
    tables = [userTable, sleepTable, settingsTable, recordsTable]
    rets = []

    # asyncio.run(deleteTable(table, emailFromToken))
    for table in tables:
        rets.append(deleteTable(table, emailFromToken))
        #rets.append(asyncio.run(deleteTable(table, emailFromToken)))
    
    for ret in rets:
        logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        logger.info(ret)
    
    try:    
        cognitoClient = boto3.client('cognito-idp')
        
        response = cognitoClient.admin_delete_user(
             UserPoolId=os.environ['COGNITIVE_USER_POOL'],
             Username=userNameFromToken
        )
        
        logger.info(response)
    except:
        logger.info("Cognitive Failed...")
    
    return {
        'statusCode' : 200
    }
        
def deleteTable(table, emailFromToken):
    logger.info("Delete " + str(table) + " process.")
        
    if str(table) == SLEEP_TABLE or str(table) == RECORDS_TABLE:
        ret = deleteTableWithSubkeys(table, emailFromToken)
        return ret
    else:
        ret = deleteTableWithoutSubkeys(table, emailFromToken)
        return ret
        
'''
deleteTableWithSubkeys function

@input parameter:
    - table: dynamodb table object
    - emailFromToken: get user's e-mail address from Auth Token
             PK of database
@return:
    - dict: status code

Description: DELETE operation for /user api
'''
def deleteTableWithSubkeys(table, emailFromToken):
    keyCondition = Key("email").eq(emailFromToken)
    sortKeyArray = []
    
    try:
        queryResponse = table.query(
            KeyConditionExpression=keyCondition
        )
    except:
        return {
            "statusCode": 200
        }
        
    if queryResponse["Count"] == 0:
        logger.info("No item to delete: " + str(table))
        return {
                "statusCode": 200
            }
    else:
        sortKeyName = ""
        if str(table) == SLEEP_TABLE:
            sortKeyName = "date"
        elif str(table) == RECORDS_TABLE:
            sortKeyName = "taskId"
        else:
            logger.error("Unknown table name. Exit")
            raise Exception(json.dumps({
                'statusCode' : 500,
                'message' : "Internal Server Error"
            }))
        
        try:
            for item in queryResponse["Items"]:
                sortKeyArray.append(item[sortKeyName])
            logger.info(sortKeyArray)
        except:
            return {
                'statusCode': 200
            }
        
        try:
            for sortKey in sortKeyArray:
                if str(table) == SLEEP_TABLE:
                    deleteResponse = table.delete_item(
                        Key = {
                            "email":emailFromToken,
                            "date" : int(sortKey)
                        },
                        ReturnValues = "ALL_OLD"
                    )
                elif str(table) == RECORDS_TABLE:
                    deleteResponse = table.delete_item(
                        Key = {
                            "email":emailFromToken,
                            "taskId" : str(sortKey)
                        },
                        ReturnValues = "ALL_OLD"
                    )
                
            if "Attributes" in deleteResponse.keys():
                logger.info("%d item(s) delete operation successful. Return 200." % len(sortKeyArray))
                return {
                    "statusCode": 200
                }
            else:
                logger.error("No data exists to delete.")
                return {
                    "statusCode": 200
                }
        except ClientError as e:
            logger.error(e.response["Error"]["Message"])
            raise Exception(json.dumps({
                'statusCode' : 500,
                'message': "Internal Server Error"
            }))
            
def deleteTableWithoutSubkeys(table, emailFromToken):
    try:
        response = table.delete_item(
            Key={
                'email': emailFromToken,
            },
        )
        return response
        
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message' : "Internal Server Error"
        }))