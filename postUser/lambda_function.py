import json, os, logging
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

'''
lambda_handler function

@input parameter:
    - emailFromToken: get user's e-mail address from Auth Token
             PK of database
    - body-json: json format body. must contain items below:
                    - email: string
                    - userName: string
                    - problems: List
                    - age: int
                    - gps: dict
                    - sex: int
                    - profiles: List
@return:
    - dict: status code

Description: POST operation for /user api
'''
def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    body = event.get('body-json', None)
    if body is not None:
        checkReturn = checkCapability(body)
    
    checkNonExistCapability(body, "user")
    
    db = boto3.resource('dynamodb')
    table = db.Table(os.environ['TABLE_NAME'])
    
    checkPresent(table, emailFromToken)
    
    items = ["userName", "problems", "age", "gps", "sex", "profile"]
    
    postItem = dict()
    postItem['email'] = emailFromToken
    
    for item in items:
        if item not in body.keys():
            raise Exception({
                "statusCode": 400,
                "message": "Bad Request. Invalid Input."
            })
        else:
            if item == 'gps':
                postItem[item] = dict()
                postItem[item]['longitude'] = Decimal(str(body[item]['longitude']))
                postItem[item]['latitude'] = Decimal(str(body[item]['latitude']))
            else:
                postItem[item] = body[item]
    
    table.put_item(
        Item=postItem
    )
    
    logger.info("Post user successful.")
    logger.info("Sending notification to SNS")
    
    sns = boto3.resource('sns')
    topic = sns.Topic(os.environ['SNS_ARN'])

    list_topic = list_topics(sns)
    logger.info(list_topic)
    for item in list_topic:
        print(item)
    
    postItem['gps']['longitude'] = body['gps']['longitude']
    postItem['gps']['latitude'] = body['gps']['latitude']
    
    messageId = publishMessage(topic, postItem, 'createUserTopic')
    
    logger.info(messageId)

    raise Exception(json.dumps({
        'statusCode' : 202,
        'message' : "Progressing"
    }))

"""
Lists topics for the current account.

:return: An iterator that yields the topics.
"""
def list_topics(sns):
    try:
        topics_iter = sns.topics.all()
        logger.info("Got topics.")
    except ClientError:
        logger.exception("Couldn't get topics.")
        raise Exception(json.dumps({
            'statusCode' : 400,
            'message': "Bad Request"
        }))
    else:
        return topics_iter

"""
publishMessage function

@input parameter:
    - topic: sns.topic object
    - message: dict (required)
    - subject: string (optional)
@return:
    - message id
"""
def publishMessage(topic, message, subject=""):
    try:
        response = topic.publish(
            Message=json.dumps(message))#, Subject=subject)
        logger.info(json.dumps(message))
        messageId = response['MessageId']
        logger.info("Published a message to topic %s.", topic.arn)
    except ClientError:
        logger.exception("Couldn't publish message to topic %s.", topic.arn)
        raise Exception(json.dumps({
            'statusCode' : 400,
            'message': "Bad Request"
        }))
    else:
        return messageId
        
'''
checkCapability function

@input parameter:

@return:
'''
def checkCapability(body):
    items = ["userName", "age", "problems", "profile", "sex", "language"]
    subItems = ["wakeUpTime", "sleepTime"]
    
    for item in items:
        if item in body:
            try:
                if item == 'userName':
                    assert type(body[item]) is str
                    assert 1 <= len(body[item]) <= 255
                elif item == 'age':
                    assert type(body[item]) is int
                    assert 0 <= body[item] <= 150
                elif item == 'problems':
                    checkPriority = False
                    for problem in body[item]:
                        assert type(problem['problem']) is int
                        assert problem['problem'] in [0, 1, 2, 3, 4, 5, 6, 7]
                        
                        if problem['priority'] == 1:
                            checkPriority = True
                    assert checkPriority is True
                elif item == 'profile':
                    if 'wakeUpTime' in body[item]:
                        assert type(body[item]['wakeUpTime']['hh']) is int
                        assert 0 <= body[item]['wakeUpTime']['hh'] < 24
                        assert type(body[item]['wakeUpTime']['mm']) is int
                        assert 0 <= body[item]['wakeUpTime']['mm'] < 60
                    if 'sleepTime' in body[item]:
                        assert type(body[item]['sleepTime']['hh']) is int
                        assert 0 <= body[item]['sleepTime']['hh'] < 24
                        assert type(body[item]['sleepTime']['mm']) is int
                        assert 0 <= body[item]['sleepTime']['mm'] < 60
                elif item == 'sex':
                    assert type(body[item]) is int
                    assert body[item] in [0, 1, 2]
                elif item == 'language':
                    assert type(body[item]) is int
                    assert body[item] in [0, 1, 2]
                else:
                    raise Exception(json.dumps({
                        'statusCode' : 400,
                        'message': "Bad Request"
                    }))
            except:
                raise Exception(json.dumps({
                    'statusCode': 400,
                    'message': 'Invalid Input: ' + item + " -> " + str(body[item])
                }))
        else:
            continue
    
    return True
    
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
                        'statusCode' : 400,
                        'message': "Bad Request"
                    }))
            elif mode == "profile":
                if key not in profileItems:
                    raise Exception(json.dumps({
                        'statusCode' : 400,
                        'message': "Bad Request"
                    }))
            elif mode == "device":
                if key not in deviceItems:
                    raise Exception(json.dumps({
                        'statusCode' : 400,
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
    
def checkPresent(table, emailFromToken):
    # try 1: Read DB
    try:
        res = table.get_item(Key={
            'email' : emailFromToken,
        })
        
        # Return: User not found
        if 'Item' not in res.keys():
            logger.info("User (" + emailFromToken + ") has not been found. Progress creating.")
            return {
                'statusCode': 202
            }
        else:
            logger.info("User (" + emailFromToken + ") is already present. Use PUT method.")
            raise Exception(json.dumps({
                'statusCode' : 400,
                'message': 'User Already Exist'
            }))
        
    # try 1: Read DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message' : "Internal Server Error",
        }))