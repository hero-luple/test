import json, os, logging
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

profileItems = ["wakeUpTime", "sleepTime", "effectiveDays"]

'''
lambda_handler function

@input parameter:
    - emailFromToken: get user's e-mail address from Auth Token
             PK of database
    - body-json: json format body. could contain one or more items below:
                    - userName: string
                    - problems: List
                    - age: int
                    - gps: dict
                    - sex: int
@return:
    - dict: status code

Description: PUT operation for /user api
'''
def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    body = event.get('body-json', None)
    urlFromRequest = event.get('url', None)

    if body == {}:
        raise Exception(json.dumps({
            'statusCode': 400,
            'message': "Bad Request"
        }))
    
    logger.info(os.environ['URI_USER'])
    logger.info(urlFromRequest)
    
    if urlFromRequest == os.environ['URI_USER']:
        checkNonExistCapability(body, "user")
        ret = putUserHandler(emailFromToken, body)
    elif urlFromRequest == os.environ['URI_PROFILE']:
        checkNonExistCapability(body, "profile")
        ret = putProfileHandler(emailFromToken, body)
    else:
        raise Exception(json.dumps({
            'statusCode': 400,
            'message': "Bad Request"
        }))
    
    return ret
        
'''
putUserHandler function

@input parameter:

@return:
'''
def putUserHandler(emailFromToken, body):
    if body is not None:
        checkReturn = checkCapability(body)
    
    items = ["userName", "problems", "age", "gps", "sex"]
    
    '''
    Make update expression & update values for aws dynamoDB first
        * update expression must be string
        * update values must be dict (json style)
    Only contains items that will be updated in this process
    '''
    updateExpression = "set"
    updateVals = dict()
    for item in body.keys():
        # gather values
        if item == 'gps':
            updateVals[":"+str(item)] = {
                'latitude' : Decimal(str(body[item]['latitude'])),
                'longitude' : Decimal(str(body[item]['longitude']))
            }
        else:
            updateVals[":"+str(item)] = body[item]
        
        # gather update expression
        # if item == "problems":
        #     updateExpression += " " + str(item) + " = list_append(" + str(item) + ", :" + str(item) + "),"
        # else:
        updateExpression += " " + str(item) + " = :" + str(item) + ","
    updateExpression = updateExpression[:-1]
    
    db = boto3.resource('dynamodb')
    table = db.Table(os.environ['TABLE_NAME'])
    
    # try 1: Read DB
    try:
        res = table.get_item(Key={
            'email' : emailFromToken,
        })
        
        # Return: if Item element is not found (DB error)
        if 'Item' not in res.keys():
            logger.error("User (" + emailFromToken + ") is not found.")
            raise Exception(json.dumps({
                'statusCode' : 400,
                'message': "User Not Found"
            }))
        
        # try 2: Update DB
        try:
            res = table.update_item(Key={
                'email' : emailFromToken,
            },
            UpdateExpression=updateExpression,
            ExpressionAttributeValues=updateVals)
        
        # try 2: Update DB
        except ClientError as e:
            logger.error(e.response['Error']['Message'])
            raise Exception(json.dumps({
                'statusCode' : 500,
                'message': "Internal Server Error"
            }))
        
        '''
        if profiles is in input body (with user info),
        handle with putProfileId too.
        '''
        if 'profile' in body:
            logger.info("Profile update needed.")
            response = putProfileHandler(emailFromToken, body['profile'])
            
            logger.info("Got response from: putProfileId")
            logger.info(response)
        
        # Return (Success)
        logger.info("Operation successful. Return 200.")
        return {
            'statusCode' : 200
        }
    # try 1: Read DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message': "Internal Server Error"
        }))
   
'''
putProfileHandler function

@input parameter:
    - emailFromToken: get user's e-mail address from Auth Token
             PK of database
    - body-json: body that contains editable profile elements
@return:
    - dict: status code

Description: PUT operation for /user/profile api
'''
def putProfileHandler(emailFromToken, profile):
    if profile is not None:
        checkReturn = checkCapabilityForProfile(profile)
    
    db = boto3.resource('dynamodb')
    table = db.Table(os.environ['TABLE_NAME'])
    
    # try 1: Read DB
    try:
        res = table.get_item(Key={
            'email' : emailFromToken,
        })
        
        # if User data is corrupted, or User Name is not in DB
        if 'Item' not in res.keys():
            logger.error("User (" + emailFromToken + ")not found.")
            raise Exception(json.dumps({
                'statusCode' : 400,
                'message' : "User Not Found"
            }))
        
        updateExpression = "set"
        updateVals = dict()
        
        for item in profile:
            updateExpression += " profile." + str(item) + " = :" + str(item) + ","
            updateVals[":"+str(item)] = profile[item]
            
        updateExpression = updateExpression[:-1]
    
        ret = updateDb(table, emailFromToken, updateExpression, updateVals)
        
        return ret
        
    # try 1: Read DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message': "Internal Server Error"
        }))
        
'''
! DEPRECATED !
putProfileIdHandler function

@input parameter:
    - emailFromToken: get user's e-mail address from Auth Token
             PK of database
    - profileId: get name of profile ID from path parameter
    - body-json: body that contains editable profile elements
@return:
    - dict: status code

Description: PUT operation for /user/{profileId} api
'''
def putProfileIdHandler(emailFromToken, profileId):
    if profileId is not None:
        checkReturn = checkCapabilityForProfiles(profileId)
    
    db = boto3.resource('dynamodb')
    table = db.Table(os.environ['TABLE_NAME'])
    
    # List 'ids' will save all ids in profiles section
    ids = []
    
    # try 1: Read DB
    try:
        res = table.get_item(Key={
            'email' : emailFromToken,
        })
        
        # if User data is corrupted, or User Name is not in DB
        if 'Item' not in res.keys():
            logger.error("User (" + emailFromToken + ")not found.")
            raise Exception(json.dumps({
                'statusCode' : 400,
                'message' : "User Not Found"
            }))
        
        # if profiles section is not in DB
        if 'profiles' not in res['Item'].keys():
            logger.info("profiles section does not exist. Creating new section.")
            ret = handleNonExistence(table, profileId, emailFromToken)
            return ret
        
        # get all stored ids
        for item in res['Item']['profiles']:
            ids.append(item['id'])
        # ids.append(profileId['id'])
        
        # if profile id is new
        if profileId['id'] not in ids:
            logger.info("profile ID is new. Appending new profile.")
            ret = handleNewProfile(table, profileId, emailFromToken, res, ids)
            return ret
        
        # if profile id exists
        else:
            logger.info("profile ID exists. Editing the profile.")
            logger.info("collected ids: " + str(ids))
            ret = handleExistingProfile(table, profileId, emailFromToken, res, ids)
            return ret
        
    # try 1: Read DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message': "Internal Server Error"
        }))

'''
! DEPRECATED !
handleNonExistence function

@input parameter:
    - table: DB table object
    - profileId: input variable of body-json (profile to be updated)
    - emailFromToken: input vaiable of emailFromToken (user's PK)
@return:
    - dict: status code

Description: handler function for putProfileId
    - use this func when DB's user data does not contain 'profile' section
'''
def handleNonExistence(table, profileId, emailFromToken):
    # Prepare all elements, since this is creating a new section
    # If input variable body-json does not contain elements, put it as null
    profileObj = profileGenerator(profileId, exist=False)
    
    updateExpression = "set profiles = :q"
    updateVals = {
        ":q": [profileObj]
    }
    
    ret = updateDb(table, emailFromToken, updateExpression, updateVals)
    return ret

'''
! DEPRECATED !
handleNewProfile function

@input parameter:
    - table: DB table object
    - profileId: input variable of body-json (profile to be updated)
    - emailFromToken: input vaiable of emailFromToken (user's PK)
    - origin: original DB data before update
    - ids: list of ids of stored profiles
@return:
    - dict: status code

Description: handler function for putProfileId
    - use this func when input profile is new, and does not exist in DB
        - however, profile section must exist beforehand
'''
def handleNewProfile(table, profileId, emailFromToken, origin, ids):
    profileObj = profileGenerator(profileId, exist=False)
    
    # Handle with selection
    # Find out if other profiles have True on selected, and if so,
    # switch it to False
    if profileObj['selected'] == True:
        try:
            idx = ids.index(profileObj['id'])
        except:
            idx = -1
        updateExpression = "set"
        updateVals = dict()
        for i in range(len(origin['Item']['profiles'])):
            if origin['Item']['profiles'][i]['selected'] == True:
                if i != idx:
                    updateExpression += " profiles[" + str(i) + "].selected = :selected" + str(i) + ","
                    off_key = ":selected" + str(i)
                    updateVals[off_key] = False
        
        # if all other profiles are False for selected (error state)
        if updateExpression == 'set':
            errState = True
        else:
            errState = False
        updateExpression = updateExpression[:-1]
        
        if not errState:
            # update for profile selected turn off
            ret = updateDb(table, emailFromToken, updateExpression, updateVals)
            if ret['statusCode'] == 500:
                return ret
    
    updateExpression = "set profiles = list_append(profiles, :i)"
    updateVals = {
        ":i": [profileObj]
    }
    
    # update DB
    ret = updateDb(table, emailFromToken, updateExpression, updateVals)
    return ret

'''
handleExistingProfile function

@input parameter:
    - table: DB table object
    - profileId: input variable of body-json (profile to be updated)
    - emailFromToken: input vaiable of emailFromToken (user's PK)
    - origin: original DB data before update
    - ids: list of ids of stored profiles
@return:
    - dict: status code

Description: handler function for putProfileId
    - use this func when input profile already exists in DB,
      and trying to edit all or some of the attributes
'''
def handleExistingProfile(table, profileId, emailFromToken, origin, ids):
    profileObj = profileGenerator(profileId, exist=True)
    
    # Build up update expression for DB
    idx = ids.index(profileObj['id'])
    prefix = " profiles[" + str(idx) + "]."
    updateExpression = "set"
    updateVals = dict()
    logger.info(str(profileObj))
    for key, val in profileObj.items():
        updateExpression += prefix + key + " = :" + key + ","
        updateVals[":"+key] = val
    
    # Handle with selection
    # Find out if other profiles have True on selected, and if so,
    # switch it to False    
    if 'selected' in profileObj.keys():
        if profileObj['selected'] == True:
            for i in range(len(origin['Item']['profiles'])):
                if origin['Item']['profiles'][i]['selected'] == True:
                    # exclude input profile index (You must not change input profile's selected option!)
                    if i != idx:
                        updateExpression += " profiles[" + str(i) + "].selected = :selected" + str(i) + ","
                        off_key = ":selected" + str(i)
                        updateVals[off_key] = False
    updateExpression = updateExpression[:-1]
    logger.info(updateExpression)
    logger.info(str(updateVals))
    
    # update DB
    ret = updateDb(table, emailFromToken, updateExpression, updateVals)
    return ret
    
'''
profileGenerator function

@input parameter:
    - profileId: dict
    - exist: bool
@return:
    - dict
'''
def profileGenerator(profileId, exist):
    if exist:
        profileObj = dict()
        for item in profileItems:
            if item in profileId.keys():
                profileObj[item] = profileId[item]
    else:
        profileObj = dict()
        for item in profileItems:
            if item in profileId.keys():
                profileObj[item] = profileId[item]
            else:
                profileObj[item] = None
    
    return profileObj
    
'''
updateDb function

@input parameter:
    - table: dynamodb db.table object
    - emailFromToken: string
    - updateExpression: string
    - updateVals: dict
@return:
    - json response (dict)
'''
def updateDb(table, emailFromToken, updateExpression, updateVals):
    # try 1: Update DB
    try:
        res = table.update_item(Key={
            'email' : emailFromToken,
        },
        UpdateExpression=updateExpression,
        ExpressionAttributeValues=updateVals
        )
        return {
            'statusCode' : 200
        }
    # try 1: Update DB
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(json.dumps({
            'statusCode' : 500,
            'message': "Internal Server Error"
        }))
        
'''
! DEPRECATED !
checkCapabilityForProfiles function

@input parameter:

@return:
'''
def checkCapabilityForProfiles(body):
    items = ["userName", "age", "problems", "profiles", "sex", "language"]
    subItems = ["wakeUpTime", "sleepTime"]
    
    for item in items:
        if item in body:
            try:
                if 'wakeUpTime' in item:
                    assert type(item['wakeUpTime']['hh']) is int
                    assert 0 <= item['wakeUpTime']['hh'] < 24
                    assert type(item['wakeUpTime']['mm']) is int
                    assert 0 <= item['wakeUpTime']['mm'] < 60
                if 'sleepTime' in item:
                    assert type(item['sleepTime']['hh']) is int
                    assert 0 <= item['sleepTime']['hh'] < 24
                    assert type(item['sleepTime']['mm']) is int
                    assert 0 <= item['sleepTime']['mm'] < 60
                else:
                    raise Exception(json.dumps({
                        'statusCode' : 400,
                        'message' : "Bad request"
                    }))
            except:
                raise Exception(json.dumps({
                    'statusCode': 400,
                    'message': 'Invalid Input: ' + item + " -> " + str(body[item])
                }))
        else:
            continue
    
    return True
    
'''
checkCapabilityForProfile function

@input parameter:

@return:
'''
def checkCapabilityForProfile(profile):
    items = ["wakeUpTime", "sleepTime"]
    
    for item in items:
        if item in profile:
            try:
                assert type(profile[item]['hh']) is int
                assert 0 <= profile[item]['hh'] < 24
                assert type(profile[item]['mm']) is int
                assert 0 <= profile[item]['mm'] < 60
            except:
                raise Exception(json.dumps({
                    'statusCode': 400,
                    'message': 'Invalid Input: ' + item + " -> " + str(profile[item])
                }))
        else:
            continue
    
    return True
        
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
                        logger.info(body[item])
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
                        'message' : "Bad request"
                    }))
            except:
                raise Exception(json.dumps({
                    'statusCode': 400,
                    'message': 'Invalid Input: ' + item + " -> " + str(body[item])
                }))
        else:
            continue
    
    return True

'''

'''
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
                        'statusCode': 400,
                        'message': "Bad Request"
                    }))
            elif mode == "profile":
                if key not in profileItems:
                    raise Exception(json.dumps({
                        'statusCode': 400,
                        'message': "Bad Request"
                    }))
            elif mode == "device":
                if key not in deviceItems:
                    raise Exception(json.dumps({
                        'statusCode': 400,
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