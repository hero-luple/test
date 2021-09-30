import json, os, logging, asyncio, time
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
	emailFromToken = event.get('email', None)
	userNameFromToken = event.get('userName', None)
	
	try:
		cognitoClient = boto3.client(os.environ['COGNITO'])
        
		response = cognitoClient.admin_get_user(
		 	UserPoolId=os.environ['COGNITIVE_USER_POOL'],
		 	Username=userNameFromToken
		)
        
		logger.info(response)
	except:
		raise Exception({
			'statusCode' : 400,
			'message' : "User Not Found"
		})
	
	payload = {
		'email': emailFromToken,
		'userName': userNameFromToken
	}
	lambdaClient = boto3.client('lambda')
	responseFromLambda = lambdaClient.invoke(
		FunctionName=os.environ['LAMBDA_ARN'],
		LogType='Tail',
		InvocationType='Event',
		Payload=json.dumps(payload))
	
	logger.info("Got response from lambda function: /user DELETE")
	logger.info(responseFromLambda)
	
	# Return (Success)
	logger.info("Operation successful. Return 202.")
	
	return {
	    'statusCode': 202,
	    'message': "Progressing"
	}