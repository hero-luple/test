import json, os, logging
import boto3

def lambda_handler(event, context):
    # TODO implement
    
    records = event["Records"]
    if records:
        body = json.loads(records[0]['Sns']['Message'])
        db = boto3.resource('dynamodb')
        table = db.Table(os.environ['TABLE_NAME'])
        
        # table.put_item(
        #     Item=body
        # )
        
        ## Send it to S3
    return {
        'statusCode': 200,
        'event' : event
    }
