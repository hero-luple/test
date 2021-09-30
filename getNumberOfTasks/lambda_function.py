import os, logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    emailFromToken = event.get('email', None)
    
    #taskType = ["sunshine", "caffeine", "olly", "eating", "exercise"]
    taskType = [0, 1, 2, 3, 4]
    
    result = {"countOfTasks": len(taskType), "taskType" : taskType}
    
    return {
        "statusCode": 200,
        "body": result
    }