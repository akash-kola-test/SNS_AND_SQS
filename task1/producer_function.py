import logging
import os


import boto3


logger = logging.getLogger("producer lambda")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    TOPIC_ARN = os.getenv("TOPIC_ARN")
    if TOPIC_ARN is None:
        logger.error("Topic Arn is not found")
        return {
            "statusCode": 500,
            "error": "Topic Arn is missing"
        }
    
    logger.info("Publishing message to the SNS topic")
    sns_client = boto3.client("sns")
    response = sns_client.publish(
            TopicArn=TOPIC_ARN,
            Subject="Test Subject",
            Message="Message from lambda producer"
    )

    logger.info("Send the message with Id of %s successfully to the topic", response.get("MessageId", ""))
    return {
        "statusCode": 200,
        "body": {
            "message": "Published messge to Topic successfully"
        }
    }

