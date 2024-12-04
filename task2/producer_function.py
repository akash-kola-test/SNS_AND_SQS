import logging
import os
import random


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
    
    fruits = [ "Apple", "Banana", "Mango" ]
    random_fruit = random.choice(fruits)

    logger.info("Publishing message to the SNS topic with fruit %s", random_fruit)
    sns_client = boto3.client("sns")
    response = sns_client.publish(
            TopicArn=TOPIC_ARN,
            Subject="Test Subject",
            Message=random_fruit,
            MessageAttributes={
                "Fruit": {
                    "DataType": "String",
                    "StringValue": random_fruit
                }
            }
    )

    logger.info("Send the message with Id of %s successfully to the topic", response.get("MessageId", ""))
    return {
        "statusCode": 200,
        "body": {
            "message": f"Published messge to Topic successfully with fruit {random_fruit}"
        }
    }

