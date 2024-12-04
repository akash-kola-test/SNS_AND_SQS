import logging
import json


logger = logging.getLogger("consumer 1")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info("Received event")
    logger.info(event)

    records = event.get("Records", [])

    if len(records) == 0:
        logger.info("no records to process")
        return {
            "statusCode": 200,
            "body": {
                "message": "No records to process"
            }
        }
    
    logger.info("Received %d of records in event", len(records))
    for record in records:
        logger.info("Processing record")
        logger.info(record)

        json_body = record.get("body", "")
        if len(json_body) == 0:
            logger.info("skipping the record as the body is empty")
            continue

        body = json.loads(json_body)

        
        logger.info("Subject is %s", body.get("Subject", ""))
        logger.info("Message is %s", body.get("Message", ""))

    return {
        "statusCode": 200,
        "body": {
            "message": "Processed records successfully"
        }
    }

