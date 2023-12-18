import json
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.client import BaseClient
from datetime import datetime, timezone
import logging
from logging import Logger

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger: Logger = logging.getLogger(name="AwsUtils")

# Environment lookup; if null, set to INFO level.
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

# Environmental variables.
REGION: str = os.environ.get('REGION', 'ap-south-1')
SUBSCRIPTION_TABLE_NAME: str = os.environ.get("SUBSCRIPTION_TABLE_NAME", "msi-dev-subscriptions-ddb")

# Boto3 resources and clients.
DYNAMODB_RESOURCE: BaseClient = boto3.resource("dynamodb", region_name=REGION)

SUBSCRIPTION_TABLE: BaseClient = DYNAMODB_RESOURCE.Table(SUBSCRIPTION_TABLE_NAME)

# Global variables.
CHARSET: str = "utf-8"
S3_KEY_DATE_FORMAT: str = "%Y/%m/%d"
SITE_PREFIX: str = "SITE#"


# Returns file from s3 as a string
def get_file_from_s3(bucket: str, key: str) -> str:
    s3 = boto3.resource('s3', region_name=REGION)
    obj = s3.Object(bucket, key)
    return obj.get()['Body'].read().decode(CHARSET)


# saves a given file into supplied s3 bucket, with default key
def save_file_with_s3_key(bucket: str, key: str, file_name: str, contents: str) -> str:
    s3 = boto3.resource('s3', region_name=REGION)
    bucket = s3.Bucket(bucket)
    s3_key = f"{key}/{file_name}"
    bucket.put_object(Key=s3_key, Body=contents)
    return s3_key


# Send message to SQS queue
def send_sqs_message(name: str, message: dict) -> str:
    sqs = boto3.client('sqs')
    queue = sqs.get_queue_url(QueueName=name)
    response = sqs.send_message(QueueUrl=queue['QueueUrl'], MessageBody=json.dumps(message))
    return response


# call a given lambda with a dictionary converted to a json payload
def invoke_lambda(name: str, request: dict, is_async: bool = False):
    client = boto3.client('lambda', region_name=REGION)
    request_type = 'Event' if is_async else 'RequestResponse'
    response = client.invoke(FunctionName=name, InvocationType=request_type,
                             Payload=json.dumps(request))
    if is_async:
        return response['ResponseMetadata']['HTTPStatusCode']
    else:
        return json.loads(response["Payload"].read().decode(CHARSET))


def today_between_dates(start_date: datetime, end_date: datetime) -> bool:
    """
    Check today's date is between the start and end date (inclusive).
    Note: end_date can be None and means we only check today's date is after or equal to the start_date

    :param start_date:
    :param end_date:
    :return:
    """
    between_dates: bool = False
    now: datetime = datetime.now(timezone.utc)

    if end_date:
        if start_date <= now <= end_date:
            between_dates = True
    elif start_date <= now:
        between_dates = True

    return between_dates


def get_subscription(subscription_id: str, service_type: str, active: bool = True):
    """
    Gets a record from the subscription table if it exists with the supplied subscription_id.

    :param subscription_id:
    :param service_type:
    :param active:
    :return:
    """
    result: dict = SUBSCRIPTION_TABLE.query(
        IndexName="subId-index",
        KeyConditionExpression=Key("subId").eq(subscription_id),
        FilterExpression=Attr("svcName").eq(service_type) & Attr("active").eq(active)
    )

    # Can only contain 0 or 1 items in the subscriptions list as the subscription_id is unique.
    subscriptions: list = result.get("Items")
    logger.debug("Subscriptions found for subscription %s / service %s:\n%s", subscription_id, service_type, subscriptions)

    active_subscriptions: list = []

    for subscription in subscriptions:
        # Check start and end dates of subscription - "now" should be after/equal start and before/equal end.
        st_dt: str = subscription.get("stDt")
        start_date: datetime = datetime.fromisoformat(st_dt)

        # End date can be empty.
        ed_dt: str = subscription.get("edDt")
        end_date = datetime.fromisoformat(ed_dt) if ed_dt else None

        between_dates: bool = today_between_dates(start_date, end_date)

        if between_dates:
            active_subscriptions.append(subscription)

    logger.debug("Active subscriptions:\n%s", active_subscriptions)

    return active_subscriptions[0] if active_subscriptions else None


if __name__ == '__main__':
    pass
