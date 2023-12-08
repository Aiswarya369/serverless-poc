# Imports.
import json
import logging
import os
import boto3
from typing import Union
from botocore.client import BaseClient

from cresconet_aws.support import send_message_to_support, SupportMessage, alert_on_exception

from src.config.config import AppConfig

# Logging.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger: logging.Logger = logging.getLogger(name="GetRequestStatus")

# Environment lookup, if null set to INFO level.
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

# Environmental variables.
REGION: str = os.environ.get("REGION", "ap-south-1")
REQUEST_TRACKER_TABLE_NAME: str = os.environ.get("REQUEST_TRACKER_TABLE_NAME", "msi-dev-request-tracker-ddb")

# Boto3 resources.
DYNAMODB_RESOURCE: BaseClient = boto3.resource("dynamodb", region_name=REGION)

# Constants.
HTTP_SUCCESS: int = 200
HTTP_NOT_FOUND: int = 404
HTTP_INTERNAL_ERROR: int = 500
GET_REQUEST_STATUS_ALERT_FORMAT = "Load Control Get Request Status has Failed - {hint}"
LOAD_CONTROL_ALERT_SOURCE = "get-request-status"
PK_PREFIX: str = "REQ#"
SK_PREFIX: str = "METADATA#"


class RequestNotFoundException(Exception):
    """
    Exception class for scenarios where the supplied request id could not be found.
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


def format_response(status_code: int, response: Union[dict, str]) -> dict:
    """
    Format HTTP response.

    :param status_code:
    :param response:
    :return:
    """
    body: str = json.dumps(response) if isinstance(response, dict) else response

    return {
        "statusCode": status_code,
        "body": body
    }


def get_status(correlation_id: str) -> str:
    """
    Get request status.

    :return:
    """
    request_tracker_table: BaseClient = DYNAMODB_RESOURCE.Table(REQUEST_TRACKER_TABLE_NAME)

    pk: str = f"{PK_PREFIX}{correlation_id}"
    sk: str = f"{SK_PREFIX}{correlation_id}"

    # Get data from DynamoDB table.
    response: dict = request_tracker_table.get_item(Key={
        "PK": pk,
        "SK": sk
    })
    logger.debug("Response:\n%s", response)

    if "Item" in response:
        item: dict = response["Item"]
        return item["currentStg"]
    else:
        raise RequestNotFoundException("Correlation id not found")


def job_entry(event: dict) -> dict:
    """
    Get request status; formulate response.

    :param event:
    :return:
    """
    # The correlation id will always exist as this request has come via API Gateway.
    correlation_id: str = event["pathParameters"]["correlation_id"]
    logger.info("Correlation id: %s", correlation_id)

    try:
        status: str = get_status(correlation_id)
        logger.info("Request status: %s", status)

        return format_response(HTTP_SUCCESS, {
            "message": "Request status query accepted",
            "status": status,
            "correlation_id": correlation_id
        })
    except RequestNotFoundException as e:
        logger.exception(str(e))

        return format_response(HTTP_NOT_FOUND, {
            "message": e.message,
            "correlation_id": correlation_id
        })
    except Exception as e:
        logger.exception(str(e))
        subject = GET_REQUEST_STATUS_ALERT_FORMAT.format(hint="Internal Error")
        support_message = SupportMessage(reason="Request status query failed with internal error", subject=subject,
                                         stack_trace=repr(e), tags=AppConfig.LOAD_CONTROL_TAGS)
        send_message_to_support(support_message, correlation_id=correlation_id)
        return format_response(HTTP_INTERNAL_ERROR, {
            "message": f"Request status query failed with internal error: {str(e)}",
            "correlation_id": correlation_id
        })


# @alert_on_exception(tags=AppConfig.LOAD_CONTROL_TAGS, service_name=LOAD_CONTROL_ALERT_SOURCE)
def lambda_handler(event: dict, _):
    """
    Lambda entry.

    :param event:
    :param _: Lambda context (not used).
    :return:
    """
    logger.info("Getting request status")
    logger.info("Event:\n%s", str(event))

    return job_entry(event)


if __name__ == "__main__":
    data: dict = {
        "pathParameters": {
            "correlation_id": "3120309956-2022-03-23T030621-eedab981-ca2b-4f75-a1fb-960edbd71ef9"
        }
    }
    result: dict = job_entry(data)
    logger.info("Result:\n%s", result)
