"""
A series of Lambda utility functions that can be used to invoke Lambdas.
"""

import json
import logging
import os
from functools import lru_cache
from typing import Union, Optional

import boto3
from botocore.client import BaseClient
from botocore.config import Config

# Environment Variables
LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

logger = logging.getLogger("cresconet_lambda")
logger.setLevel(LOG_LEVEL)


@lru_cache
def retrieve_client(region_name: str = "ap-southeast-2", client_config: Optional[Config] = None) -> BaseClient:
    """
    Retrieves an LambdaClient with given parameters.
    """
    client = boto3.client("lambda", region_name=region_name, config=client_config)
    return client


def invoke_lambda(name: str,
                  payload: Union[bool, dict, int, str],
                  region_name: str = "ap-southeast-2",
                  client_config: Optional[Config] = None,
                  is_async: bool = False,
                  return_response_payload_only: bool = True,
                  parse_response_from_json: bool = False) -> Union[bool, dict, int, str]:
    """
    Invokes the Lambda with the given payload.

    :param name:                            the name of Lambda Function.
    :param payload:                         the payload to be sent to the Lambda Function.
    :param region_name:                     the AWS region where the Lambda Function is located.
    :param client_config:                   (optional) Lambda client configuration.
    :param is_async:                        indicates if the API invocation should be asynchronous or not (default False).
    :param return_response_payload_only:    indicates that only the response payload (decoded) should be returned (default True).
    :param parse_response_from_json:        indicates (if return_response_payload_only is True) that the response should be parsed from a JSON string.
    :return: the response from Boto3 'invoke' call, or the raw response payload.
    """
    lambda_client = retrieve_client(region_name=region_name, client_config=client_config)
    request_type: str = "Event" if is_async else "RequestResponse"

    if isinstance(payload, dict):
        payload = json.dumps(payload).encode("utf-8")

    response = lambda_client.invoke(
        FunctionName=name,
        InvocationType=request_type,
        Payload=payload
    )

    # Just return the HTTP status code.
    if is_async:
        return response["ResponseMetadata"]["HTTPStatusCode"]

    # Retrieve the raw payload response.
    if return_response_payload_only:
        response_payload = response["Payload"].read().decode("utf-8")
        if parse_response_from_json:
            return json.loads(response_payload)
        return response_payload

    # Default to the raw API response value.
    return response
