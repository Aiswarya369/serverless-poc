"""
A series of AWS Secret Manager utility functions that can be used to access secrets.
"""
import json
import logging
import os
from typing import Optional

import boto3
# Environment Variables
from botocore.client import BaseClient

LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

logger = logging.getLogger("cresconet_secrets_manager")
logger.setLevel(LOG_LEVEL)

secrets_manager_client: Optional[BaseClient] = None


def retrieve_client(region_name: str):
    """
    Retrieves the Secrets Manager Client, if one is not already set.
    """
    global secrets_manager_client

    if not secrets_manager_client:
        secrets_manager_client = boto3.client("secretsmanager", region_name=region_name)


def get_secret_value_string(secret_name: str, region_name: str) -> str:
    """
    Retrieve a Secret from the Secret Manager for a given a 'secret_name'.

    :param secret_name: name of secret.
    :param region_name: the AWS region where the secret is located.
    :return: the SecretString.
    """
    retrieve_client(region_name=region_name)
    secret_value_response = secrets_manager_client.get_secret_value(SecretId=secret_name)
    secret_string = secret_value_response["SecretString"]
    return secret_string


def get_secret_value_dict(secret_name: str, region_name: str = "ap-southeast-2") -> dict:
    """
    Retrieve dictionary of Secret values from the Secret Manager for a given a secret_name.

    :param secret_name: name of secret.
    :param region_name: the AWS region where the secret is located.
    :return: the Secret as a dictionary.
    """
    secret_string = get_secret_value_string(secret_name=secret_name, region_name=region_name)
    secret_dict = json.loads(secret_string)

    return secret_dict
