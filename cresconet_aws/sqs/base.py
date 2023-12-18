"""
A series of SQS utility functions that can be used to send, receive and delete messages on SQS queues.
"""

import json
import logging
import os
from functools import lru_cache
from typing import Union, Optional, List

import boto3
from botocore.client import BaseClient
from botocore.config import Config

# Environment Variables
LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

logger = logging.getLogger("cresconet_sqs")
logger.setLevel(LOG_LEVEL)


@lru_cache
def retrieve_client(region_name: str = "ap-southeast-2", client_config: Optional[Config] = None) -> BaseClient:
    """
    Retrieves an SqsClient with given parameters.
    """
    client = boto3.client("sqs", region_name=region_name, config=client_config)
    return client


def send_sqs_message(name: str,
                     message_body: Union[dict, str],
                     extra_args: Optional[dict] = None,
                     region_name: str = "ap-southeast-2",
                     client_config: Optional[Config] = None) -> dict:
    """
    Send a message to an SQS queue.
    Should only be used for 'standard' SQS queues.

    :param name:            the name of SQS queue.
    :param message_body:    message body to send to queue. Can be either string or dictionary.
    :param extra_args:      extra arguments to attach to the message. Example: {"DelaySeconds": 3}.
    :param region_name:     the AWS region where the SQS queue is located.
    :param client_config:   Optional sqs client configuration.
    :return: the response from Boto3 'send_message' call.
    """
    sqs_client = retrieve_client(region_name=region_name, client_config=client_config)

    queue = sqs_client.get_queue_url(QueueName=name)

    # Give warning message to state that they should be calling a the 'FIFO' queue function.
    if 'FIFO' in name.upper():
        logger.warning("Sending a 'standard' message to a 'FIFO' queue. Please use '%s' instead. Continuing...",
                       send_sqs_message_fifo.__name__)

    if extra_args is None:
        extra_args = {}

    if isinstance(message_body, str):
        json_message_body = message_body
    elif isinstance(message_body, dict):
        json_message_body = json.dumps(message_body)
    else:
        raise TypeError(f"Cannot handle message body type [{type(message_body)}]. Must be dictionary or string.")

    return sqs_client.send_message(QueueUrl=queue['QueueUrl'], MessageBody=json_message_body, **extra_args)


def send_sqs_message_batch(name: str,
                           message_bodies: List[Union[str, dict]],
                           extra_args: Optional[dict] = None,
                           region_name: str = "ap-southeast-2",
                           client_config: Optional[Config] = None) -> List[dict]:
    """
    Send a list of messages to the given queue.
    Should only be used for 'standard' SQS queues.

    :param name:            the name of SQS queue.
    :param message_bodies:  a list of message bodies to send to the queue. Can be a list of string or dictionary values.
    :param extra_args:      extra arguments to attach to the messages. Example: {"DelaySeconds": 3}.
    :param region_name:     the AWS region where the SQS queue is located.
    :param client_config:   Optional sqs client configuration.
    :return: the response(s) from Boto3 'send_message_batch' call(s).
    """
    sqs_client = retrieve_client(region_name=region_name, client_config=client_config)

    queue = sqs_client.get_queue_url(QueueName=name)

    # Give warning message to state that they should be calling a the 'FIFO' queue function.
    if 'FIFO' in name.upper():
        logger.warning("Sending a 'standard' message to a 'FIFO' queue. Please use '%s' instead. Continuing...",
                       send_sqs_message_fifo.__name__)

    if extra_args is None:
        extra_args = {}

    # Limitation of Boto3 'send_message_batch' API.
    max_message_batch_size = 10

    message_batches = [message_bodies[i:i + max_message_batch_size]
                       for i in range(0, len(message_bodies), max_message_batch_size)]

    responses = []
    for message_batch in message_batches:
        logger.debug("Sending %s messages in this batch.", len(message_batch))
        message_batch_to_send = []
        for i, message in enumerate(message_batch):

            # Construct the message to send.
            message_to_send = {
                "Id": str(i),
                **extra_args
            }

            # Handle string or dictionary values.
            if isinstance(message, str):
                message_to_send["MessageBody"] = message
            elif isinstance(message, dict):
                message_to_send["MessageBody"] = json.dumps(message)
            else:
                raise TypeError(f"Cannot handle message body type [{type(message)}]. Must be dictionary or string.")

            message_batch_to_send.append(message_to_send)

        # Send the message batch.
        response = sqs_client.send_message_batch(
            QueueUrl=queue['QueueUrl'],
            Entries=message_batch_to_send
        )

        responses.append(response)
    return responses


def send_sqs_message_fifo(name: str, message_body: Union[dict, str], dedup_id: str,
                          group_id: str, extra_args: Optional[dict] = None,
                          region_name: str = "ap-southeast-2",
                          client_config: Optional[Config] = None) -> dict:
    """
    Send message to SQS FIFO queue.
    Should only be used for 'FIFO' SQS queues.

    :param name:            the name of SQS queue.
    :param message_body:    message body to send to queue. Can be either string or dictionary.
    :param dedup_id:        a unique id used to 'deduplicate' messages on a FIFO queue.
    :param group_id:        a tag that specifies that a message belongs to a specific message group.
    :param extra_args:      extra arguments to attach to the message. Example: {"DelaySeconds": 3}.
    :param region_name:     the AWS region where the SQS queue is located.
    :param client_config:   Optional sqs client configuration.
    :return: the response from Boto3 'send_message' call(s).
    """
    sqs_client = retrieve_client(region_name=region_name, client_config=client_config)

    queue = sqs_client.get_queue_url(QueueName=name)

    # Give warning message to state that they should be using a standard queue.
    if 'FIFO' not in name.upper():
        logger.warning("Sending 'FIFO' message to a 'standard' queue. Please use '%s' instead. Continuing...",
                       send_sqs_message.__name__)

    if extra_args is None:
        extra_args = {}

    if isinstance(message_body, str):
        json_message_body = message_body
    elif isinstance(message_body, dict):
        json_message_body = json.dumps(message_body)
    else:
        raise TypeError(f"Cannot handle message body type [{type(message_body)}]. Must be dictionary or string.")

    response = sqs_client.send_message(QueueUrl=queue['QueueUrl'],
                                       MessageBody=json_message_body,
                                       MessageDeduplicationId=dedup_id,
                                       MessageGroupId=group_id,
                                       **extra_args)
    return response


def receive_sqs_messages(name: str,
                         include_message_body_only: bool = True,
                         parse_body_from_string_to_dict: bool = True,
                         max_number_messages: int = 10,
                         extra_args: Optional[dict] = None,
                         region_name: str = "ap-southeast-2",
                         client_config: Optional[Config] = None) -> List[Union[dict, str]]:
    """
    Receive messages of the given SQS queue.
    Is able to handle requesting more than 10 messages (which is a limitation of Boto3) by looping and collating
    responses.

    :param name:                            the name of SQS queue.
    :param include_message_body_only:       indicates if only the 'Body' of the messages needs to be returned.
    :param parse_body_from_string_to_dict:  indicates if the 'Body' of the messages should be converted to a dictionary.
    :param max_number_messages:             the maximum number of messages to receive from Boto3.
    :param extra_args:                      extra arguments to attach to the message. Example: {"VisibilityTimeout": 3}.
    :param region_name:                     the AWS region where the SQS queue is located.
    :param client_config:   Optional sqs client configuration.
    :return: the response from Boto3 'receive_message' call(s). This could be the 'raw' response, raw message bodies or
             the dictionary values of the message bodies.
    """
    sqs_client = retrieve_client(region_name=region_name, client_config=client_config)

    queue = sqs_client.get_queue_url(QueueName=name)

    if extra_args is None:
        extra_args = {}

    # Limitation of Boto3 'receive_message' API.
    max_message_batch_size = 10

    messages = []
    while len(messages) < max_number_messages:
        remaining_messages = max_number_messages - len(messages)

        num_messages_in_batch = max_message_batch_size

        if remaining_messages < 1:
            # This shouldn't really happen, but is a safety check.
            logger.debug("Reached maximum number of messages. Exiting...")
            break

        if remaining_messages < max_message_batch_size:
            num_messages_in_batch = remaining_messages

        response = sqs_client.receive_message(
            QueueUrl=queue['QueueUrl'],
            MaxNumberOfMessages=num_messages_in_batch,
            **extra_args
        )

        response_messages = response.get("Messages", [])

        if len(response_messages) == 0:
            logger.debug("No more messages on queue. Exiting...")
            break

        messages.extend(response_messages)

    message_bodies = []
    # Only give the 'body' of the messages back to the client.
    if include_message_body_only:
        for message in messages:
            message_body = message.get('Body')

            # Parse the stringified JSON content back into a dictionary.
            if parse_body_from_string_to_dict:
                try:
                    dictionary_value = json.loads(message_body)
                    message_bodies.append(dictionary_value)
                except ValueError as e:
                    logger.error(f"Failed to convert string to dictionary. Will return 'string' value. Exception: {e}")
                    message_bodies.append(message_body)

            else:
                message_bodies.append(message_body)

        # Delete all the retrieved messages from the queue.
        delete_sqs_messages(name=name, messages=messages)

        return message_bodies

    # Delete all the retrieved messages from the queue.
    delete_sqs_messages(name=name, messages=messages)

    return messages


def delete_sqs_messages(name: str, messages: List[dict],
                        region_name: str = "ap-southeast-2",
                        client_config: Optional[Config] = None) -> List[dict]:
    """
    Delete messages from an SQS queue.
    The 'messages' argument accepts message dicts received from receive_message.
    Should only be used for 'standard' SQS queues.

    :param name:        the name of SQS queue.
    :param messages:    each 'message' must include the 'ReceiptHandle' value, so it can be deleted.
    :param region_name: the AWS region where the SQS queue is located.
    :param client_config:   Optional sqs client configuration.
    :return: the response(s) from Boto3 'delete_message_batch' call(s).
    """
    sqs_client = retrieve_client(region_name=region_name, client_config=client_config)

    queue = sqs_client.get_queue_url(QueueName=name)

    # Limitation of Boto3 'delete_message_batch' API.
    max_message_batch_size = 10

    message_batches = [messages[i:i + max_message_batch_size] for i in range(0, len(messages), max_message_batch_size)]

    responses = []
    for message_batch in message_batches:
        logger.debug("Deleting %s messages in this batch.", len(message_batch))

        response = sqs_client.delete_message_batch(
            QueueUrl=queue['QueueUrl'],
            Entries=[
                {
                    'Id': str(i),
                    # there is inconsistency of case for the key depending on how message was retrieved from sqs
                    'ReceiptHandle': message.get('ReceiptHandle') if message.get('ReceiptHandle') else message.get('receiptHandle')
                }
                for i, message in enumerate(message_batch)
            ],
        )
        responses.append(response)
    return responses


def get_approximate_number_of_messages_on_queue(name: str,
                                                region_name: str = "ap-southeast-2",
                                                client_config: Optional[Config] = None) -> int:
    """
    Retrieves the approximate number of messages available on the given SQS queue.
    This includes both the 'ApproximateNumberOfMessages' and 'ApproximateNumberOfMessagesNotVisible' values.

    :param name:            the name of SQS queue.
    :param region_name:     the AWS region where the SQS queue is located.
    :param client_config:   (optional) SQS client configuration.
    :return: the approximate number of messages on the SQS queue.
    """
    sqs_client = retrieve_client(region_name=region_name, client_config=client_config)
    queue = sqs_client.get_queue_url(QueueName=name)

    queue_attributes_response = sqs_client.get_queue_attributes(
        QueueUrl=queue['QueueUrl'],
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    )
    queue_attributes = queue_attributes_response.get("Attributes")
    appx_messages = int(queue_attributes.get("ApproximateNumberOfMessages"))
    appx_messages_not_visible = int(queue_attributes.get("ApproximateNumberOfMessagesNotVisible"))

    return appx_messages + appx_messages_not_visible
