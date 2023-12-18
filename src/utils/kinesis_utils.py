import json
import logging
import os
import uuid

import boto3
from botocore.client import BaseClient
from msi_common import Stage, EventType

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(name="KinesisUtils")

# Environment lookup, if null set to INFO level.
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

REGION: str = os.environ.get("REGION", "ap-southeast-2")

# Boto3 resources and clients.
KINESIS_CLIENT: BaseClient = boto3.client("kinesis", region_name=REGION)

CHARSET: str = "utf-8"


class JSONEncoder(json.JSONEncoder):
    """
    JSON encoder to cater for Enums used when sending data to Kinesis.
    """
    def default(self, obj):
        if isinstance(obj, Stage):
            return obj.value
        elif isinstance(obj, EventType):
            return obj.value

        return json.JSONEncoder.default(self, obj)


def deliver_to_kinesis(record: dict, data_stream_name: str):
    """
    Deliver data to Kinesis.

    :param record:
    :param data_stream_name:
    :return:
    """
    logger.info("Preparing data to send to Kinesis:\n%s", record)

    record_bytes: bytes = json.dumps(record, cls=JSONEncoder).encode(CHARSET)
    partition_key: str = str(uuid.uuid4())

    response: dict = KINESIS_CLIENT.put_record(
        StreamName=data_stream_name,
        Data=record_bytes,
        PartitionKey=partition_key
    )
    logger.debug("Kinesis response: %s", response)
    logger.info("Record successfully received by Kinesis")

    return response
