import os
import boto3
import json
from enum import Enum
from functools import lru_cache
from typing import Optional, Dict, Callable
from datetime import datetime, timezone
from botocore.client import BaseClient
from dataclasses import dataclass, asdict as dataclass_as_dict

# -----------------------
# Environmental variables
# -----------------------
AWS_ACCOUNT_ID = str(os.environ.get("AWS_ACCOUNT_ID"))
SUPPORT_SNS_ARN = os.environ.get("SUPPORT_SNS_ARN")
SUPPORT_REPORTING_ENABLED = str(os.environ.get("SUPPORT_REPORTING_ENABLED", True)).upper() == "TRUE"

# -----------------------
# Constants
# -----------------------
SERVICE_FAIL_SUBJECT = "{} Failed - {}"
DEFAULT_SUBJECT = "[No Subject Defined]"
SUPPORT_TIMESPEC = "milliseconds"


class AlertPriority(Enum):
    CRITICAL = "P1"
    HIGH = "P2"


@dataclass
class StringAttribute:
    StringValue: str
    DataType: str = "String"


@dataclass
class SupportMessagePayload:
    time: str
    subject: str
    reason: str
    priority: str
    stackTrace: str
    accountId: str
    extra: Dict[str, str]


@dataclass
class SupportMessageAttributes:
    alias: StringAttribute
    priority: StringAttribute
    tags: StringAttribute


@dataclass
class SupportMessage:
    reason: str
    tags: str = ""
    subject: str = DEFAULT_SUBJECT
    stack_trace: Optional[str] = None
    sns_client: Optional[BaseClient] = None
    priority: AlertPriority = AlertPriority.CRITICAL


@lru_cache
def retrieve_sns_client():
    """
    Used to retrieve the instance of sns client to be used for reporting.

    :returns: SNS client.
    """
    sns_client = boto3.client("sns")
    return sns_client


def alert_on_exception(tags: str, service_name: str) -> Callable:
    """
    A decorator that provides a wrapper that sends any exceptions to support.

    :param tags: The tags for the alert.
    :param service_name: The name of the service that failed.
    :returns: The wrapper.
    """
    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_name = e.__class__.__name__
                support_message = SupportMessage(subject=SERVICE_FAIL_SUBJECT.format(service_name, error_name),
                                                 reason=error_name, stack_trace=repr(e), tags=tags)
                send_message_to_support(support_message)
                raise
        return wrapper
    return decorator


def send_message_to_support(args: SupportMessage, **kwargs: str) -> bool:
    """
    Send a message to the SNS support topic, which subsequently triggers support.

    :param args:   the arguments provided to the payload
    :param kwargs: the extra parameters to be provided in the payload.
    """
    if not SUPPORT_REPORTING_ENABLED:
        return False

    if SUPPORT_SNS_ARN is None:
        raise RuntimeError("Attempted to notify support. But environment variable SUPPORT_SNS_ARN is not defined.")

    sns_client = args.sns_client
    if not sns_client:
        sns_client = retrieve_sns_client()

    message_attributes = SupportMessageAttributes(
        alias=StringAttribute(StringValue=args.subject),
        priority=StringAttribute(StringValue=args.priority.value),
        tags=StringAttribute(StringValue=args.tags))
    message = SupportMessagePayload(reason=args.reason,
                                    subject=args.subject,
                                    stackTrace=str(args.stack_trace),
                                    priority=args.priority.value,
                                    time=datetime.now(timezone.utc).isoformat(timespec=SUPPORT_TIMESPEC),
                                    accountId=AWS_ACCOUNT_ID,
                                    extra=kwargs)

    sns_client.publish(
        TargetArn=SUPPORT_SNS_ARN,
        Subject=f"Alert: {args.subject}",
        Message=json.dumps({"default": json.dumps(dataclass_as_dict(message))}),
        MessageAttributes=dataclass_as_dict(message_attributes),
        MessageStructure="json"
    )
    return True
