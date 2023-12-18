import logging
import os
import uuid
from datetime import datetime
from typing import List, Dict, Union, Optional

import boto3
from botocore.client import BaseClient

# Environment Variables
LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

# Logging
logger = logging.getLogger("cresconet_metrics")
logger.setLevel(LOG_LEVEL)


class MetricManager:
    # Namespaces
    NAMESPACE_STORM = "STORM"
    NAMESPACE_COMMAND_CENTER = "COMMAND_CENTER"
    NAMESPACE_POLICYNET = "POLICYNET"

    # Units
    UNIT_COUNT = "Count"
    UNIT_SECONDS = "Seconds"

    # Statistics
    STATISTIC_SUM = "Sum"

    # Dimensions
    DIMENSION_NAME_STAGE = "STAGE"

    # Metric Names
    METRIC_NAME_NUMBER_OF_METERS = "NUMBER_OF_METERS"

    # Periods
    PERIOD_FIVE_MINUTES = 300

    # Scan By Values
    SCAN_BY_TIMESTAMP_ASCENDING = "TimestampAscending"
    SCAN_BY_TIMESTAMP_DESCENDING = "TimestampDescending"

    cloudwatch_client: BaseClient

    def __init__(self, cloudwatch_client: Optional[BaseClient] = None):
        if cloudwatch_client is not None:
            self.cloudwatch_client = cloudwatch_client
        else:
            self.cloudwatch_client = boto3.client("cloudwatch")

    def send_metric(self,
                    namespace: str,
                    metric_name: str,
                    value: Union[int, float],
                    dimensions: List[Dict[str, str]] = None,
                    unit: str = UNIT_COUNT) -> Optional[Exception]:
        """
        Sends a custom metric into the given 'namespace'.

        :param namespace:       the 'namespace' of the metric. These should be the name of the head-end, uppercase and underscore-separated.
        :param metric_name:     the name of the metric. Should be in uppercase and underscore-separated.
        :param value:           the value of the metric.
        :param dimensions:      the list of dimensions of the metric. Key value pairs. Structured like: [{"Name": "<name>", "Value": "<value"}].
        :param unit:            the unit of the metric. Defaults to 'Count'.
        """
        metric_data = [{
            'MetricName': metric_name,
            'Dimensions': dimensions,
            'Value': value,
            'Unit': str(unit),
        }]

        logger.debug("Creating metric for '%s' in namespace '%s'; value %s; unit '%s', dimensions %s.", metric_name, namespace, value, unit,
                     dimensions)

        # Don't want to fail creating metric and raise exceptions.
        try:
            self.cloudwatch_client.put_metric_data(
                MetricData=metric_data,
                Namespace=namespace
            )
            return None
        # pylint: disable=broad-except
        except Exception as ex:
            logger.exception(ex)
            return ex

    def get_metric_data(self,
                        namespace: str,
                        metric_name: str,
                        start_time: datetime.date,
                        end_time: datetime.date,
                        dimensions: List[Dict[str, str]] = None,
                        unit: str = UNIT_COUNT,
                        period: int = PERIOD_FIVE_MINUTES,
                        statistic: str = STATISTIC_SUM,
                        scan_by: str = SCAN_BY_TIMESTAMP_ASCENDING) -> Dict[datetime, int]:
        """
        Retrieves the specified metric data based off the given parameters.

        :param namespace:   the 'namespace' of the metric. These should be the name of the head-end, uppercase and underscore-separated.
        :param metric_name: the name of the metric. Should be in uppercase and underscore-separated.
        :param start_time:  the 'start datetime' to query the metrics from.
        :param end_time:    the 'end datetime' to query the metrics up to.
        :param dimensions:  the list of dimensions of the metric. Key value pairs. Structured like: [{"Name": "<name>", "Value": "<value"}].
        :param unit:        the unit of the metric. Defaults to 'Count'.
        :param period:      the granularity, in seconds, of the returned data points.
        :param statistic:   the statistic to return. Must be a CloudWatch metric.
        :param scan_by:     the order in which data points should be returned.
        :return: a dictionary containing datetime/data point key/value pairs.
        """
        # Set to an empty string, so the while loop iterates at least once.
        next_token = ""

        responses = []

        # Will loop until there are no more results.
        while next_token is not None:
            logger.debug("Next token is '%s', retrieving metrics...", next_token)
            # The metric query ID needs to be unique, but start with a lowercase character.
            metric_query_id = f'a{str(uuid.uuid4()).replace("-", "")}'

            metric_data_query = {
                'Id': metric_query_id,
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': metric_name,
                        'Dimensions': dimensions
                    },
                    'Unit': unit,
                    'Period': period,
                    'Stat': statistic
                }
            }

            logger.debug("Retrieving metric data for query: %s", metric_data_query)

            response = self.cloudwatch_client.get_metric_data(
                MetricDataQueries=[metric_data_query],
                StartTime=start_time,
                EndTime=end_time,
                ScanBy=scan_by,
            )

            logger.debug("Response from metric data query: %s", response)

            next_token = response.get('NextToken')
            responses.append(response)

        timestamp_value_dict = {}

        logger.debug("Collating %s paginated responses...", len(responses))
        # Iterate all paginated responses.
        for response in responses:
            logger.debug("Response: %s", response)
            metric_data_results = response.get('MetricDataResults')

            for metric_data_result in metric_data_results:
                timestamps: List[datetime] = metric_data_result.get('Timestamps')
                values: List[int] = metric_data_result.get('Values')

                timestamp_value_dict.update(dict(zip(timestamps, values)))

        logger.debug("Returning timestamp-to-value dictionary: %s", timestamp_value_dict)
        return timestamp_value_dict
