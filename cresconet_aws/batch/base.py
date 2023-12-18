"""
A series of AWS Batch job utility functions that can be used to execute new Batch jobs.
"""

import logging
import os
from datetime import datetime

# Environment Variables
LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

# Logging
logger = logging.getLogger(name="cresconet_batch")
logger.setLevel(LOG_LEVEL)

# Global Variables
BATCH_JOB_STATUSES = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]


def is_job_already_running(batch_client, batch_job_queue_name: str, batch_job_name_prefix: str) -> bool:
    """
    Checks if a BATCH job is already running. This ensures a reserved concurrency of one.

    :param batch_client:            the Boto3 Batch client.
    :param batch_job_queue_name:    the name of the Batch Queue the expected job is running on.
    :param batch_job_name_prefix:   the prefix of the Batch job name to check.

    :return: bool indicating whether an instance of this job already running.
    """
    job_running = False
    logger.info("Checking Batch for running jobs in Job Queue: %s.", batch_job_queue_name)
    for job_status in BATCH_JOB_STATUSES:
        response = batch_client.list_jobs(jobQueue=batch_job_queue_name, jobStatus=job_status)

        if response:
            job_summary_list = response.get("jobSummaryList")

            if job_summary_list:
                for job_summary in job_summary_list:
                    job_name = job_summary.get("jobName")
                    job_status = job_summary.get("status")

                    if job_name.startswith(batch_job_name_prefix) and job_status in BATCH_JOB_STATUSES:
                        job_running = True
                        break

    logger.info("Is job running: %s", job_running)
    return job_running


def submit_batch_job(batch_client,
                     python_script_path: str,
                     worker_cpu: str,
                     worker_memory: str,
                     retry_status_reasons: str,
                     job_attempts: str,
                     batch_job_queue_name: str,
                     batch_job_name_prefix: str,
                     batch_job_definition: str,
                     environment_variables: dict = None,
                     singleton: bool = True):
    """
    Triggers a BATCH job start based off the given parameters. Configures retry strategy.

    :param batch_client:            the Boto3 Batch client.
    :param python_script_path:      the name of the Python script (i.e. enforcement.trigger_enforcement).
    :param worker_cpu:              the CPU to give the Batch workers.
    :param worker_memory:           the memory to give the Batch workers.
    :param retry_status_reasons:    a 'string' of pipe-separated reasons to retry Batch jobs on failure.
    :param job_attempts:            the total attempts for jobs if they fail for one of the retry_status_reasons.
    :param batch_job_queue_name:    the name of the Batch job queue to run the job on.
    :param batch_job_name_prefix:   the prefix to put on the job name, is appended with the current timestamp.
    :param batch_job_definition:    the name of the Batch job definition which specifies which ECR image to run.
    :param environment_variables:   a dictionary of key/value pairs for the environment variables in the BATCH job.
    :param singleton:               boolean to declare that only one instance is allowed to run at a time.

    :return: response from the BATCH executor. This just indicates the BATCH job received the parameters, not if it
             was successful or not.
    """

    # Don't submit the BATCH job, if one is already running.
    if singleton and is_job_already_running(batch_client=batch_client,
                                            batch_job_queue_name=batch_job_queue_name,
                                            batch_job_name_prefix=batch_job_name_prefix):
        message = "An instance of this job is already running - exiting..."
        logger.warning(message)
        return message

    # Construct all environment variables for the BATCH job.
    batch_job_environment = []

    for key, value in environment_variables.items():
        batch_job_environment.append({
            "name": key,
            "value": value
        })

    lambda_py_cmd = f"from {python_script_path} import batch_handler; batch_handler()"
    container_overrides = {
        "command": [
            "python3", "-c", lambda_py_cmd
        ],
        "environment": batch_job_environment,
        "resourceRequirements": [
            {
                "value": worker_cpu,
                "type": "VCPU"
            }, {
                "value": worker_memory,
                "type": "MEMORY"
            }
        ],
    }

    # Only retry with the given RETRY_STATUS_REASONS.
    # JOB_ATTEMPTS is number of times the job will run in total. Minimum of one, max ten.
    retry_reasons = []
    for retry_status_reason in retry_status_reasons.split("|"):
        retry_reasons.append({
            "onStatusReason": retry_status_reason,
            "action": "RETRY"
        })

    retry_strategy = {
        "attempts": int(job_attempts),
        "evaluateOnExit": retry_reasons
    }

    logger.info("Launching BATCH job with parameters %s", str(batch_job_environment))

    response = batch_client.submit_job(
        jobName=f"{batch_job_name_prefix}-{datetime.now().strftime('%Y-%m-%d-%H%M%S')}",
        jobQueue=batch_job_queue_name,
        containerOverrides=container_overrides,
        jobDefinition=batch_job_definition,
        retryStrategy=retry_strategy)

    logger.info("BATCH job submitted job successfully %s", response)
    return response
