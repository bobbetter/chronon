"""
EMR Serverless Runner - Submit Chronon jobs to EMR Serverless.

This service handles:
- Job submission to EMR Serverless
- Application state management (start if stopped)
- Chronon-specific routing (config type + mode -> Driver subcommand)
"""

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum
from pydantic import BaseModel, Field

from .boto_client import BotoClient
from server import config as cfg
from server.routes.v1.compute import SparkJobRequest
logger = logging.getLogger("uvicorn.error")


# Default EMR configuration from environment variables
DEFAULT_APPLICATION_ID = os.environ.get("CHRONON_EMR_APPLICATION_ID", "00fvgrsoabsudb0d")
DEFAULT_EXECUTION_ROLE = os.environ.get("CHRONON_EMR_EXECUTION_ROLE", "arn:aws:iam::211125482819:role/EMRServerlessExecutionRole")

# Chronon routing - maps config types and modes to Driver subcommands
CONFIG_TYPE_TO_DRIVER_SUBCOMMAND: Dict[str, Dict[str, str]] = {
    "group_bys": {
        "upload": "group-by-upload",
        "upload-to-kv": "group-by-upload-bulk-load",
        "backfill": "group-by-backfill",
    },
    "joins": {
        "backfill": "join",
    },
    "staging_queries": {
        "backfill": "staging-query-backfill",
    },
}

SPARK_MODES = ["backfill", "upload", "upload-to-kv"]


class EmrJobRequest(SparkJobRequest):
    application_id: str = Field(DEFAULT_APPLICATION_ID, description="EMR Serverless application ID")


class EmrJobResult(BaseModel):
    """Result of an EMR Serverless job submission."""

    job_run_id: str
    application_id: str
    job_name: str
    subcommand: str
    conf_type: str


class StartApplicationRequest(BaseModel):
    """Request to start an EMR Serverless application."""

    application_id: str = Field(
        default=DEFAULT_APPLICATION_ID,
        description="EMR Serverless application ID",
    )
    timeout_seconds: int = Field(
        default=300,
        description="Maximum time to wait for application to start (seconds)",
        ge=30,
        le=600,
    )


class ApplicationStates(str, Enum):
    """States of an EMR Serverless application."""
    STARTED = "STARTED"
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    CREATED = "CREATED"

class ApplicationStateResponse(BaseModel):
    """Response with application state information."""

    application_id: str
    state: ApplicationStates
    message: str


class EmrServerlessRunner:
    """
    Submit Chronon jobs to EMR Serverless.

    This class handles:
    - Parsing Chronon config paths to determine job type
    - Building Spark submit parameters
    - Submitting jobs to EMR Serverless
    - Optionally starting stopped applications
    """

    def __init__(self, boto_client: BotoClient = None) -> None:
        """
        Initialize the EMR Serverless runner.

        Args:
            boto_client: Optional BotoClient instance. If not provided, creates a new one.
        """

        self.client = boto_client.get_client("emr-serverless")

    @staticmethod
    def parse_config_path(config_path: str) -> str:
        """
        Parse config path to extract config_type.

        Expected format: compiled/group_bys/team/name.v1__1
        Returns: config_type (group_bys, joins, staging_queries)
        """
        parts = config_path.split("/")

        for part in parts:
            if part in CONFIG_TYPE_TO_DRIVER_SUBCOMMAND:
                return part

        raise ValueError(
            f"Invalid config path: {config_path}. "
            f"Could not find config_type. Expected one of: {list(CONFIG_TYPE_TO_DRIVER_SUBCOMMAND.keys())}"
        )

    @staticmethod
    def get_subcommand(config_type: str, mode: str) -> str:
        """Get the Chronon Driver subcommand for given config type and mode."""
        if config_type not in CONFIG_TYPE_TO_DRIVER_SUBCOMMAND:
            raise ValueError(f"Unknown config type: {config_type}")

        if mode not in CONFIG_TYPE_TO_DRIVER_SUBCOMMAND[config_type]:
            valid_modes = list(CONFIG_TYPE_TO_DRIVER_SUBCOMMAND[config_type].keys())
            raise ValueError(
                f"Invalid mode '{mode}' for {config_type}. Valid modes: {valid_modes}"
            )

        return CONFIG_TYPE_TO_DRIVER_SUBCOMMAND[config_type][mode]

    @staticmethod
    def get_jar_s3_paths() -> Dict[str, str]:
        """Get Chronon JAR paths in S3."""
        return {
            "spark": f"s3://{cfg.S3_BUCKET}/{cfg.CHRONON_JAR_S3_KEY_PREFIX}{cfg.CHRONON_SPARK_JAR}",
            "aws": f"s3://{cfg.S3_BUCKET}/{cfg.CHRONON_JAR_S3_KEY_PREFIX}{cfg.CHRONON_AWS_JAR}",
        }

    def _build_spark_params(self) -> str:
        """
        Build Spark submit parameters for EMR Serverless.

        These parameters are for the PySpark orchestration script (chronon_starter.py).
        The actual Chronon Driver runs via a nested spark-submit with its own configs.
        """
        jar_paths = self.get_jar_s3_paths()

        params = [
            "--conf",
            f"spark.emr-serverless.driverEnv.CHRONON_DRIVER_JAR={jar_paths['spark']}",
            "--conf",
            f"spark.emr-serverless.driverEnv.CHRONON_ONLINE_JAR={jar_paths['aws']}",
        ]

        return " ".join(params)

    def _build_entry_args(self, job_request: EmrJobRequest, subcommand: str) -> List[str]:
        """Build entry point arguments for chronon_starter.py."""
        args = [
            "--config-path",
            job_request.conf_path,
            "--subcommand",
            subcommand,
            "--ds",
            job_request.ds or "",
            "--s3-bucket",
            cfg.S3_BUCKET,
            "--s3-prefix",
            cfg.COMPILED_CONFIGS_S3_KEY_PREFIX,
        ]

        # Only pass online args for upload-to-kv mode
        if job_request.mode == "upload-to-kv":
            args.extend(["--online-class", cfg.CHRONON_ONLINE_CLASS or ""])

        return args

    def check_application_state(self, application_id: str) -> ApplicationStates:
        """
        Check the current state of an EMR Serverless application.

        Returns: Application state (e.g., 'STARTED', 'STOPPED', 'STARTING')
        """
        response = self.client.get_application(applicationId=application_id)
        try:
            return ApplicationStates(response["application"]["state"])
        except KeyError:
            raise ValueError(f"Invalid application state: {response['application']['state']}")

    def start_application(
        self, application_id: str, timeout_seconds: int = 300
    ) -> bool:
        """
        Start an EMR Serverless application if it's stopped.

        Args:
            application_id: The EMR Serverless application ID
            timeout_seconds: Maximum time to wait for application to start

        Returns:
            True if application is started, False otherwise
        """

        logger.info(f"Starting EMR application {application_id}...")
        self.client.start_application(applicationId=application_id)

        # Wait for application to start
        wait_interval = 10
        elapsed_time = 0

        while elapsed_time < timeout_seconds:
            response = self.client.get_application(applicationId=application_id)
            app_state = ApplicationStates(response["application"]["state"])

            if app_state == ApplicationStates.STARTED:
                logger.info(f"EMR application {application_id} is now {ApplicationStates.STARTED.value}")
                return True
            elif app_state in [ApplicationStates.STARTING, ApplicationStates.CREATED]:
                logger.debug(f"Application state: {app_state}, waiting...")
                time.sleep(wait_interval)
                elapsed_time += wait_interval
            else:
                logger.error(f"Unexpected application state: {app_state}")
                return False

        logger.warning(
            f"Timeout waiting for application to start (waited {timeout_seconds}s)"
        )
        return False

    def submit_job(
        self, job_request: EmrJobRequest, auto_start: bool = True
    ) -> EmrJobResult:
        """
        Submit a Chronon job to EMR Serverless.

        Args:
            job_request: Job configuration
            auto_start: If True, start the application if it's stopped

        Returns:
            EmrJobResult with job details

        Raises:
            ValueError: If config path or mode is invalid
            RuntimeError: If application is not ready and auto_start is False
        """
        # Parse config to get type and subcommand
        conf_type = self.parse_config_path(job_request.conf_path)
        subcommand = self.get_subcommand(conf_type, job_request.mode)

        # Check application state
        app_state = self.check_application_state(job_request.application_id)
        logger.info(f"EMR application state: {app_state}")

        if app_state == ApplicationStates.STOPPED:
            if auto_start:
                logger.info("Application is STOPPED, starting...")
                if not self.start_application(job_request.application_id):
                    raise RuntimeError(
                        f"Failed to start EMR application {job_request.application_id}"
                    )
            else:
                raise RuntimeError(
                    f"EMR application {job_request.application_id} is STOPPED"
                )
        elif app_state not in [ApplicationStates.STARTED, ApplicationStates.CREATED]:
            raise RuntimeError(
                f"EMR application {job_request.application_id} not ready (State: {app_state})"
            )

        # Generate job name
        conf_name = job_request.conf_path.split("/")[-1].replace(".", "_")
        app_name = f"chronon_{conf_type}_{job_request.mode}_{conf_name}"
        job_name = f"Chronon-{subcommand}-{job_request.ds}"

        # Build job configuration
        job_config = {
            "applicationId": job_request.application_id,
            "executionRoleArn": DEFAULT_EXECUTION_ROLE,
            "name": job_name,
            "jobDriver": {
                "sparkSubmit": {
                    "entryPoint": f"s3://{cfg.S3_BUCKET}/{cfg.EMR_STARTER_S3_KEY}",
                    "entryPointArguments": self._build_entry_args(job_request, subcommand),
                    "sparkSubmitParameters": self._build_spark_params(),
                }
            },
            "configurationOverrides": {
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{cfg.S3_BUCKET}/emr-serverless-logs/{app_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
                    },
                    "cloudWatchLoggingConfiguration": {
                        "enabled": True,
                        "logGroupName": f"/aws/emr-serverless/applications/{job_request.application_id}",
                    },
                }
            },
        }

        # Submit job
        logger.info(f"Submitting EMR job: {job_name}")
        response = self.client.start_job_run(**job_config)
        job_run_id = response["jobRunId"]

        logger.info(f"Job submitted successfully. Job Run ID: {job_run_id}")

        return EmrJobResult(
            job_run_id=job_run_id,
            application_id=job_request.application_id,
            job_name=job_name,
            subcommand=subcommand,
            conf_type=conf_type,
        )

    def get_job_status(self, application_id: str, job_run_id: str) -> Dict:
        """
        Get the status of a submitted job.

        Returns:
            Dictionary with job run details from EMR Serverless
        """
        response = self.client.get_job_run(
            applicationId=application_id, jobRunId=job_run_id
        )
        return response["jobRun"]