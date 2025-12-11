"""
Base Boto3 Client - Handles AWS client creation and configuration.

This service provides a base class for creating and managing boto3 clients
with support for various AWS services (S3, EMR Serverless, etc.).

Credential Resolution Order:
1. Explicit credentials passed to constructor
2. Environment variables (AWS_ACCESS_KEY_ID_REMOTE, etc.)
3. AWS default credential chain (includes SSO, profiles, instance roles)

For AWS SSO support:
- Run `aws sso login --profile <your-profile>` on your local machine
- Mount ~/.aws directory into the container (done in docker-compose.yml)
- Optionally set AWS_PROFILE_REMOTE environment variable
"""

import logging
import os
from typing import Any, Dict, Optional

import boto3
from botocore.client import BaseClient
from botocore.exceptions import NoCredentialsError, ProfileNotFound

logger = logging.getLogger("uvicorn.error")


# Default AWS configuration from environment variables
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION_REMOTE", "us-west-2")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID_REMOTE")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY_REMOTE")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN_REMOTE")
AWS_PROFILE = os.environ.get("AWS_PROFILE_REMOTE")


class BotoClient:
    """
    Base class for boto3 client management.

    Provides common functionality for:
    - Building AWS client configuration from credentials
    - Creating boto3 clients for different services
    - Managing AWS region and authentication settings

    Supports multiple authentication methods:
    - Explicit credentials (access key + secret key)
    - AWS SSO (via mounted ~/.aws directory and profile)
    - Default credential chain (environment, instance roles, etc.)
    """

    def __init__(
        self,
        region_name: str = AWS_REGION,
        access_key_id: Optional[str] = AWS_ACCESS_KEY_ID,
        secret_access_key: Optional[str] = AWS_SECRET_ACCESS_KEY,
        session_token: Optional[str] = AWS_SESSION_TOKEN,
        profile_name: Optional[str] = AWS_PROFILE,
    ) -> None:
        """
        Initialize the boto client manager.

        Args:
            region_name: AWS region
            access_key_id: AWS access key ID (optional if using SSO/profile)
            secret_access_key: AWS secret access key (optional if using SSO/profile)
            session_token: Optional AWS session token (for temporary credentials)
            profile_name: AWS profile name (for SSO or named profiles)
        """
        self.region_name = region_name
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._session_token = session_token
        self._profile_name = profile_name
        self._session: Optional[boto3.Session] = None
        self._credential_source: str = "unknown"

    def _get_session(self) -> boto3.Session:
        """
        Get or create a boto3 Session with the appropriate credentials.

        The session handles credential resolution including SSO tokens.

        Returns:
            A configured boto3.Session
        """
        if self._session is not None:
            return self._session

        # If explicit credentials are provided, use them
        if self._access_key_id and self._secret_access_key:
            logger.info("Using explicit AWS credentials from environment variables")
            self._credential_source = "explicit_credentials"
            self._session = boto3.Session(
                region_name=self.region_name,
                aws_access_key_id=self._access_key_id,
                aws_secret_access_key=self._secret_access_key,
                aws_session_token=self._session_token,
            )
            self._log_session_info()
            return self._session

        # Try to use a named profile (supports SSO)
        if self._profile_name:
            try:
                logger.info(f"Attempting to use AWS profile: {self._profile_name}")
                self._session = boto3.Session(profile_name=self._profile_name)
                credentials = self._session.get_credentials()
                if credentials:
                    self._credential_source = f"profile:{self._profile_name}"
                    logger.info(f"Successfully loaded credentials from profile: {self._profile_name}")
                    self._log_session_info()
                    return self._session
            except ProfileNotFound:
                logger.warning(f"AWS profile '{self._profile_name}' not found, falling back to default chain")
            except Exception as e:
                logger.warning(f"Error loading profile '{self._profile_name}': {e}, falling back to default chain")

        # No credentials configured - raise an error with helpful message
        self._credential_source = "none"
        raise NoCredentialsError(
            "No AWS credentials configured. Please either:\n"
            "  1. Set AWS_PROFILE_REMOTE to your SSO profile name and run 'aws sso login --profile <profile>'\n"
            "  2. Set AWS_ACCESS_KEY_ID_REMOTE and AWS_SECRET_ACCESS_KEY_REMOTE environment variables"
        )

    def _log_session_info(self) -> None:
        """Log details about the current session (region, credentials)."""
        if not self._session:
            return
        try:
            region = self._session.region_name
            credentials = self._session.get_credentials()
            if credentials:
                # Mask the access key for security (show first 4 and last 4 chars)
                masked_key = f"{credentials.access_key[:5]}..."
                logger.info(f"AWS Session: region={region}, access_key={masked_key}, source={self._credential_source}")
            else:
                logger.warning(f"AWS Session: region={region}, NO CREDENTIALS FOUND")
        except Exception as e:
            logger.warning(f"Could not log session info: {e}")

    def get_client(self, client_type: str, **extra_kwargs: Any) -> BaseClient:
        """
        Create a boto3 client for the specified service.

        Args:
            client_type: The AWS service name (e.g., 's3', 'emr-serverless', 'dynamodb')
            **extra_kwargs: Additional kwargs to pass to session.client() 
                           (e.g., endpoint_url for localstack)

        Returns:
            A boto3 client for the specified service

        Examples:
            >>> client = boto_client.get_client('s3')
            >>> client = boto_client.get_client('emr-serverless')
            >>> client = boto_client.get_client('dynamodb', endpoint_url='http://localhost:8000')
        """
        session = self._get_session()
        logger.debug(f"Creating boto3 client for service: {client_type} (credential source: {self._credential_source})")
        return session.client(client_type, **extra_kwargs)

    @property
    def has_credentials(self) -> bool:
        """Check if AWS credentials are available (from any source)."""
        try:
            session = self._get_session()
            credentials = session.get_credentials()
            return credentials is not None
        except NoCredentialsError:
            return False
        except Exception as e:
            logger.warning(f"Error checking credentials: {e}")
            return False

    @property
    def credential_source(self) -> str:
        """Return the source of the current credentials."""
        # Trigger session creation if not already done
        self._get_session()
        return self._credential_source

    def get_config_info(self) -> Dict[str, Any]:
        """
        Get information about the current configuration (for debugging/status endpoints).

        Returns:
            Dictionary with configuration details (credentials are masked)
        """
        return {
            "region": self.region_name,
            "has_credentials": self.has_credentials,
            "credential_source": self.credential_source,
            "profile_name": self._profile_name,
            "has_session_token": bool(self._session_token),
        }

