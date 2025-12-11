"""
API routes for EMR Serverless operations.
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Request

from server.services.remote.emr import (
    EmrServerlessRunner,
    DEFAULT_APPLICATION_ID,
    StartApplicationRequest,
    ApplicationStateResponse,
    ApplicationStates,
    EmrJobRequest,
    EmrJobResult,
)

logger = logging.getLogger("uvicorn.error")
router = APIRouter(prefix="/v1/remote-resources/emr", tags=["remote-resources"])


def get_emr_runner(request: Request) -> EmrServerlessRunner:
    """Get EMR runner using the shared boto_client from app.state."""
    return EmrServerlessRunner(boto_client=request.app.state.boto_client)


@router.post("/application/start", response_model=ApplicationStateResponse)
async def start_application(
    request: StartApplicationRequest = StartApplicationRequest(),
    emr_runner: EmrServerlessRunner = Depends(get_emr_runner),
):
    """
    Start an EMR Serverless application.

    If the application is already started, returns the current state.
    If stopped, initiates start and waits for it to become ready.

    Args:
        body: Start application options

    Returns:
        ApplicationStateResponse with current state
    """
    try:
        application_id = request.application_id
        logger.info(f"Checking EMR application state: {application_id}")
        current_state = emr_runner.check_application_state(application_id)

        if current_state == ApplicationStates.STARTED:
            return ApplicationStateResponse(
                application_id=application_id,
                state=current_state,
                message="Application is already running",
            )

        if current_state == ApplicationStates.STOPPED:
            logger.info(f"Starting EMR application {application_id}...")
            success = emr_runner.start_application(
                application_id, timeout_seconds=request.timeout_seconds
            )
            if success:
                return ApplicationStateResponse(
                    application_id=application_id,
                    state=ApplicationStates.STARTED,
                    message="Application started successfully",
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to start application within {request.timeout_seconds}s",
                )

        # For other states (STARTING, CREATED, etc.)
        return ApplicationStateResponse(
            application_id=application_id,
            state=current_state,
            message=f"Application is in {current_state} state",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting EMR application: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to start application: {str(e)}"
        )


@router.get("/application/status", response_model=ApplicationStateResponse)
async def get_application_status(
    application_id: str = DEFAULT_APPLICATION_ID,
    emr_runner: EmrServerlessRunner = Depends(get_emr_runner),
):
    """
    Get the current state of an EMR Serverless application.

    Args:
        application_id: EMR Serverless application ID

    Returns:
        ApplicationStateResponse with current state
    """
    try:
        if not application_id:
            raise HTTPException(
                status_code=400,
                detail="application_id is required. Set CHRONON_EMR_APPLICATION_ID env var or provide as query param.",
            )

        logger.info(f"Getting EMR application state: {application_id}")
        state = emr_runner.check_application_state(application_id)

        return ApplicationStateResponse(
            application_id=application_id,
            state=state,
            message=f"Application is {state}",
        )

    except Exception as e:
        logger.error(f"Error getting EMR application status: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get application status: {str(e)}"
        )

