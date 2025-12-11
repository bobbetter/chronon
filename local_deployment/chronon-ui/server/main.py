from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from server.routes import router
from server.services.datascanner_local import DataScannerLocal
from server.services.dynamodb import create_local_dynamodb_client, DynamoDBClient
from server.services.runner import SparkJobRunner
from server.services.remote.datascanner_remote import DataScannerRemote
from server.services.teamparser import TeamParser
from server.services.graphparser import GraphParser
from server.services.remote.boto_client import BotoClient
from server.services.kinesis import KinesisClient
from server.config import (
    warehouse_path,
    teams_metadata_dir,
    compiled_dir_gbs,
    compiled_dir_joins,
)

logger = logging.getLogger("uvicorn.error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize shared app resources once (e.g., DataScanner, TeamParser, GraphParser, BotoClient)."""
    logger.info("Initializing AWS BotoClient for remote services...")
    app.state.boto_client = BotoClient()

    logger.info("Initializing local Spark Job Runner")
    app.state.spark_runner = SparkJobRunner()

    logger.info(f"Initializing local DataScanner with warehouse path: {warehouse_path}")
    app.state.datascanner_local = DataScannerLocal(warehouse_path)

    logger.info("Initializing remote DataScanner with AWS BotoClient")
    app.state.datascanner_remote = DataScannerRemote(boto_client=app.state.boto_client)

    logger.info("Initializing local DynamoDB client...")
    app.state.dynamodb_client_local = create_local_dynamodb_client()

    logger.info("Initializing remote DynamoDB client with AWS BotoClient...")
    app.state.dynamodb_client_remote = DynamoDBClient(app.state.boto_client)

    logger.info(f"Using teams metadata directory: {teams_metadata_dir}")
    app.state.teamparser = TeamParser(teams_metadata_dir)

    logger.info("Initializing Kinesis client...")
    app.state.kinesis_client = KinesisClient()

    logger.info(f"Using compiled directories - GBs: {compiled_dir_gbs}, Joins: {compiled_dir_joins}")
    app.state.graphparser = GraphParser(
        directory_path_gbs=compiled_dir_gbs,
        directory_path_joins=compiled_dir_joins,
        datascanner_local=app.state.datascanner_local,
        datascanner_remote=app.state.datascanner_remote,
        dynamodb_client_local=app.state.dynamodb_client_local,
        dynamodb_client_remote=app.state.dynamodb_client_remote,
        kinesis_client=app.state.kinesis_client,
    )

    yield


app = FastAPI(lifespan=lifespan)

# Enable CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify the frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)
