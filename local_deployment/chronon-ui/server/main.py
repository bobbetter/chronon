from contextlib import asynccontextmanager
from pathlib import Path
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from server.routes import router
from server.services.datascanner import DataScanner
from server.services.teamparser import TeamParser
import logging

logger = logging.getLogger("uvicorn.error")

server_root = Path(__file__).parent
teams_metadata_dir = str(server_root / "compiled" / "teams_metadata")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize shared app resources once (e.g., DataScanner, TeamParser)."""
    warehouse_path = os.environ.get("SPARK_WAREHOUSE_PATH")
    if not warehouse_path:
        warehouse_path = str(server_root / "spark-data/warehouse")
    logger.info(f"Using warehouse path: {warehouse_path}")
    app.state.datascanner = DataScanner(warehouse_path)

    logger.info(f"Using teams metadata directory: {teams_metadata_dir}")
    app.state.teamparser = TeamParser(teams_metadata_dir)
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
