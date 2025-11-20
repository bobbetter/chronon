from fastapi import APIRouter
from .v1.graph import router as graph_router
from .v1.spark_data import router as spark_data_router
from .v1.actions import router as actions_router
from .v1.kinesis import router as kinesis_router
from .v1.teams import router as teams_router

router = APIRouter()
router.include_router(graph_router)
router.include_router(spark_data_router)
router.include_router(actions_router)
router.include_router(kinesis_router)
router.include_router(teams_router)
