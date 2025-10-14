from fastapi import APIRouter
from .v1.graph import router as graph_router

router = APIRouter()
router.include_router(graph_router)