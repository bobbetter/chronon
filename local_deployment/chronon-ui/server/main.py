from typing import Union
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from routes import router

app = FastAPI()

# Enable CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify the frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)


# Request/Response models
class NodeActionRequest(BaseModel):
    nodeName: str
    action: str

class NodeActionResponse(BaseModel):
    nodeName: str
    action: str
    timestamp: str
    status: str
    message: str



@app.post("/node_action", response_model=NodeActionResponse)
def execute_node_action(request: NodeActionRequest):
    """
    Execute an action on a node (e.g., backfill, upload, show)
    """
    # This is a placeholder implementation
    # In a real implementation, this would trigger actual backend operations
    return NodeActionResponse(
        nodeName=request.nodeName,
        action=request.action,
        timestamp=datetime.now().isoformat(),
        status="success",
        message=f"Successfully executed {request.action} on {request.nodeName}"
    )