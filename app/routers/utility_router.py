"""
utility_router.py

This module defines API endpoints for various utility functions, primarily focused
on session management for the frontend client.
"""
from fastapi import APIRouter, HTTPException
import uuid
import time

from ..cache import redis_client
from .. import schemas

router = APIRouter(
    prefix="/utils",
    tags=["Utilities"]
)

# --- Session Management Endpoints ---

@router.get("/session/initiate", response_model=schemas.SessionInfo)
def initiate_session():
    """
    Generates a new unique session token for a client.

    This endpoint is the first one a client should call. It creates a UUID-based
    token and stores it in Redis with a timestamp and an expiration time. This
    token is then used in subsequent API calls to associate requests with a
    specific user session, mainly for caching purposes.

    Returns:
        A SessionInfo object containing the new session_token.
    """
    session_token = str(uuid.uuid4())
    # Store the session token in Redis with its creation time as the value.
    # The expiration (ex=2700 seconds, or 45 minutes) acts as a safety net to
    # clean up abandoned sessions.
    redis_client.set(f"session:{session_token}", int(time.time()), ex=60 * 45)
    return schemas.SessionInfo(session_token=session_token)

@router.post("/session/heartbeat", response_model=dict)
def session_heartbeat(session: schemas.SessionInfo):
    """
    Allows a client to signal that its session is still active.

    The frontend calls this endpoint periodically. It updates the timestamp
    associated with the session token in Redis, effectively resetting its
    Time-To-Live (TTL). This prevents active user sessions from expiring.

    Args:
        session: A SessionInfo object containing the client's session_token.

    Returns:
        A dictionary indicating the status of the heartbeat operation.
    """
    token_key = f"session:{session.session_token}"
    if redis_client.exists(token_key):
        # If the session exists, update its timestamp and reset the TTL.
        redis_client.set(token_key, int(time.time()), ex=60 * 45)
        return {"status": "ok"}
    else:
        # If the session key doesn't exist, it has either expired or is invalid.
        # The client should be prompted to re-initiate a new session.
        raise HTTPException(status_code=404, detail="Session not found or expired.")