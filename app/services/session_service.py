import uuid
import time
from fastapi import HTTPException
from ..cache import redis_client
from .. import schemas

def initiate_session() -> schemas.SessionInfo:
    """
    Generates a new unique session token for a client and stores it in Redis.
    """
    session_token = str(uuid.uuid4())
    # Set with an expiration of 45 minutes, renewed by heartbeat.
    redis_client.set(f"session:{session_token}", int(time.time()), ex=60 * 45)
    return schemas.SessionInfo(session_token=session_token)

def process_heartbeat(session: schemas.SessionInfo) -> dict:
    """
    Refreshes the TTL of an active session token.
    """
    token_key = f"session:{session.session_token}"
    if redis_client.exists(token_key):
        # Reset the expiration time to 45 minutes from now.
        redis_client.expire(token_key, 60 * 45)
        return {"status": "ok"}
    else:
        # If the session key has expired or is invalid, raise an error.
        raise HTTPException(status_code=404, detail="Session not found or expired.")