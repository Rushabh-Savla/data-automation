"""Live job progress over WebSocket. Sends the current snapshot on connect,
then forwards every subsequent Redis pub/sub event for the job (progress
deltas, connector_finished, job_status transitions, errors).

Auth is via an `api_key` query param, not a header, because browsers cannot
set custom headers during the WebSocket handshake.
"""

import asyncio
import json

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from app.core.config import get_settings
from app.core.redis_client import get_redis
from app.services.progress_service import ProgressPublisher

router = APIRouter(tags=["websocket"])


@router.websocket("/ws/v1/jobs/{job_id}")
async def job_progress_ws(websocket: WebSocket, job_id: str, api_key: str | None = Query(default=None)):
    settings = get_settings()
    if not api_key or api_key not in settings.api_key_set:
        await websocket.close(code=4401)
        return

    await websocket.accept()

    publisher = ProgressPublisher(get_redis())
    snapshot = publisher.get_snapshot(job_id)
    if snapshot is not None:
        await websocket.send_json(snapshot)

    pubsub = get_redis().pubsub()
    pubsub.subscribe(publisher.channel_name(job_id))

    async def forward_events() -> None:
        while True:
            message = await asyncio.to_thread(pubsub.get_message, timeout=1.0)
            if message and message.get("type") == "message":
                await websocket.send_text(message["data"])

    async def watch_disconnect() -> None:
        # The client never needs to send anything; this just resolves as soon
        # as the socket closes, so we can cancel the forwarding task.
        await websocket.receive_text()

    forward_task = asyncio.ensure_future(forward_events())
    disconnect_task = asyncio.ensure_future(watch_disconnect())
    try:
        done, pending = await asyncio.wait(
            {forward_task, disconnect_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        for task in done:
            task.exception()  # surface WebSocketDisconnect etc. without raising here
    except WebSocketDisconnect:
        pass
    finally:
        forward_task.cancel()
        disconnect_task.cancel()
        pubsub.unsubscribe()
        pubsub.close()
