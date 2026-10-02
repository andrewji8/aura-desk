"""WebSocket transport for the virtual-human streaming session.

One connection drives four concurrent tasks:

``endpoint`` -> accepts, supervises, and guarantees cleanup
``receiver``  -> validates frames, feeds ``input_queue``, honours barge-in
``sender``    -> drains ``output_queue`` onto the wire
``pipeline``  -> :class:`~aura_gateway.pipeline.streaming.StreamingPipeline`

The endpoint never processes audio itself: it only owns task lifetime, so a
client disconnect cannot leave a model call running.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
import json
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from aura_gateway.core.llm import get_llm_client
from aura_gateway.core.logging import (
    bind_request_context,
    clear_request_context,
    get_logger,
    new_request_id,
)
from aura_gateway.pipeline.streaming import InboundItem, PipelineItem, StreamingPipeline
from aura_schemas.ws import (
    MAX_AUDIO_FRAME_BYTES,
    ClientAudioMessage,
    ClientInterruptMessage,
    ClientMessage,
    ClientMessageType,
    ErrorPayload,
    PipelineStatus,
    ServerMessage,
    ServerMessageType,
    StatusPayload,
    build_server_message,
    parse_client_message,
)

__all__ = ["WEBSOCKET_PATH", "router", "virtual_human_socket"]

#: Route template, kept next to its registration so the two cannot drift.
WEBSOCKET_PATH = "/ws/virtual-human/{session_id}"

router = APIRouter()
_log = get_logger("aura_gateway.ws")


def _frame(message: ServerMessage) -> dict[str, Any]:
    """Render a server message as a JSON-serialisable dictionary."""
    return message.model_dump(mode="json")


def _status_frame(
    session_id: str, state: PipelineStatus, detail: str | None = None
) -> dict[str, Any]:
    """Build a status frame ready for ``send_json``."""
    return _frame(
        build_server_message(
            ServerMessageType.STATUS,
            session_id,
            status=StatusPayload(state=state, detail=detail),
        ),
    )


def _error_frame(
    session_id: str, code: str, message: str, *, fatal: bool = False
) -> dict[str, Any]:
    """Build an error frame ready for ``send_json``."""
    return _frame(
        build_server_message(
            ServerMessageType.ERROR,
            session_id,
            error=ErrorPayload(code=code, message=message, fatal=fatal),
        ),
    )


async def _send_json_safe(websocket: WebSocket, payload: dict[str, Any]) -> bool:
    """Send one frame, returning ``False`` if the socket is already gone.

    Args:
        websocket: The connection to write to.
        payload: A JSON-serialisable frame.

    Returns:
        ``True`` when the frame reached the wire.
    """
    try:
        await websocket.send_json(payload)
    except (RuntimeError, WebSocketDisconnect):
        return False
    return True


async def _send_status(
    websocket: WebSocket,
    session_id: str,
    state: PipelineStatus,
    detail: str | None = None,
) -> bool:
    """Emit one status frame, tolerating a socket that just went away."""
    return await _send_json_safe(websocket, _status_frame(session_id, state, detail))


async def _receiver(websocket: WebSocket, pipeline: StreamingPipeline) -> None:
    """Read client frames until disconnect, feeding the pipeline.

    Barge-in is executed here rather than queued: this task calls
    :meth:`StreamingPipeline.handle_interrupt` directly, so the output queue is
    cleared and generation is cancelled without waiting behind queued audio.

    Args:
        websocket: The accepted connection.
        pipeline: The session pipeline that receives validated messages.

    Raises:
        WebSocketDisconnect: When the client closes the connection.
    """
    session_id = pipeline.session_id
    log = _log.bind(session_id=session_id)
    # Set by an `audio` control frame, consumed by the next binary frame.
    awaiting_audio = False

    try:
        while True:
            raw = await websocket.receive()
            frame_type = raw["type"]

            if frame_type == "websocket.disconnect":
                log.info("ws.client_disconnected", code=raw.get("code"))
                raise WebSocketDisconnect(raw.get("code", 1000))

            audio_bytes = raw.get("bytes")
            if audio_bytes is not None:
                if not awaiting_audio:
                    log.warning("ws.unexpected_binary_frame", size=len(audio_bytes))
                    continue
                if len(audio_bytes) > MAX_AUDIO_FRAME_BYTES:
                    log.warning("ws.oversized_audio_frame", size=len(audio_bytes))
                    awaiting_audio = False
                    continue
                await pipeline.submit_audio(
                    ClientAudioMessage(session_id=session_id),
                    audio_bytes,
                )
                log.info("ws.received", kind="audio", size=len(audio_bytes))
                awaiting_audio = False
                continue

            text_payload = raw.get("text")
            if text_payload is None:
                continue

            try:
                message = parse_client_message(json.loads(text_payload))
            except ValueError as exc:  # covers json.JSONDecodeError as well
                log.warning("ws.invalid_frame", error=str(exc))
                await _send_json_safe(
                    websocket,
                    _error_frame(session_id, "invalid_message", "Payload failed validation"),
                )
                continue

            log.info("ws.received", kind=message.type, sequence=message.sequence)

            if message.type == ClientMessageType.INTERRUPT:
                await _handle_interrupt(pipeline, message)
                continue

            if message.type == ClientMessageType.AUDIO:
                # The control frame declares that a binary frame follows.
                awaiting_audio = True
                continue

            await pipeline.submit(InboundItem(message=message))
    except WebSocketDisconnect:
        raise
    except asyncio.CancelledError:
        log.info("ws.receiver_cancelled")
        raise


async def _handle_interrupt(pipeline: StreamingPipeline, message: ClientMessage) -> None:
    """Execute a barge-in immediately, without queueing it.

    Args:
        pipeline: The session pipeline to interrupt.
        message: The validated interrupt frame.
    """
    interrupt = message if isinstance(message, ClientInterruptMessage) else None
    await pipeline.handle_interrupt(interrupt)


async def _sender(websocket: WebSocket, pipeline: StreamingPipeline) -> None:
    """Drain the output queue onto the wire.

    Binary payloads are written after the metadata frame that announced them,
    preserving the ordering a client needs to reassemble the stream.

    Args:
        websocket: The accepted connection.
        pipeline: Session pipeline producing outbound items.

    Raises:
        WebSocketDisconnect: When the socket is closed underneath the writer.
    """
    session_id = pipeline.session_id
    log = _log.bind(session_id=session_id)
    sent = 0

    try:
        while True:
            item = await pipeline.output_queue.get()
            try:
                if not isinstance(item, PipelineItem):
                    continue
                if not await _send_json_safe(websocket, _frame(item.message)):
                    raise WebSocketDisconnect(1000)
                if item.audio is not None:
                    await websocket.send_bytes(item.audio)
                sent += 1
                log.info("ws.sent", kind=item.message.type)
            finally:
                pipeline.output_queue.task_done()
    except (WebSocketDisconnect, RuntimeError):
        log.info("ws.sender_disconnected", frames_sent=sent)
        raise


@router.websocket(WEBSOCKET_PATH)
async def virtual_human_socket(websocket: WebSocket, session_id: str) -> None:
    """Stream a virtual-human session over one websocket connection.

    Args:
        websocket: The connection FastAPI accepted for this route.
        session_id: Caller-provided session identifier, echoed on every frame.

    The function always reaches its cleanup block: a client disconnect, a
    pipeline error and a normal return all cancel the background tasks and
    drain the queues.
    """
    request_id = new_request_id()
    bind_request_context(request_id, session_id=session_id)
    log = _log.bind(session_id=session_id, request_id=request_id)
    # The HTTP client is shared process-wide so pooled connections are reused
    # across sessions instead of being rebuilt per turn.
    pipeline = StreamingPipeline(session_id, client=get_llm_client())

    await websocket.accept()
    log.info("ws.connected", path=websocket.url.path)
    await _send_status(websocket, session_id, PipelineStatus.LISTENING, "ready")

    pipeline.start()
    receiver = asyncio.create_task(_receiver(websocket, pipeline), name=f"ws-recv:{session_id}")
    sender = asyncio.create_task(_sender(websocket, pipeline), name=f"ws-send:{session_id}")
    background = (receiver, sender)

    try:
        done, _pending = await asyncio.wait(background, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            failure = task.exception()
            if failure is not None and not isinstance(
                failure,
                (WebSocketDisconnect, asyncio.CancelledError),
            ):
                log.error("ws.task_failed", task=task.get_name(), error=str(failure))
                await _send_json_safe(
                    websocket,
                    _error_frame(session_id, "stream_failed", "Streaming pipeline failed"),
                )
    except WebSocketDisconnect:
        log.info("ws.disconnected")
    finally:
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        await pipeline.shutdown()
        await _send_status(websocket, session_id, PipelineStatus.CLOSED)
        with suppress(RuntimeError, WebSocketDisconnect):
            await websocket.close()
        log.info("ws.closed")
        clear_request_context()
