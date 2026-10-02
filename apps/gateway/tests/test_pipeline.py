"""Streaming pipeline behaviour with a mocked upstream LLM.

The LLM stage runs against ``httpx.MockTransport``, so SSE parsing, status
mapping and retry policy are exercised without a network.
"""

from __future__ import annotations

import asyncio

import httpx
from llm_doubles import (
    AsyncByteStream,
    delayed_sse_bytes,
    error_response,
    mock_client,
    sse_response,
    token_frames,
    token_response,
)
import pytest

from aura_gateway.core.config import Settings
from aura_gateway.pipeline.streaming import (
    SHUTDOWN,
    InboundItem,
    LLMProtocolError,
    LLMRateLimitError,
    LLMTimeoutError,
    LLMUpstreamError,
    PipelineItem,
    StreamingPipeline,
)
from aura_schemas.ws import (
    ClientAudioMessage,
    ClientInterruptMessage,
    ClientTextMessage,
    PipelineStatus,
    ServerMessageType,
    TextPayload,
)

# Full-width punctuation would trip RUF001, so the fixture uses ASCII commas.
REPLY = ("您好", ", ", "我是", "Aura", "助手")
SETTINGS = Settings(_env_file=None, LLM_API_KEY="test-key", LLM_RETRY_BACKOFF=0.0)


def _pipeline(handler: object, **kwargs: object) -> StreamingPipeline:
    """Build a pipeline wired to a mocked upstream."""
    client = mock_client(handler)  # type: ignore[arg-type]
    return StreamingPipeline("s1", client=client, settings=SETTINGS, **kwargs)  # type: ignore[arg-type]


def _text_message(session_id: str = "s1", text: str = "你好") -> ClientTextMessage:
    return ClientTextMessage(session_id=session_id, text=TextPayload(text=text))


async def _drain(pipeline: StreamingPipeline) -> list[PipelineItem]:
    """Empty the output queue and return its items."""
    items: list[PipelineItem] = []
    while not pipeline.output_queue.empty():
        item = await pipeline.output_queue.get()
        assert isinstance(item, PipelineItem)
        items.append(item)
    return items


async def _collect(pipeline: StreamingPipeline) -> list[str]:
    """Return the text deltas a pipeline emits for one queued turn."""
    await pipeline.submit(InboundItem(message=_text_message()))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=10)
    return [
        item.message.text.text  # type: ignore[union-attr]
        for item in await _drain(pipeline)
        if item.message.type == ServerMessageType.TEXT_DELTA  # type: ignore[union-attr]
    ]


# ---- happy path ------------------------------------------------------------


async def test_streams_real_deltas() -> None:
    pipeline = _pipeline(lambda request: token_response(*REPLY))
    pipeline.start()

    deltas = await _collect(pipeline)

    assert deltas[:-1] == list(REPLY)
    assert deltas[-1] == "".join(REPLY)  # final frame carries the whole reply

    await pipeline.shutdown()


async def test_full_turn_streams_text_then_audio() -> None:
    pipeline = _pipeline(lambda request: token_response("您好", "世界"))
    pipeline.start()

    await pipeline.submit(InboundItem(message=_text_message()))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=10)
    kinds = [item.message.type for item in await _drain(pipeline)]  # type: ignore[union-attr]

    assert ServerMessageType.TEXT_DELTA in kinds
    assert ServerMessageType.AUDIO_CHUNK in kinds
    # Text is fully streamed before any audio is emitted.
    assert kinds.index(ServerMessageType.TEXT_DELTA) < kinds.index(ServerMessageType.AUDIO_CHUNK)

    await pipeline.shutdown()


async def test_done_marker_ends_stream() -> None:
    pipeline = _pipeline(
        lambda request: sse_response(*token_frames("a"), "data: [DONE]", 'data: {"x":1}')
    )
    pipeline.start()

    assert await _collect(pipeline) == ["a", "a"]
    await pipeline.shutdown()


async def test_keep_alive_comments_and_blank_lines_are_ignored() -> None:
    pipeline = _pipeline(
        lambda request: sse_response(": ping", "", *token_frames("x"), "data: [DONE]"),
    )
    pipeline.start()

    assert await _collect(pipeline) == ["x", "x"]
    await pipeline.shutdown()


async def test_usage_only_frame_is_skipped() -> None:
    pipeline = _pipeline(
        lambda request: sse_response(
            *token_frames("x"),
            'data: {"choices":[],"usage":{"total_tokens":5}}',
            "data: [DONE]",
        ),
    )
    pipeline.start()

    assert await _collect(pipeline) == ["x", "x"]
    await pipeline.shutdown()


async def test_request_targets_chat_completions_with_auth() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return token_response("ok")

    pipeline = _pipeline(handler)
    pipeline.start()
    await _collect(pipeline)

    request = seen[0]
    assert str(request.url) == "https://api.deepseek.com/v1/chat/completions"
    assert request.headers["authorization"] == "Bearer test-key"
    assert request.headers["accept"] == "text/event-stream"
    assert request.method == "POST"

    await pipeline.shutdown()


async def test_conversation_history_is_sent_on_second_turn() -> None:
    bodies: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content.decode())
        return token_response("好")

    pipeline = _pipeline(handler)
    pipeline.start()
    await _collect(pipeline)
    await _collect(pipeline)

    assert len(bodies) == 2
    assert "你好" in bodies[1]
    assert "好" in bodies[1]

    await pipeline.shutdown()


# ---- error handling --------------------------------------------------------


async def test_rate_limit_emits_error_frame_and_ends_turn() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return error_response(429, "slow down")

    pipeline = _pipeline(handler)
    pipeline.start()
    await pipeline.submit(InboundItem(message=_text_message()))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=10)

    items = await _drain(pipeline)
    errors = [item for item in items if item.message.type == ServerMessageType.ERROR]  # type: ignore[union-attr]

    assert len(errors) == 1
    assert errors[0].message.error.code == "llm_rate_limited"  # type: ignore[union-attr]
    # Initial attempt plus LLM_MAX_RETRIES.
    assert attempts == SETTINGS.LLM_MAX_RETRIES + 1
    # The processor survived the failure and is ready for the next message.
    assert pipeline.task is not None
    assert not pipeline.task.done()

    await pipeline.shutdown()


async def test_non_retryable_status_is_not_retried() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return error_response(401, "bad key")

    pipeline = _pipeline(handler)
    pipeline.start()
    await pipeline.submit(InboundItem(message=_text_message()))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=10)

    errors = [
        item
        for item in await _drain(pipeline)
        if item.message.type == ServerMessageType.ERROR  # type: ignore[union-attr]
    ]

    assert attempts == 1
    assert errors[0].message.error.code == "llm_upstream_401"  # type: ignore[union-attr]

    await pipeline.shutdown()


async def test_transient_status_is_retried_then_succeeds() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return error_response(503, "warming up")
        return token_response("恢复")

    pipeline = _pipeline(handler)
    pipeline.start()

    assert await _collect(pipeline) == ["恢复", "恢复"]
    assert attempts == 2

    await pipeline.shutdown()


async def test_timeout_emits_error_frame() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("too slow", request=request)

    pipeline = _pipeline(handler)
    pipeline.start()
    await pipeline.submit(InboundItem(message=_text_message()))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=10)

    errors = [
        item
        for item in await _drain(pipeline)
        if item.message.type == ServerMessageType.ERROR  # type: ignore[union-attr]
    ]

    assert errors[0].message.error.code == "llm_timeout"  # type: ignore[union-attr]

    await pipeline.shutdown()


async def test_invalid_json_emits_protocol_error() -> None:
    pipeline = _pipeline(lambda request: sse_response("data: {not json", "data: [DONE]"))
    pipeline.start()
    await pipeline.submit(InboundItem(message=_text_message()))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=10)

    errors = [
        item
        for item in await _drain(pipeline)
        if item.message.type == ServerMessageType.ERROR  # type: ignore[union-attr]
    ]

    assert errors[0].message.error.code == "llm_protocol_error"  # type: ignore[union-attr]

    await pipeline.shutdown()


async def test_empty_reply_is_reported() -> None:
    pipeline = _pipeline(lambda request: sse_response("data: [DONE]"))
    pipeline.start()
    await pipeline.submit(InboundItem(message=_text_message()))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=10)

    errors = [
        item
        for item in await _drain(pipeline)
        if item.message.type == ServerMessageType.ERROR  # type: ignore[union-attr]
    ]

    assert errors[0].message.error.code == "llm_empty_response"  # type: ignore[union-attr]

    await pipeline.shutdown()


async def test_truncated_stream_is_not_retried() -> None:
    """A stream that dies mid-event must not replay the delta already sent."""
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=_truncated_stream(),
        )

    pipeline = _pipeline(handler)
    pipeline.start()

    deltas = await _collect(pipeline)

    # Only the delta that arrived before the break, and no retry, because a
    # replay would duplicate text the client already received.
    assert attempts == 1
    assert deltas == ["a"]

    await pipeline.shutdown()


def _truncated_stream() -> bytes:
    """A stream cut mid-event; httpx surfaces the remainder as a read error."""
    return b'data: {"choices": [{"delta": {"content": "a"}}]}\n\ndata: {"choices": [{"del'


async def test_error_after_first_token_surfaces_protocol_error() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return sse_response(*token_frames("a"), "data: {broken", "data: [DONE]")

    pipeline = _pipeline(handler)
    pipeline.start()
    await pipeline.submit(InboundItem(message=_text_message()))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=10)

    items = await _drain(pipeline)
    errors = [item for item in items if item.message.type == ServerMessageType.ERROR]  # type: ignore[union-attr]

    # The client keeps the delta it already received, then learns about the fault.
    assert attempts == 1
    assert errors[0].message.error.code == "llm_protocol_error"  # type: ignore[union-attr]

    await pipeline.shutdown()


# ---- barge-in --------------------------------------------------------------


async def test_interrupt_drains_output_queue() -> None:
    pipeline = _pipeline(lambda request: token_response(*("x" * 200)))
    pipeline.start()

    for _ in range(20):
        await pipeline.output_queue.put(PipelineItem(message=object()))

    dropped = await pipeline.handle_interrupt(ClientInterruptMessage(session_id="s1"))

    # Everything queued before the barge-in is discarded; only the fresh
    # `interrupted` acknowledgement survives.
    assert dropped == 20
    remaining = await _drain(pipeline)
    assert [item.message.type for item in remaining] == [ServerMessageType.STATUS]  # type: ignore[union-attr]
    assert remaining[0].message.status.state is PipelineStatus.INTERRUPTED  # type: ignore[union-attr]

    await pipeline.shutdown()


async def test_interrupt_cancels_in_flight_stream() -> None:
    def slow(request: httpx.Request) -> httpx.Response:
        lines = [*token_frames(*(f"t{i}" for i in range(200))), "data: [DONE]"]
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=AsyncByteStream(delayed_sse_bytes(*lines)),
        )

    pipeline = _pipeline(slow)
    pipeline.start()

    await pipeline.submit(InboundItem(message=_text_message()))
    await asyncio.sleep(0.05)  # let the first deltas flow

    task = pipeline.task
    assert task is not None
    await pipeline.handle_interrupt(ClientInterruptMessage(session_id="s1"))

    await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), timeout=5)
    assert task.cancelled() is True

    await pipeline.shutdown()


async def test_interrupted_turn_stops_producing_output() -> None:
    def slow(request: httpx.Request) -> httpx.Response:
        lines = [*token_frames(*("y" for _ in range(200))), "data: [DONE]"]
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=AsyncByteStream(delayed_sse_bytes(*lines)),
        )

    pipeline = _pipeline(slow)
    pipeline.start()

    await pipeline.submit(InboundItem(message=_text_message()))
    await asyncio.sleep(0.05)
    await pipeline.handle_interrupt(ClientInterruptMessage(session_id="s1"))

    await asyncio.sleep(0.05)
    after = await _drain(pipeline)

    assert [item.message.type for item in after] == [ServerMessageType.STATUS]  # type: ignore[union-attr]

    await pipeline.shutdown()


async def test_interrupt_inside_processor_does_not_self_cancel() -> None:
    pipeline = _pipeline(lambda request: token_response("x"))
    pipeline.start()

    await pipeline.submit(InboundItem(message=ClientInterruptMessage(session_id="s1")))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=5)

    task = pipeline.task
    assert task is not None
    assert task.cancelled() is False

    items = await _drain(pipeline)
    assert items[-1].message.status.state is PipelineStatus.INTERRUPTED  # type: ignore[union-attr]

    await pipeline.shutdown()


async def test_interrupt_can_discard_conversation_context() -> None:
    bodies: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content.decode())
        return token_response("好")

    pipeline = _pipeline(handler)
    pipeline.start()
    await _collect(pipeline)
    await _collect(pipeline)

    # Routed through the queue so the processor handles it internally, which
    # clears the context without cancelling itself.
    interrupt = ClientInterruptMessage(
        session_id="s1",
        interrupt={"reason": "user_speech", "keep_context": False},
    )
    await pipeline.submit(InboundItem(message=interrupt))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=5)
    await _drain(pipeline)

    await _collect(pipeline)

    # Turn 2 carries the first exchange in its history; turn 3, after the
    # barge-in discarded it, carries only the fresh user message.
    assert bodies[1].count("你好") == 2
    assert bodies[2].count("你好") == 1

    await pipeline.shutdown()


# ---- misc ------------------------------------------------------------------


async def test_oversized_audio_frame_rejected() -> None:
    pipeline = _pipeline(lambda request: token_response("x"))
    pipeline.start()

    with pytest.raises(ValueError, match="exceeds"):
        await pipeline.submit_audio(ClientAudioMessage(session_id="s1"), bytes(20_000))

    await pipeline.shutdown()


async def test_shutdown_sentinel_ends_processor() -> None:
    pipeline = _pipeline(lambda request: token_response("x"))
    pipeline.start()

    await pipeline.input_queue.put(SHUTDOWN)
    task = pipeline.task
    assert task is not None

    await asyncio.wait_for(task, timeout=5)

    assert task.done()
    await pipeline.shutdown()


async def test_concurrent_sessions_are_isolated() -> None:
    client = mock_client(lambda request: token_response("共享"))
    first = StreamingPipeline("s1", client=client, settings=SETTINGS)
    second = StreamingPipeline("s2", client=client, settings=SETTINGS)
    first.start()
    second.start()

    await first.submit(InboundItem(message=_text_message("s1")))
    await second.submit(InboundItem(message=_text_message("s2")))
    await asyncio.wait_for(
        asyncio.gather(first.input_queue.join(), second.input_queue.join()),
        timeout=10,
    )

    assert first.session_id == "s1"
    assert second.session_id == "s2"
    assert not first.output_queue.empty()
    assert not second.output_queue.empty()

    await first.shutdown()
    await second.shutdown()
    await client.aclose()


def test_error_types_carry_codes() -> None:
    assert LLMTimeoutError("x").code == "llm_timeout"
    assert LLMRateLimitError("x").code == "llm_rate_limited"
    assert LLMUpstreamError("x", status_code=500, retryable=True).code == "llm_upstream_500"
    assert LLMProtocolError("x").code == "llm_protocol_error"
    assert LLMTimeoutError("x").retryable is True
    assert LLMUpstreamError("x", status_code=401, retryable=False).retryable is False
