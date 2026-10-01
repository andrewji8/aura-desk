"""Streaming pipeline behaviour, including barge-in cancellation."""

from __future__ import annotations

import asyncio

import pytest

from aura_gateway.pipeline.streaming import (
    LLM_TOKEN_INTERVAL,
    SHUTDOWN,
    InboundItem,
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


async def _drain(pipeline: StreamingPipeline) -> list[PipelineItem]:
    """Empty the output queue and return its items."""
    items: list[PipelineItem] = []
    while not pipeline.output_queue.empty():
        item = await pipeline.output_queue.get()
        assert isinstance(item, PipelineItem)
        items.append(item)
    return items


def _text_message(session_id: str, text: str = "你好") -> ClientTextMessage:
    return ClientTextMessage(session_id=session_id, text=TextPayload(text=text))


async def test_full_turn_streams_text_then_audio() -> None:
    pipeline = StreamingPipeline("s1")
    pipeline.start()

    await pipeline.submit(InboundItem(message=_text_message("s1")))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=10)

    items = await _drain(pipeline)
    kinds = [item.message.type for item in items]

    assert ServerMessageType.STATUS in kinds
    assert ServerMessageType.TEXT_DELTA in kinds
    assert ServerMessageType.AUDIO_CHUNK in kinds
    # Audio metadata is always immediately followed by its payload.
    assert any(item.is_binary for item in items)

    await pipeline.shutdown()


async def test_binary_item_follows_its_metadata_frame() -> None:
    pipeline = StreamingPipeline("s1")
    pipeline.start()

    await pipeline.submit(InboundItem(message=_text_message("s1")))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=10)

    items = await _drain(pipeline)
    for index, item in enumerate(items[:-1]):
        if item.is_binary:
            assert items[index + 1].message.type == ServerMessageType.AUDIO_CHUNK

    await pipeline.shutdown()


async def test_interrupt_drains_output_queue() -> None:
    pipeline = StreamingPipeline("s1")
    pipeline.start()

    for _ in range(20):
        await pipeline.output_queue.put(PipelineItem(message=object()))

    dropped = await pipeline.handle_interrupt(ClientInterruptMessage(session_id="s1"))

    # Everything queued before the barge-in is discarded; the only survivor is
    # the fresh `interrupted` acknowledgement emitted by the handler itself.
    assert dropped == 20
    assert pipeline.interrupted is True

    remaining = await _drain(pipeline)
    assert [item.message.type for item in remaining] == [ServerMessageType.STATUS]
    assert remaining[0].message.status.state is PipelineStatus.INTERRUPTED

    await pipeline.shutdown()


async def test_interrupt_cancels_in_flight_generation() -> None:
    pipeline = StreamingPipeline("s1")
    pipeline.start()

    await pipeline.submit(InboundItem(message=_text_message("s1")))
    # Let the generator emit a couple of deltas, then cut in.
    await asyncio.sleep(LLM_TOKEN_INTERVAL * 3)

    task = pipeline.task
    assert task is not None
    await pipeline.handle_interrupt(ClientInterruptMessage(session_id="s1"))

    await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), timeout=5)
    assert task.cancelled() is True

    await pipeline.shutdown()


async def test_interrupted_turn_stops_producing_output() -> None:
    pipeline = StreamingPipeline("s1")
    pipeline.start()

    await pipeline.submit(InboundItem(message=_text_message("s1")))
    await asyncio.sleep(LLM_TOKEN_INTERVAL * 2)
    await pipeline.handle_interrupt(ClientInterruptMessage(session_id="s1"))

    await asyncio.sleep(LLM_TOKEN_INTERVAL * 5)
    after = await _drain(pipeline)

    assert [item.message.type for item in after] == [ServerMessageType.STATUS]

    await pipeline.shutdown()


async def test_interrupt_inside_processor_does_not_self_cancel() -> None:
    pipeline = StreamingPipeline("s1")
    pipeline.start()

    # Routed through the input queue: the processor drains and reports the
    # interrupted state without cancelling the task running it.
    await pipeline.submit(InboundItem(message=ClientInterruptMessage(session_id="s1")))
    await asyncio.wait_for(pipeline.input_queue.join(), timeout=5)

    task = pipeline.task
    assert task is not None
    assert task.cancelled() is False

    items = await _drain(pipeline)
    status = items[-1].message
    assert status.type == ServerMessageType.STATUS
    assert status.status.state is PipelineStatus.INTERRUPTED

    await pipeline.shutdown()


async def test_oversized_audio_frame_rejected() -> None:
    pipeline = StreamingPipeline("s1")
    pipeline.start()

    with pytest.raises(ValueError, match="exceeds"):
        await pipeline.submit_audio(ClientAudioMessage(session_id="s1"), bytes(20_000))

    await pipeline.shutdown()


async def test_shutdown_sentinel_ends_processor() -> None:
    pipeline = StreamingPipeline("s1")
    pipeline.start()

    await pipeline.input_queue.put(SHUTDOWN)
    task = pipeline.task
    assert task is not None

    await asyncio.wait_for(task, timeout=5)

    assert task.done()
    await pipeline.shutdown()


async def test_concurrent_sessions_are_isolated() -> None:
    first = StreamingPipeline("s1")
    second = StreamingPipeline("s2")
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
