"""Mock ASR -> LLM -> TTS streaming pipeline.

The pipeline is a bounded-buffer producer/consumer stage::

    Receiver (websocket) -> input_queue -> process_stream -> output_queue -> Sender (websocket)

Running the stages as separate tasks decouples socket reads from model
latency: a slow client cannot stall generation, and a fast generator cannot
block the receive loop.

The AI stages are simulated with :func:`asyncio.sleep` and async generators, so
the whole link is exercisable without an external AI provider. Every ``await``
is a cancellation point, which is what makes barge-in fast:
:meth:`StreamingPipeline.handle_interrupt` drains the output queue and cancels
the processing task, so the in-flight generator is torn down at its next
suspension point instead of being awaited to completion.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from aura_gateway.core.logging import get_logger
from aura_schemas.ws import (
    MAX_AUDIO_FRAME_BYTES,
    AudioChunkPayload,
    AvatarAction,
    ClientInterruptMessage,
    ClientMessage,
    ClientTextMessage,
    ErrorPayload,
    PipelineStatus,
    ServerMessage,
    ServerMessageType,
    StatusPayload,
    TextPayload,
    build_server_message,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

__all__ = ["InboundItem", "PipelineItem", "StreamingPipeline"]

#: Sentinel pushed onto ``input_queue`` to ask the processor to return cleanly.
SHUTDOWN: Final = object()

#: Mock stage latencies in seconds. Deliberately short so a full round trip
#: finishes inside a test, yet non-zero to keep cancellation observable.
ASR_LATENCY: Final = 0.02
LLM_TOKEN_INTERVAL: Final = 0.01
TTS_CHUNK_INTERVAL: Final = 0.01

#: Size of one mock TTS chunk, and how many characters of text produce one.
TTS_CHUNK_BYTES: Final = 1_024
TTS_CHUNK_CHARS: Final = 6

#: Deterministic mock reply, split into tokens so the deltas read naturally.
#: Written with ASCII punctuation on purpose: the project's Ruff config rejects
#: ambiguous full-width characters (RUF001).
MOCK_REPLY: Final = "您好, 我是 Aura 虚拟助手, 很高兴为您服务. 请问有什么可以帮您?"


@dataclass(frozen=True, slots=True)
class InboundItem:
    """One validated client message, with its optional binary attachment."""

    message: ClientMessage
    audio: bytes | None = None


@dataclass(frozen=True, slots=True)
class PipelineItem:
    """One unit of outbound work travelling to the sender task.

    A JSON item carries a pre-validated server message. A binary item also
    carries the audio payload, which the sender writes after the metadata frame
    that announced it.
    """

    message: ServerMessage | object
    audio: bytes | None = None

    @property
    def is_binary(self) -> bool:
        """Whether this item requires a ``send_bytes`` call."""
        return self.audio is not None


class StreamingPipeline:
    """Coordinate one virtual-human streaming session.

    An instance owns the queues and the task running :meth:`process_stream` for
    exactly one session, so concurrency scales per session and no state is
    shared between sessions.
    """

    def __init__(
        self,
        session_id: str,
        *,
        input_queue_size: int = 32,
        output_queue_size: int = 64,
    ) -> None:
        """Create the queues for a session.

        Args:
            session_id: Correlation id attached to every log line and frame.
            input_queue_size: Buffered inbound items before the receiver applies
                backpressure to the socket.
            output_queue_size: Buffered outbound items before generation blocks,
                which is the intended behaviour when the client is slow.
        """
        self.session_id = session_id
        self.input_queue: asyncio.Queue[InboundItem | object] = asyncio.Queue(
            maxsize=input_queue_size,
        )
        self.output_queue: asyncio.Queue[PipelineItem | object] = asyncio.Queue(
            maxsize=output_queue_size,
        )
        self._task: asyncio.Task[None] | None = None
        self._log = get_logger("aura_gateway.pipeline").bind(session_id=session_id)
        self._interrupted = False
        self._turn = 0

    @property
    def task(self) -> asyncio.Task[None] | None:
        """The running processor task, or ``None`` when not started."""
        return self._task

    @property
    def interrupted(self) -> bool:
        """Whether the most recent turn was cut short by a barge-in."""
        return self._interrupted

    def start(self) -> None:
        """Start the processor task unless it is already running."""
        if self._task is None or self._task.done():
            self._log.info("pipeline.started")
            self._task = asyncio.create_task(
                self.process_stream(self.session_id, self.input_queue, self.output_queue),
                name=f"pipeline:{self.session_id}",
            )

    async def shutdown(self) -> None:
        """Stop the processor and release both queues.

        Cancellation is awaited rather than merely requested, so the processor's
        own ``finally`` blocks run and no task is left pending on the loop.
        """
        if self._task is not None and not self._task.done():
            self._task.cancel()
            # Shutting down is a cancellation: the processor logs it in its own
            # `except asyncio.CancelledError` branch and re-raises.
            with suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        await self._drain(self.output_queue)
        self._log.info("pipeline.stopped")

    async def _drain(self, queue: asyncio.Queue[object]) -> int:
        """Discard and count every currently queued item.

        Uses non-blocking ``get_nowait`` instead of :meth:`asyncio.Queue.join`
        so a barge-in never waits on a producer that is itself being cancelled.
        """
        dropped = 0
        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                return dropped
            else:
                dropped += 1

    async def handle_interrupt(
        self,
        message: ClientInterruptMessage | None = None,
        *,
        cancel_active: bool = True,
    ) -> int:
        """Execute a barge-in: drop queued output and cancel active generation.

        The output queue is emptied first so the client stops hearing audio as
        early as possible, and only then is generation cancelled.

        Args:
            message: The validated interrupt frame, used for log detail.
            cancel_active: Whether to cancel the processor task. Pass ``False``
                when the interrupt is handled from inside the processor itself,
                where cancelling would target the running task.

        Returns:
            The number of discarded outbound items.
        """
        self._interrupted = True
        dropped = await self._drain(self.output_queue)
        reason = message.interrupt.reason if message else "unspecified"
        keep_context = message.interrupt.keep_context if message else True

        cancelled = False
        if cancel_active and self._task is not None and not self._task.done():
            self._task.cancel()
            cancelled = True

        self._log.info(
            "pipeline.interrupted",
            reason=reason,
            dropped_frames=dropped,
            turn=self._turn,
            keep_context=keep_context,
            generation_cancelled=cancelled,
        )
        await self._emit_status(PipelineStatus.INTERRUPTED, detail=reason)
        return dropped

    async def _emit_status(self, state: PipelineStatus, detail: str | None = None) -> None:
        """Queue a lifecycle status frame."""
        await self.output_queue.put(
            PipelineItem(
                message=build_server_message(
                    ServerMessageType.STATUS,
                    self.session_id,
                    status=StatusPayload(state=state, detail=detail),
                ),
            ),
        )

    async def _emit_avatar(self, action: AvatarAction, intensity: float = 1.0) -> None:
        """Queue an avatar animation cue."""
        await self.output_queue.put(
            PipelineItem(
                message=build_server_message(
                    ServerMessageType.AVATAR_ACTION,
                    self.session_id,
                    avatar={"action": action, "intensity": intensity},
                ),
            ),
        )

    async def _emit_error(self, code: str, message: str, *, fatal: bool = False) -> None:
        """Queue an error frame."""
        await self.output_queue.put(
            PipelineItem(
                message=build_server_message(
                    ServerMessageType.ERROR,
                    self.session_id,
                    error=ErrorPayload(code=code, message=message, fatal=fatal),
                ),
            ),
        )

    # ---- inbound submission (called by the receiver task) ------------------

    async def submit(self, item: InboundItem) -> None:
        """Enqueue a validated client message for processing."""
        await self.input_queue.put(item)
        self._log.info("pipeline.submitted", kind=item.message.type)

    async def submit_audio(self, message: ClientMessage, audio: bytes) -> None:
        """Enqueue an audio turn, validating the frame size first.

        Args:
            message: The validated ``audio`` control frame.
            audio: Raw PCM bytes carried by the binary frame.

        Raises:
            ValueError: If ``audio`` exceeds :data:`MAX_AUDIO_FRAME_BYTES`.
        """
        if len(audio) > MAX_AUDIO_FRAME_BYTES:
            msg = (
                f"audio frame of {len(audio)} bytes exceeds the {MAX_AUDIO_FRAME_BYTES} byte limit"
            )
            raise ValueError(msg)
        await self.submit(InboundItem(message=message, audio=audio))

    # ---- mock AI stages ----------------------------------------------------

    async def _mock_asr(self, audio: bytes) -> str:
        """Simulate speech recognition of one audio frame.

        Args:
            audio: Raw PCM bytes received from the client.

        Returns:
            A transcript derived from the frame length, so different inputs
            produce different prompts.
        """
        await asyncio.sleep(ASR_LATENCY)
        self._log.info("pipeline.asr_completed", audio_bytes=len(audio))
        return f"模拟语音输入({len(audio)} 字节)"

    async def _mock_llm_stream(self, prompt: str) -> AsyncIterator[str]:
        """Yield the assistant reply one token at a time.

        Mirrors the cadence of a real streaming model, and makes the surrounding
        task cancellable between tokens.

        Args:
            prompt: Recognised user text; unused by the mock.
        """
        del prompt  # a real implementation would condition generation on it
        for token in MOCK_REPLY:
            await asyncio.sleep(LLM_TOKEN_INTERVAL)
            yield token

    async def _mock_tts_stream(self, text: str) -> AsyncIterator[bytes]:
        """Yield synthesized speech as fixed-size chunks.

        The payload is filler rather than real audio: during infrastructure
        bring-up the chunk cadence is what the sender and client depend on.

        Args:
            text: The reply being spoken, which sets the chunk count.
        """
        chunk_count = max(1, -(-len(text) // TTS_CHUNK_CHARS))
        for _ in range(chunk_count):
            await asyncio.sleep(TTS_CHUNK_INTERVAL)
            yield bytes(TTS_CHUNK_BYTES)

    # ---- processor ---------------------------------------------------------

    async def process_stream(
        self,
        session_id: str,
        input_queue: asyncio.Queue[InboundItem | object],
        output_queue: asyncio.Queue[PipelineItem | object],
    ) -> None:
        """Run the ASR -> LLM -> TTS loop until shutdown or cancellation.

        One client message is handled per iteration, so an interrupt frame
        waits behind at most the turn in flight, never behind queued audio.

        Args:
            session_id: Session correlation id, echoed on emitted frames.
            input_queue: Inbound items produced by the receiver task.
            output_queue: Outbound items drained by the sender task.
        """
        self._log.info("pipeline.processing_started")
        try:
            while True:
                raw = await input_queue.get()
                try:
                    if raw is SHUTDOWN:
                        self._log.info("pipeline.shutdown_signal")
                        return
                    if not isinstance(raw, InboundItem):
                        self._log.warning("pipeline.unexpected_item", item_type=type(raw).__name__)
                        continue
                    await self._process_turn(session_id, raw, output_queue)
                finally:
                    input_queue.task_done()
        except asyncio.CancelledError:
            self._log.info("pipeline.processing_cancelled")
            raise
        except Exception:
            self._log.exception("pipeline.processing_failed")
            raise
        finally:
            self._log.info("pipeline.processing_stopped")

    async def _process_turn(
        self,
        session_id: str,
        item: InboundItem,
        output_queue: asyncio.Queue[PipelineItem | object],
    ) -> None:
        """Handle a single client message end to end.

        Args:
            session_id: Session correlation id for emitted frames.
            item: The validated client message and its optional audio.
            output_queue: Queue drained by the sender task.
        """
        message = item.message
        self._interrupted = False
        self._turn += 1

        if isinstance(message, ClientInterruptMessage):
            # Executed inside the processor, so cancelling here would target
            # the running task; only the queues need clearing.
            await self.handle_interrupt(message, cancel_active=False)
            return

        if isinstance(message, ClientTextMessage):
            prompt = message.text.text
        else:
            prompt = await self._mock_asr(item.audio or b"")

        self._log.info("pipeline.turn_started", turn=self._turn, prompt_chars=len(prompt))
        await self._emit_status(PipelineStatus.THINKING)
        await self._emit_avatar(AvatarAction.THINK)

        sequence = 0
        parts: list[str] = []
        async for token in self._mock_llm_stream(prompt):
            parts.append(token)
            await output_queue.put(
                PipelineItem(
                    message=build_server_message(
                        ServerMessageType.TEXT_DELTA,
                        session_id,
                        sequence=sequence,
                        text=TextPayload(text=token, final=False),
                    ),
                ),
            )
            sequence += 1

        full_reply = "".join(parts)
        await output_queue.put(
            PipelineItem(
                message=build_server_message(
                    ServerMessageType.TEXT_DELTA,
                    session_id,
                    sequence=sequence,
                    text=TextPayload(text=full_reply, final=True),
                ),
            ),
        )

        await self._emit_status(PipelineStatus.SPEAKING)
        await self._emit_avatar(AvatarAction.SPEAK)

        audio_sequence = 0
        async for chunk in self._mock_tts_stream(full_reply):
            await output_queue.put(
                PipelineItem(
                    message=build_server_message(
                        ServerMessageType.AUDIO_CHUNK,
                        session_id,
                        sequence=audio_sequence,
                        audio=AudioChunkPayload(
                            sample_rate=24_000,
                            channels=1,
                            sequence=audio_sequence,
                            final=False,
                        ),
                    ),
                    audio=chunk,
                ),
            )
            audio_sequence += 1

        self._log.info("pipeline.turn_completed", turn=self._turn, deltas=sequence)

    async def fail(self, code: str, message: str) -> None:
        """Queue a terminal error frame, used when a turn cannot be processed."""
        self._log.error("pipeline.turn_failed", code=code)
        await self._emit_error(code, message, fatal=True)
