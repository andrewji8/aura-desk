"""Real LLM / TTS streaming pipeline.

The pipeline is a bounded-buffer producer/consumer stage::

    Receiver (websocket) -> input_queue -> process_stream -> output_queue -> Sender (websocket)

Running the stages as separate tasks decouples socket reads from model latency:
a slow client cannot stall generation, and a fast generator cannot block the
receive loop.

Text generation is a real call to an OpenAI-compatible endpoint
(:meth:`StreamingPipeline._real_llm_stream`) driven by ``httpx.AsyncClient``
streaming SSE. Every ``await`` is a cancellation point, which is what makes
barge-in fast: :meth:`StreamingPipeline.handle_interrupt` drains the output queue
and cancels the processing task, so the in-flight HTTP stream is torn down at its
next suspension point instead of being awaited to completion.

ASR and TTS are still simulated; only the text stage talks to a real model.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
import json
from typing import TYPE_CHECKING, Any, Final

import edge_tts
import httpx

from aura_gateway.core.config import Settings, get_settings
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

__all__ = [
    "InboundItem",
    "LLMError",
    "LLMProtocolError",
    "LLMRateLimitError",
    "LLMTimeoutError",
    "LLMUpstreamError",
    "PipelineItem",
    "StreamingPipeline",
]

#: Sentinel pushed onto ``input_queue`` to ask the processor to return cleanly.
SHUTDOWN: Final = object()

#: Mock ASR latency in seconds. Deliberately non-zero so cancellation
#: remains observable while the TTS stage is real.
ASR_LATENCY: Final = 0.02

#: SSE framing constants for the OpenAI-compatible streaming protocol.
_SSE_DATA_PREFIX: Final = "data:"
_SSE_DONE_MARKER: Final = "[DONE]"

#: Upstream status codes worth another attempt: rate limiting and transient
#: server-side failures.
_RETRYABLE_STATUS: Final = frozenset({408, 409, 425, 429, 500, 502, 503, 504})

#: Default chat history handed to the model when no conversation is tracked yet.
_SYSTEM_ROLE: Final = "system"
_USER_ROLE: Final = "user"
_ASSISTANT_ROLE: Final = "assistant"


class LLMError(Exception):
    """Base class for upstream LLM failures that must reach the client.

    Raising this instead of letting ``httpx`` errors escape keeps the pipeline
    from crashing while guaranteeing the client is told what happened.
    """

    def __init__(self, message: str, *, code: str = "llm_error", retryable: bool = False) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.retryable = retryable


class LLMTimeoutError(LLMError):
    """The upstream did not answer within the configured timeout."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="llm_timeout", retryable=True)


class LLMRateLimitError(LLMError):
    """The upstream rejected the request with HTTP 429."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="llm_rate_limited", retryable=True)


class LLMUpstreamError(LLMError):
    """The upstream returned an unexpected non-success status code."""

    def __init__(self, message: str, *, status_code: int, retryable: bool) -> None:
        super().__init__(message, code=f"llm_upstream_{status_code}", retryable=retryable)
        self.status_code = status_code


class LLMProtocolError(LLMError):
    """The upstream sent a frame that does not match the streaming protocol."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="llm_protocol_error")


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
    shared between sessions. The HTTP client may be shared across sessions: an
    ``httpx.AsyncClient`` is safe to use concurrently and connection pooling is
    only effective when the client outlives a single request.
    """

    def __init__(
        self,
        session_id: str,
        *,
        client: httpx.AsyncClient | None = None,
        settings: Settings | None = None,
        input_queue_size: int = 32,
        output_queue_size: int = 64,
    ) -> None:
        """Create the queues for a session.

        Args:
            session_id: Correlation id attached to every log line and frame.
            client: Shared ``httpx.AsyncClient``. When omitted, the process-wide
                client from the lifespan is used; injecting one keeps tests off
                the network.
            settings: Configuration override, mainly for tests.
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
        self._settings = settings or get_settings()
        self._client = client
        self._task: asyncio.Task[None] | None = None
        self._log = get_logger("aura_gateway.pipeline").bind(session_id=session_id)
        self._interrupted = False
        self._turn = 0
        self._history: list[dict[str, str]] = []

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
        early as possible, and only then is generation cancelled. The
        in-flight HTTP request is aborted by cancelling its task, which closes
        the response stream and releases the connection.

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
        if not keep_context:
            self._history.clear()
            self._log.info("pipeline.context_cleared")
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

    # ---- mock ASR / TTS stages ---------------------------------------------

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

    async def _real_tts_stream(self, text: str) -> AsyncIterator[bytes]:
        """Stream synthesized speech chunks from Edge TTS.

        Uses :meth:`edge_tts.Communicate.stream` to generate real MP3 audio
        chunks asynchronously, yielding each chunk as it becomes available
        rather than waiting for the entire utterance to be synthesized.

        Args:
            text: The assistant reply being spoken.

        Yields:
            Raw MP3 audio bytes produced by Edge TTS for each streamed chunk.
        """
        communicate = edge_tts.Communicate(
            text=text,
            voice=self._settings.TTS_VOICE,
        )
        async for chunk in communicate.stream():
            if chunk.get("type") == "audio":
                yield chunk["data"]

    # ---- real LLM stage -----------------------------------------------------

    def _build_headers(self) -> dict[str, str]:
        """Build the outbound request headers for the upstream call."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }
        api_key = self._settings.LLM_API_KEY.get_secret_value()
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    def _build_request_body(self, prompt: str) -> dict[str, Any]:
        """Build the OpenAI-compatible chat completions request body."""
        settings = self._settings
        messages: list[dict[str, str]] = [
            {"role": _SYSTEM_ROLE, "content": settings.LLM_SYSTEM_PROMPT},
            *self._history,
            {"role": _USER_ROLE, "content": prompt},
        ]
        return {
            "model": settings.LLM_MODEL,
            "messages": messages,
            "stream": True,
            "temperature": settings.LLM_TEMPERATURE,
            "max_tokens": settings.LLM_MAX_TOKENS,
        }

    def _request_timeout(self) -> httpx.Timeout:
        """Return the connect/read timeout pair for one streaming request."""
        return httpx.Timeout(
            timeout=self._settings.LLM_TIMEOUT_READ,
            connect=self._settings.LLM_TIMEOUT_CONNECT,
        )

    def _get_client(self) -> httpx.AsyncClient:
        """Return the HTTP client used for generation.

        Raises:
            LLMError: If no client was injected and the application lifespan has
                not installed the shared one yet.
        """
        if self._client is None:
            from aura_gateway.core.llm import get_llm_client

            self._client = get_llm_client()
        return self._client

    @staticmethod
    def _error_message_from_payload(payload: object) -> str | None:
        """Extract the upstream's human-readable error message, if present."""
        if not isinstance(payload, dict):
            return None
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if isinstance(message, str) and message:
                return message[:500]
        return None

    @staticmethod
    def _raise_for_status(status_code: int, body: str) -> None:
        """Translate a non-success HTTP status into a typed :class:`LLMError`.

        Args:
            status_code: Status returned by the upstream.
            body: Raw response body, parsed for a provider error message.

        Raises:
            LLMRateLimitError: On HTTP 429.
            LLMUpstreamError: On any other non-success status.
        """
        detail = (
            StreamingPipeline._error_message_from_payload(
                _try_json(body),
            )
            or f"upstream returned HTTP {status_code}"
        )
        if status_code == 429:
            raise LLMRateLimitError(detail)
        raise LLMUpstreamError(
            detail,
            status_code=status_code,
            retryable=status_code in _RETRYABLE_STATUS,
        )

    @staticmethod
    def _extract_delta(frame: str) -> str | None:
        """Return the text delta carried by one SSE ``data:`` payload.

        Args:
            frame: The raw payload following ``data:``.

        Returns:
            The incremental text, an empty string for a keep-alive chunk, or
            ``None`` when the frame carries no ``choices`` array (for example a
            final usage-only frame).

        Raises:
            LLMProtocolError: If the payload is not a JSON object, or its
                ``choices`` entry cannot be interpreted.
        """
        payload = _try_json(frame)
        if not isinstance(payload, dict):
            msg = f"expected a JSON object in SSE data, got {frame[:200]!r}"
            raise LLMProtocolError(msg)

        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            return None
        first = choices[0]
        if not isinstance(first, dict):
            msg = f"expected a choice object, got {type(first).__name__}"
            raise LLMProtocolError(msg)

        delta = first.get("delta")
        if not isinstance(delta, dict):
            return None
        content = delta.get("content")
        if content is None:
            return None
        if not isinstance(content, str):
            msg = f"delta content must be a string, got {type(content).__name__}"
            raise LLMProtocolError(msg)
        return content

    async def _real_llm_stream(self, prompt: str) -> AsyncIterator[str]:
        """Stream the assistant reply from an OpenAI-compatible endpoint.

        Issues ``POST {LLM_BASE_URL}/chat/completions`` with ``stream: true`` and
        parses the Server-Sent Events response, yielding each text delta as it
        arrives so the client sees tokens as early as the upstream produces
        them.

        The request is retried on rate limits and transient upstream errors, but
        *only* while no delta has been emitted yet; retrying mid-stream would
        duplicate text the client already received.

        Args:
            prompt: The recognised or typed user utterance.

        Yields:
            Successive text deltas, possibly empty strings for keep-alive frames.

        Raises:
            LLMTimeoutError: The upstream did not respond in time.
            LLMRateLimitError: The upstream rate-limited every attempt.
            LLMUpstreamError: The upstream returned an unexpected status.
            LLMProtocolError: The upstream sent a malformed SSE frame.
        """
        settings = self._settings
        headers = self._build_headers()
        body = self._build_request_body(prompt)
        url = settings.llm_chat_completions_url
        last_error: LLMError | None = None

        for attempt in range(settings.LLM_MAX_RETRIES + 1):
            emitted = False
            try:
                async for delta in self._stream_once(url, headers, body):
                    emitted = True
                    yield delta
                return
            except httpx.TimeoutException as exc:
                last_error = LLMTimeoutError(f"LLM request timed out: {exc.__class__.__name__}")
            except httpx.HTTPError as exc:
                last_error = LLMUpstreamError(
                    f"LLM transport error: {exc.__class__.__name__}",
                    status_code=0,
                    retryable=True,
                )
            except LLMError as exc:
                last_error = exc

            self._log.warning(
                "pipeline.llm_attempt_failed",
                attempt=attempt + 1,
                code=last_error.code,
                error=last_error.message,
                emitted=emitted,
            )
            # A failure after tokens were emitted cannot be retried safely.
            if emitted or not last_error.retryable or attempt >= settings.LLM_MAX_RETRIES:
                raise last_error
            await asyncio.sleep(settings.LLM_RETRY_BACKOFF * (2**attempt))

    async def _stream_once(
        self,
        url: str,
        headers: dict[str, str],
        body: dict[str, Any],
    ) -> AsyncIterator[str]:
        """Perform one streaming request and yield its text deltas.

        Args:
            url: Absolute chat completions URL.
            headers: Request headers including authorization.
            body: JSON request body.

        Yields:
            Text deltas decoded from the SSE stream.

        Raises:
            LLMRateLimitError: On HTTP 429.
            LLMUpstreamError: On any other non-success status.
            LLMProtocolError: On a malformed SSE frame.
            httpx.TimeoutException: On connect/read timeout.
        """
        client = self._get_client()
        self._log.info(
            "pipeline.llm_request_started",
            model=self._settings.LLM_MODEL,
            base_url=self._settings.LLM_BASE_URL,
        )

        async with client.stream(
            "POST",
            url,
            headers=headers,
            json=body,
            timeout=self._request_timeout(),
        ) as response:
            if response.status_code != 200:
                raw = await response.aread()
                self._raise_for_status(response.status_code, raw.decode("utf-8", "replace"))

            delta_count = 0
            async for line in response.aiter_lines():
                if not line or line.startswith(":"):
                    continue  # keep-alive / SSE comment
                if not line.startswith(_SSE_DATA_PREFIX):
                    continue
                frame = line[len(_SSE_DATA_PREFIX) :].strip()
                if not frame:
                    continue
                if frame == _SSE_DONE_MARKER:
                    self._log.info("pipeline.llm_stream_done", deltas=delta_count)
                    return

                delta = self._extract_delta(frame)
                if delta:
                    delta_count += 1
                    yield delta

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

        An upstream failure is reported to the client as an error frame and ends
        the turn normally, so the session survives a rate limit or a bad frame
        without the processor task dying.

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
        try:
            async for delta in self._real_llm_stream(prompt):
                parts.append(delta)
                await output_queue.put(
                    PipelineItem(
                        message=build_server_message(
                            ServerMessageType.TEXT_DELTA,
                            session_id,
                            sequence=sequence,
                            text=TextPayload(text=delta, final=False),
                        ),
                    ),
                )
                sequence += 1
        except LLMError as exc:
            # Graceful degradation: tell the client, then end the turn. The
            # processor stays alive for the next message.
            self._log.warning(
                "pipeline.llm_failed",
                turn=self._turn,
                code=exc.code,
                error=exc.message,
            )
            await self._emit_error(exc.code, exc.message, fatal=False)
            await self._emit_status(PipelineStatus.LISTENING, detail="llm_error")
            return

        full_reply = "".join(parts)
        if not full_reply:
            await self._emit_error("llm_empty_response", "LLM returned no content", fatal=False)
            return

        self._history.extend(
            (
                {"role": _USER_ROLE, "content": prompt},
                {"role": _ASSISTANT_ROLE, "content": full_reply},
            ),
        )
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
        # 关键修复 1: 先收集所有 chunk, 以便我们知道哪一个是最后一个
        tts_chunks = []
        async for chunk in self._real_tts_stream(full_reply):
            tts_chunks.append(chunk)  # noqa: PERF401

        # 关键修复 2: 遍历收集到的 chunks, 动态设置 final 标记
        for idx, chunk in enumerate(tts_chunks):
            is_final = idx == len(tts_chunks) - 1  # 最后一个 chunk 设为 True

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
                            final=is_final,  # <--- 必须动态设置
                        ),
                    ),
                    audio=chunk,
                ),
            )
            audio_sequence += 1  # noqa: SIM113

        self._log.info("pipeline.turn_completed", turn=self._turn, deltas=sequence)

    async def fail(self, code: str, message: str) -> None:
        """Queue a terminal error frame, used when a turn cannot be processed."""
        self._log.error("pipeline.turn_failed", code=code)
        await self._emit_error(code, message, fatal=True)


def _try_json(raw: str) -> Any:
    """Decode JSON, returning ``None`` instead of raising on malformed input.

    Used on error bodies and SSE frames, where a non-JSON payload is a normal
    occurrence to be reported rather than a crash.

    Args:
        raw: Candidate JSON text.

    Returns:
        The decoded value, or ``None`` when the text is not valid JSON.
    """
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None
