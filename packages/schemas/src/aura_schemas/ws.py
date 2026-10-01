"""WebSocket wire contract for the virtual-human streaming session.

Every frame exchanged over ``/ws/virtual-human/{session_id}`` is described by a
Pydantic V2 model here, so both ends of the socket validate identically and the
gateway can never construct a message the client cannot parse.

Text frames use a discriminated union on ``type``: exactly one payload variant
may be set, and the matching payload is required. Binary frames carry raw audio
only; their meaning is fixed by the frame that preceded them.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "SERVER_MESSAGE_TYPES",
    "AudioChunkPayload",
    "AudioPayload",
    "AvatarAction",
    "AvatarActionPayload",
    "ClientAudioMessage",
    "ClientInterruptMessage",
    "ClientMessage",
    "ClientTextMessage",
    "ErrorPayload",
    "PipelineStatus",
    "ServerAudioChunkMessage",
    "ServerAvatarActionMessage",
    "ServerErrorMessage",
    "ServerMessage",
    "ServerStatusMessage",
    "ServerTextDeltaMessage",
    "StatusPayload",
    "TextPayload",
    "build_server_message",
    "parse_client_message",
]

#: Maximum accepted size of a single binary audio frame (16 KiB). Larger frames
#: are rejected before allocation to keep a malicious client from exhausting
#: gateway memory.
MAX_AUDIO_FRAME_BYTES = 16_384


class ClientMessageType(StrEnum):
    """Frame types a client may send."""

    AUDIO = "audio"
    TEXT = "text"
    INTERRUPT = "interrupt"


class ServerMessageType(StrEnum):
    """Frame types the gateway may send."""

    TEXT_DELTA = "text_delta"
    AUDIO_CHUNK = "audio_chunk"
    AVATAR_ACTION = "avatar_action"
    STATUS = "status"
    ERROR = "error"


class PipelineStatus(StrEnum):
    """Coarse lifecycle state reported to the client."""

    IDLE = "idle"
    LISTENING = "listening"
    THINKING = "thinking"
    SPEAKING = "speaking"
    INTERRUPTED = "interrupted"
    CLOSED = "closed"


class AvatarAction(StrEnum):
    """Coarse avatar animation cues the renderer maps to motion clips."""

    IDLE = "idle"
    LISTEN = "listen"
    THINK = "think"
    SPEAK = "speak"
    NOD = "nod"
    SHAKE = "shake"


class _Payload(BaseModel):
    """Base class shared by every payload model.

    ``str_strip_whitespace`` is deliberately off: a streamed text delta may be
    a single space, and stripping it would turn a valid delta into an empty
    string that fails ``min_length``. Identifiers are constrained by pattern
    instead of by trimming.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")


#: Session identifiers are opaque but must be safe to echo and to log.
SessionId = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.:-]+$")]


class AudioPayload(_Payload):
    """Metadata accompanying a binary audio frame."""

    sample_rate: int = Field(default=16_000, ge=8_000, le=48_000)
    channels: int = Field(default=1, ge=1, le=2)


class TextPayload(_Payload):
    """User text, or a delta of generated text."""

    text: str = Field(min_length=1, max_length=4_000)
    final: bool = Field(
        default=True,
        description="False marks a partial utterance that more text will follow.",
    )


class InterruptPayload(_Payload):
    """Reason a client gave for cutting the assistant off mid-utterance."""

    reason: str = Field(default="user_speech", max_length=128)
    keep_context: bool = Field(
        default=True,
        description="False also discards the partially generated reply context.",
    )


class AudioChunkPayload(_Payload):
    """Metadata for one binary chunk of synthesized speech."""

    sample_rate: int = Field(default=24_000, ge=8_000, le=48_000)
    channels: int = Field(default=1, ge=1, le=2)
    sequence: int = Field(default=0, ge=0)
    final: bool = Field(default=False)


class AvatarActionPayload(_Payload):
    """One avatar animation cue."""

    action: AvatarAction = AvatarAction.IDLE
    intensity: float = Field(default=1.0, ge=0.0, le=1.0)


class StatusPayload(_Payload):
    """Session lifecycle notification."""

    state: PipelineStatus
    detail: str | None = Field(default=None, max_length=256)


class ErrorPayload(_Payload):
    """Machine-readable failure description."""

    code: str = Field(min_length=1, max_length=64)
    message: str = Field(min_length=1, max_length=1_000)
    fatal: bool = Field(default=False)


class _ClientMessageBase(_Payload):
    """Fields common to every client frame."""

    session_id: SessionId
    request_id: str | None = Field(default=None, max_length=64)
    sequence: int = Field(default=0, ge=0)


class ClientAudioMessage(_ClientMessageBase):
    """Declares that the next binary frame carries microphone audio."""

    type: Literal[ClientMessageType.AUDIO] = ClientMessageType.AUDIO
    audio: AudioPayload = AudioPayload()


class ClientTextMessage(_ClientMessageBase):
    """A typed utterance, used when speech recognition runs on the client."""

    type: Literal[ClientMessageType.TEXT] = ClientMessageType.TEXT
    text: TextPayload


class ClientInterruptMessage(_ClientMessageBase):
    """Barge-in signal: drop queued output and cancel in-flight generation."""

    type: Literal[ClientMessageType.INTERRUPT] = ClientMessageType.INTERRUPT
    interrupt: InterruptPayload = InterruptPayload()


ClientMessage = Annotated[
    ClientAudioMessage | ClientTextMessage | ClientInterruptMessage,
    Field(discriminator="type"),
]


class _ServerMessageBase(_Payload):
    """Fields common to every server frame."""

    session_id: SessionId
    request_id: str | None = Field(default=None, max_length=64)
    sequence: int = Field(default=0, ge=0)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ServerTextDeltaMessage(_ServerMessageBase):
    """One streamed fragment of the assistant reply."""

    type: Literal[ServerMessageType.TEXT_DELTA] = ServerMessageType.TEXT_DELTA
    text: TextPayload


class ServerAudioChunkMessage(_ServerMessageBase):
    """Metadata for the binary speech frame that follows."""

    type: Literal[ServerMessageType.AUDIO_CHUNK] = ServerMessageType.AUDIO_CHUNK
    audio: AudioChunkPayload


class ServerAvatarActionMessage(_ServerMessageBase):
    """An avatar animation cue."""

    type: Literal[ServerMessageType.AVATAR_ACTION] = ServerMessageType.AVATAR_ACTION
    avatar: AvatarActionPayload


class ServerStatusMessage(_ServerMessageBase):
    """A session lifecycle transition."""

    type: Literal[ServerMessageType.STATUS] = ServerMessageType.STATUS
    status: StatusPayload


class ServerErrorMessage(_ServerMessageBase):
    """A failure the client should surface to the end user."""

    type: Literal[ServerMessageType.ERROR] = ServerMessageType.ERROR
    error: ErrorPayload


ServerMessage = Annotated[
    ServerTextDeltaMessage
    | ServerAudioChunkMessage
    | ServerAvatarActionMessage
    | ServerStatusMessage
    | ServerErrorMessage,
    Field(discriminator="type"),
]

#: Concrete model for each ``ServerMessage`` variant, keyed by its ``type``.
SERVER_MESSAGE_TYPES: dict[ServerMessageType, type[_ServerMessageBase]] = {
    ServerMessageType.TEXT_DELTA: ServerTextDeltaMessage,
    ServerMessageType.AUDIO_CHUNK: ServerAudioChunkMessage,
    ServerMessageType.AVATAR_ACTION: ServerAvatarActionMessage,
    ServerMessageType.STATUS: ServerStatusMessage,
    ServerMessageType.ERROR: ServerErrorMessage,
}


def parse_client_message(
    raw: object,
) -> ClientAudioMessage | ClientTextMessage | ClientInterruptMessage:
    """Validate one inbound JSON frame into a concrete client message.

    Beyond the per-model ``extra="forbid"`` check, this rejects a frame that
    carries a payload belonging to a different ``type``: a ``text`` frame with a
    stray ``audio`` key would otherwise be silently accepted and dropped.

    Args:
        raw: The decoded JSON object, typically the result of
            :meth:`websockets.WebSocket.receive_json`.

    Returns:
        The matching client message model instance.

    Raises:
        ValueError: If ``raw`` is not an object, the ``type`` discriminator is
            unknown, or the payload fails validation.
    """
    from pydantic import TypeAdapter

    if isinstance(raw, Mapping):
        message_type = raw.get("type")
        expected = {
            ClientMessageType.AUDIO: "audio",
            ClientMessageType.TEXT: "text",
            ClientMessageType.INTERRUPT: "interrupt",
        }.get(message_type)
        if expected is not None:
            mismatched = sorted({"audio", "text", "interrupt"} - {expected} & raw.keys())
            if mismatched:
                msg = f"frame type {message_type!r} must not carry payload(s) {mismatched}"
                raise ValueError(msg)
    return TypeAdapter(ClientMessage).validate_python(raw)


def build_server_message(
    message_type: ServerMessageType,
    session_id: SessionId,
    **payload: object,
) -> _ServerMessageBase:
    """Construct and validate a server frame of the requested ``type``.

    Args:
        message_type: Discriminator selecting the payload model.
        session_id: Correlation id of the session that produced the frame.
        **payload: Fields of the selected payload model.

    Returns:
        A validated, frozen server message ready for ``send_json``.

    Raises:
        ValueError: If the payload does not satisfy the selected model.
    """
    model = SERVER_MESSAGE_TYPES[message_type]
    return model(session_id=session_id, type=message_type, **payload)
