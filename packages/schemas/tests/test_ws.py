"""Validation of the websocket wire contract."""

from __future__ import annotations

from pydantic import TypeAdapter
import pytest

from aura_schemas.ws import (
    MAX_AUDIO_FRAME_BYTES,
    AudioChunkPayload,
    ClientAudioMessage,
    ClientInterruptMessage,
    ClientTextMessage,
    PipelineStatus,
    ServerMessage,
    ServerMessageType,
    TextPayload,
    build_server_message,
    parse_client_message,
)


def test_parse_text_message() -> None:
    message = parse_client_message(
        {"type": "text", "session_id": "s1", "text": {"text": "你好"}},
    )

    assert isinstance(message, ClientTextMessage)
    assert message.text.text == "你好"


def test_parse_interrupt_message() -> None:
    message = parse_client_message({"type": "interrupt", "session_id": "s1"})

    assert isinstance(message, ClientInterruptMessage)
    assert message.interrupt.reason == "user_speech"
    assert message.interrupt.keep_context is True


def test_parse_audio_message() -> None:
    message = parse_client_message({"type": "audio", "session_id": "s1"})

    assert isinstance(message, ClientAudioMessage)
    assert message.audio.sample_rate == 16_000


def test_unknown_type_rejected() -> None:
    with pytest.raises(ValueError):
        parse_client_message({"type": "teleport", "session_id": "s1"})


def test_missing_required_payload_rejected() -> None:
    with pytest.raises(ValueError):
        parse_client_message({"type": "text", "session_id": "s1"})


def test_mismatched_payload_rejected() -> None:
    with pytest.raises(ValueError):
        parse_client_message(
            {
                "type": "text",
                "session_id": "s1",
                "text": {"text": "hi"},
                "audio": {"sample_rate": 16_000},
            },
        )


def test_unknown_field_rejected() -> None:
    with pytest.raises(ValueError):
        parse_client_message(
            {"type": "interrupt", "session_id": "s1", "surprise": 1},
        )


def test_empty_session_id_rejected() -> None:
    with pytest.raises(ValueError):
        parse_client_message({"type": "interrupt", "session_id": ""})


def test_text_over_max_length_rejected() -> None:
    with pytest.raises(ValueError):
        ClientTextMessage(session_id="s1", text=TextPayload(text="x" * 4_001))


def test_build_text_delta_frame() -> None:
    frame = build_server_message(
        ServerMessageType.TEXT_DELTA,
        "s1",
        sequence=3,
        text=TextPayload(text="a", final=False),
    )

    assert frame.type == "text_delta"
    assert frame.sequence == 3
    assert frame.timestamp.tzinfo is not None


def test_build_status_frame() -> None:
    frame = build_server_message(
        ServerMessageType.STATUS,
        "s1",
        status={"state": PipelineStatus.SPEAKING, "detail": None},
    )

    assert frame.type == "status"
    assert frame.status.state is PipelineStatus.SPEAKING


def test_build_audio_chunk_frame_rejects_bad_rate() -> None:
    with pytest.raises(ValueError):
        build_server_message(
            ServerMessageType.AUDIO_CHUNK,
            "s1",
            audio=AudioChunkPayload(sample_rate=1),
        )


def test_server_message_is_json_serialisable() -> None:
    frame = build_server_message(
        ServerMessageType.ERROR,
        "s1",
        error={"code": "boom", "message": "failed", "fatal": True},
    )

    dumped = frame.model_dump(mode="json")

    assert isinstance(dumped, dict)
    assert dumped["error"]["code"] == "boom"


def test_max_audio_frame_size_is_sane() -> None:
    assert 1_000 < MAX_AUDIO_FRAME_BYTES <= 65_536


@pytest.mark.parametrize(
    ("message_type", "payload"),
    [
        (ServerMessageType.TEXT_DELTA, {"text": {"text": "hi", "final": False}}),
        (ServerMessageType.AUDIO_CHUNK, {"audio": {"sequence": 0}}),
        (ServerMessageType.AVATAR_ACTION, {"avatar": {"action": "speak"}}),
        (ServerMessageType.STATUS, {"status": {"state": "thinking"}}),
        (ServerMessageType.ERROR, {"error": {"code": "x", "message": "y"}}),
    ],
)
def test_every_server_type_parses_through_the_union(
    message_type: ServerMessageType,
    payload: dict[str, object],
) -> None:
    adapter: TypeAdapter[ServerMessage] = TypeAdapter(ServerMessage)

    frame = adapter.validate_python(
        {"type": message_type.value, "session_id": "s1", **payload},
    )

    assert frame.type == message_type
    assert frame.session_id == "s1"
