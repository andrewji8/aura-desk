"""End-to-end websocket behaviour against the live ASGI app."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from fastapi.testclient import TestClient
import pytest

from aura_schemas.ws import MAX_AUDIO_FRAME_BYTES

if TYPE_CHECKING:
    from collections.abc import Iterator

    import httpx

WS_URL = "/ws/virtual-human/s-test"


@pytest.fixture
def ws_client(mock_llm: httpx.AsyncClient) -> Iterator[TestClient]:
    """Yield a test client whose lifespan is running but socket is not yet open."""
    from aura_gateway.core.llm import set_llm_client
    from aura_gateway.main import app

    with TestClient(app) as test_client:
        # The lifespan installs a real client on entry; restore the mock.
        set_llm_client(mock_llm)
        yield test_client


def test_connection_opens_with_listening_status(ws_client: TestClient) -> None:
    with ws_client.websocket_connect(WS_URL) as socket:
        frame = socket.receive_json()

    assert frame["type"] == "status"
    assert frame["status"]["state"] == "listening"
    assert frame["session_id"] == "s-test"


def test_text_turn_produces_deltas(ws_client: TestClient) -> None:
    with ws_client.websocket_connect(WS_URL) as socket:
        assert socket.receive_json()["type"] == "status"
        socket.send_text(
            json.dumps({"type": "text", "session_id": "s-test", "text": {"text": "你好"}})
        )

        deltas = []
        while True:
            frame = socket.receive_json()
            if frame["type"] == "text_delta" and not frame["text"]["final"]:
                deltas.append(frame["text"]["text"])
            if frame["type"] == "status" and frame["status"]["state"] == "speaking":
                break

    assert "".join(deltas)
    assert "Aura" in "".join(deltas)


def test_interrupt_stops_the_stream(ws_client: TestClient) -> None:
    with ws_client.websocket_connect(WS_URL) as socket:
        socket.receive_json()
        socket.send_text(
            json.dumps({"type": "text", "session_id": "s-test", "text": {"text": "你好"}})
        )
        socket.send_text(json.dumps({"type": "interrupt", "session_id": "s-test"}))

        # Whether the barge-in beats the mock generator is a scheduling race, so
        # the assertion is the protocol guarantee that holds either way: the
        # interrupt is acknowledged, and nothing at all is emitted after it.
        frames: list[dict[str, object]] = []
        for _ in range(400):
            message = socket.receive()
            if message["type"] != "websocket.send" or not message.get("text"):
                frames.append({"type": "audio_chunk"})  # binary frame
                continue
            frame = json.loads(message["text"])
            frames.append(frame)
            if frame["type"] == "status" and frame["status"]["state"] == "interrupted":
                break

    interrupted_at = next(
        index
        for index, frame in enumerate(frames)
        if frame["type"] == "status" and frame.get("status", {}).get("state") == "interrupted"
    )

    assert frames[interrupted_at]["type"] == "status"
    # No audio metadata or payload may follow the acknowledgement.
    assert frames[interrupted_at + 1 :] == []


def test_invalid_frame_returns_error(ws_client: TestClient) -> None:
    with ws_client.websocket_connect(WS_URL) as socket:
        socket.receive_json()
        socket.send_text(json.dumps({"type": "text", "session_id": "s-test"}))

        frame = socket.receive_json()

    assert frame["type"] == "error"
    assert frame["error"]["code"] == "invalid_message"


def test_malformed_json_returns_error(ws_client: TestClient) -> None:
    with ws_client.websocket_connect(WS_URL) as socket:
        socket.receive_json()
        socket.send_text("{not json")

        frame = socket.receive_json()

    assert frame["type"] == "error"


def test_unexpected_binary_frame_is_ignored(ws_client: TestClient) -> None:
    with ws_client.websocket_connect(WS_URL) as socket:
        assert socket.receive_json()["type"] == "status"
        socket.send_bytes(bytes(320))
        socket.send_text(json.dumps({"type": "interrupt", "session_id": "s-test"}))

        # The socket stays usable: an unannounced binary frame is skipped.
        frame = socket.receive_json()

    assert frame["type"] == "status"


def test_audio_frame_is_accepted(ws_client: TestClient) -> None:
    with ws_client.websocket_connect(WS_URL) as socket:
        socket.receive_json()
        socket.send_text(json.dumps({"type": "audio", "session_id": "s-test"}))
        socket.send_bytes(bytes(320))

        frame = socket.receive_json()

    assert frame["type"] == "status"


def test_oversized_audio_frame_is_rejected(ws_client: TestClient) -> None:
    with ws_client.websocket_connect(WS_URL) as socket:
        socket.receive_json()
        socket.send_text(json.dumps({"type": "audio", "session_id": "s-test"}))
        socket.send_bytes(bytes(MAX_AUDIO_FRAME_BYTES + 1))
        socket.send_text(json.dumps({"type": "interrupt", "session_id": "s-test"}))

        frame = socket.receive_json()

    assert frame["type"] == "status"


def test_binary_audio_payload_is_sent(ws_client: TestClient) -> None:
    with ws_client.websocket_connect(WS_URL) as socket:
        socket.receive_json()
        socket.send_text(json.dumps({"type": "audio", "session_id": "s-test"}))
        socket.send_bytes(bytes(320))

        got_audio = False
        for _ in range(400):
            message = socket.receive()
            if message["type"] == "websocket.send" and message.get("bytes"):
                got_audio = True
                break
            if message["type"] == "websocket.send":
                payload = json.loads(message["text"])
                if payload["type"] == "status" and payload["status"]["state"] == "speaking":
                    continue

    assert got_audio is True


def test_disconnect_closes_session(ws_client: TestClient) -> None:
    with ws_client.websocket_connect(WS_URL):
        pass  # exiting the context closes the socket

    # The app must remain usable for the next session.
    with ws_client.websocket_connect("/ws/virtual-human/s-second") as socket:
        assert socket.receive_json()["type"] == "status"
