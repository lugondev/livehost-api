"""A stand-in for WS /v1/conversation/stream.

It implements exactly the subset of the real socket that the plugin depends
on. If the real gateway and this fake ever drift apart, the contract has been
broken silently -- which is why the gateway keeps a matching test asserting its
own socket still satisfies the same script.

Control messages in : {"type": "text"|"abort"|"reset"|"new_session"|"flush"|"end"}
Audio frames in     : binary
Events out          : session_started, speech_start, speech_end, processing,
                       user_transcript, response_text, audio_start, audio_end,
                       turn_done, aborted, command, engines_ready, error,
                       reset, warning, done
Audio out           : binary
"""

import json

from fastapi import FastAPI, WebSocket, WebSocketDisconnect


def _bearer_from_subprotocols(websocket: WebSocket) -> str | None:
    """Mirrors the gateway's own auth_guard._bearer_from_subprotocols exactly:
    the credential rides as the element right after "bearer" in the
    Sec-WebSocket-Protocol offer list, never a query param. Found live: this
    fake used to accept() unconditionally and read only query_params, so it
    could not have caught Bug 5 (token sent as `?token=`, and even transported
    correctly would have failed the gateway's real salt check) -- three of
    five live bugs escaped review specifically because this "executable spec"
    never modeled the real gateway's auth mechanism at all."""
    protocols = websocket.scope.get("subprotocols") or []
    for index, proto in enumerate(protocols):
        if proto == "bearer" and index + 1 < len(protocols):
            return protocols[index + 1]
    return None


def build_fake_gateway() -> tuple[FastAPI, dict]:
    """Return (app, log). `log` records everything the plugin sent upstream."""
    app = FastAPI()
    log = {"audio": [], "control": [], "query": {}, "session_token": None}

    @app.websocket("/v1/conversation/stream")
    async def stream(websocket: WebSocket):
        # Real gateway behavior (conversation.py's conversation_stream):
        # reject *before* accept() when no bearer subprotocol is offered --
        # Starlette turns a pre-accept close into an HTTP-level rejection at
        # the handshake, exactly the 403 Bug 5 was diagnosed from in
        # /tmp/livehost-smoke/plugin.log. A `?token=` query param is never
        # read here, on purpose: that transport was the first half of Bug 5.
        bearer = _bearer_from_subprotocols(websocket)
        if not bearer:
            await websocket.close(code=4401, reason="unauthorized")
            return
        log["session_token"] = bearer
        await websocket.accept(subprotocol="bearer")
        log["query"] = dict(websocket.query_params)
        await websocket.send_json(
            {
                "event": "session_started",
                "session_id": "sess-1",
                "profile": websocket.query_params.get("profile"),
                "stt_engine": "fake-stt",
                "tts_engine": "fake-tts",
                "responder": "fake",
                "sample_rate": 16000,
                "audio_codec": "pcm16",
                "output": ["audio", "text"],
                "audio_out": "wav",
                "output_sample_rate": None,
                "stt_ready": True,
                "tts_ready": True,
            }
        )
        try:
            while True:
                message = await websocket.receive()
                if message.get("type") == "websocket.disconnect":
                    break
                if message.get("bytes") is not None:
                    log["audio"].append(message["bytes"])
                    # The real socket answers audio with the endpointer's
                    # verdict, then a turn. Livehost's arbitration reads
                    # exactly these two events -- speech_start sets
                    # voice_active, turn_done clears it -- so a fake that
                    # stays silent here specifies away the primary path.
                    await websocket.send_json({"event": "speech_start"})
                    await websocket.send_json({"event": "speech_end"})
                    await websocket.send_json(
                        {
                            "event": "user_transcript",
                            "turn": 1,
                            "text": "hello",
                            "engine": "fake-stt",
                        }
                    )
                    await websocket.send_json({"event": "turn_done", "turn": 1})
                    continue
                if message.get("text") is not None:
                    control = json.loads(message["text"])
                    log["control"].append(control)
                    if control.get("type") == "text":
                        # A turn, condensed: the events the plugin actually keys on.
                        await websocket.send_json({"event": "processing", "turn": 1})
                        await websocket.send_json(
                            {"event": "response_text", "text": f"re: {control['text']}"}
                        )
                        await websocket.send_json({"event": "audio_start"})
                        await websocket.send_bytes(b"AUDIO")
                        await websocket.send_json({"event": "audio_end"})
                        await websocket.send_json({"event": "turn_done", "turn": 1})
                    elif control.get("type") == "abort":
                        await websocket.send_json({"event": "aborted", "reason": "user"})
                    elif control.get("type") == "end":
                        await websocket.send_json({"event": "done"})
                        break
        except WebSocketDisconnect:
            pass

    return app, log
