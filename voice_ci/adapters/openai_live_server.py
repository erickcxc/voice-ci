"""OpenAI Live API assistant server for EVA (voice-ci).

Runs a voice agent's exact Live session (gpt-live-1 style: a live voice model plus an
optional delegated Responses backend) over the Live API's WebSocket, and replays the
call-ending logic your app runs around it, driven by a JSON profile
(s2s_params["live_config"]):

  session              the session.start payload your app sends (audio forced to PCMU 8 kHz)
  greeting             sent ~0.6 s after session.started (greeting_event: instructions | commentary)
  caller_context       optional thinking.append sent with the greeting
  said_goodbye         regex: the agent has said its goodbye
  wrap_up_regex        optional extra regex that counts as wrapping up ("one moment")
  outcome_tool         the function the backend calls to record the call (default finish_call)
  tool_output          {kind: note} sent back as function_call_output ("default" fallback)
  closing_instruction  {kind: instruction} cued when the agent has not said goodbye
  cue_now_kinds        kinds whose closing line the app speaks right away (e.g. ["sales"])
  hang_up_after_goodbye  default true; false for apps that never end the call (a browser
                       demo where the visitor hangs up): no closing cues, no hang-up
  wrap_up_nudge        default true; false when the app has no nudge
  timings              optional overrides, see DEFAULT_TIMINGS

The outcome tool runs in EVA's tool executor; its result may carry a "kind" that picks
the note and closing line. Lifecycle mirrored from a production app:

- closing: cue_now kinds get their closing line at once; otherwise, if the goodbye was
  not said within closing_cue_after_s, cue it
- hang up hangup_delay_s after the goodbye, let queued audio play, then close the line
- wrap-up nudge: the agent said goodbye with nothing recorded -> response.create
  (at most nudge_max), and hang up unrecorded_hangup_s after the first nudge
- the Live API streams digital silence between sentences; only voiced chunks are
  forwarded, so EVA's caller does not think the agent is still talking
- caller audio is paced in real time with line silence in the gaps; the Live API
  rejects faster-than-real-time input and needs audio flowing for context injection
"""

import asyncio
import base64
import json
import re
import time
from pathlib import Path
from typing import Any

import numpy as np
import uvicorn
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from eva.assistant.base_server import AbstractAssistantServer
from eva.assistant.pipeline.observers import FrameworkLogWriter, MetricsLogWriter
from eva.utils.audio_utils import create_twilio_media_message, mulaw_8k_to_pcm16_24k, parse_twilio_media_message
from eva.utils.logging import get_logger

logger = get_logger(__name__)

LIVE_URL = "wss://api.openai.com/v1/live/sessions"
SAMPLE_RATE = 24000  # recording rate (EVA buffers are PCM16 24 kHz)
CHUNK = 160  # 20 ms of PCMU at 8 kHz
CHUNK_S = 0.02
VOICE_RMS = 500  # PCM16 RMS above which a user frame counts as speech
OUT_VOICED_RMS = 50  # PCM16 RMS above which an agent frame counts as voiced

DEFAULT_TIMINGS = {
    "greet_after_s": 0.6,
    "closing_cue_after_s": 4.0,
    "hangup_delay_s": 0.9,
    "cue_fallback_s": 8.0,
    "closing_fallback_s": 10.0,
    "nudge_after_s": 2.5,
    "nudge_max": 2,
    "unrecorded_hangup_s": 10.0,
}


def _wall_ms() -> int:
    return int(round(time.time() * 1000))


def _sync(buf: bytearray, target: int) -> None:
    if len(buf) < target:
        buf.extend(b"\x00" * (target - len(buf)))


class OpenAILiveAssistantServer(AbstractAssistantServer):
    _service_name = "OpenAI Live"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._audio_sample_rate = SAMPLE_RATE
        s2s = self.pipeline_config.s2s_params or {}
        self._api_key = s2s.get("api_key")
        self._cfg = json.loads(Path(s2s["live_config"]).read_text())
        self._model = self._cfg["session"]["model"]
        self._bye = re.compile(self._cfg["said_goodbye"], re.I)
        wrap = self._cfg.get("wrap_up_regex")
        self._wrap = re.compile(wrap, re.I) if wrap else None
        self._outcome_tool = self._cfg.get("outcome_tool", "finish_call")
        self._cue_now = set(self._cfg.get("cue_now_kinds", []))
        self._t = {**DEFAULT_TIMINGS, **self._cfg.get("timings", {})}
        self._ends_call = self._cfg.get("hang_up_after_goodbye", True)
        self._nudge_on = self._cfg.get("wrap_up_nudge", True)
        self._greet_type = "session.commentary.append" if self._cfg.get("greeting_event") == "commentary" else "session.instructions.append"

    async def start(self) -> None:
        if self._running:
            return
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._fw_log = FrameworkLogWriter(self.output_dir)
        self._metrics_log = MetricsLogWriter(self.output_dir)
        self._app = FastAPI()

        @self._app.websocket("/ws")
        async def ws_ep(websocket: WebSocket):
            await websocket.accept()
            await self._handle_session(websocket)

        @self._app.websocket("/")
        async def ws_root(websocket: WebSocket):
            await websocket.accept()
            await self._handle_session(websocket)

        self._server = uvicorn.Server(uvicorn.Config(self._app, host="0.0.0.0", port=self.port, log_level="warning", lifespan="off"))
        self._running = True
        self._server_task = asyncio.create_task(self._server.serve())
        while not self._server.started:
            await asyncio.sleep(0.01)
        logger.info(f"{self._service_name} server started on ws://localhost:{self.port}")

    async def _shutdown(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._server:
            self._server.should_exit = True
            if self._server_task:
                try:
                    await asyncio.wait_for(self._server_task, timeout=5.0)
                except (TimeoutError, asyncio.CancelledError):
                    self._server_task.cancel()
            self._server = None
            self._server_task = None

    # ── session ──────────────────────────────────────────────────────

    def _reset(self) -> None:
        self._stream_sid = self.conversation_id
        self._user_text = ""
        self._user_turn_ts: int | None = None
        self._last_voice_ms: int | None = None
        self._asst_text = ""
        self._asst_turn_ts: int | None = None
        self._asst_first_audio = False
        self._decided = False
        self._kind: str | None = None
        self._hung_up = False
        self._cued = False
        self._timers: list[asyncio.Task] = []
        self._in_buf = bytearray()
        self._out_q = None
        self._cue_pos = 0
        self._nudges = 0
        self._nudge_from = 0
        self._wrap_task = None

    async def _handle_session(self, websocket: WebSocket) -> None:
        self._reset()
        headers = {"Authorization": f"Bearer {self._api_key}"}
        try:
            try:
                live = await websockets.connect(LIVE_URL, additional_headers=headers, max_size=None)
            except TypeError:
                live = await websockets.connect(LIVE_URL, extra_headers=headers, max_size=None)
            self._live = live
            self._sim_ws = websocket
            await self._send({"type": "session.start", "session": self._cfg["session"]})
            out_q: asyncio.Queue[bytes] = asyncio.Queue()
            self._out_q = out_q
            tasks = [
                asyncio.create_task(self._forward_user(websocket)),
                asyncio.create_task(self._recv_live(out_q)),
                asyncio.create_task(self._pace(websocket, out_q)),
            ]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()
            for t in self._timers:
                t.cancel()
            self._flush_user()
            self._flush_asst()
            try:
                await self._send({"type": "session.close"})
                await live.close()
            except Exception:
                pass
        except Exception as e:
            logger.error(f"{self._service_name} session error: {e}", exc_info=True)

    async def _send(self, event: dict) -> None:
        try:
            await self._live.send(json.dumps(event))
        except Exception as e:
            logger.error(f"live send failed: {e}")

    # ── caller audio in ──────────────────────────────────────────────

    async def _forward_user(self, websocket: WebSocket) -> None:
        try:
            while True:
                raw = await websocket.receive_text()
                data = json.loads(raw)
                ev = data.get("event")
                if ev == "start":
                    self._stream_sid = data.get("start", {}).get("streamSid", self.conversation_id)
                    continue
                if ev == "stop":
                    break
                if ev != "media":
                    continue
                mulaw = parse_twilio_media_message(raw)
                if not mulaw:
                    continue
                pcm = mulaw_8k_to_pcm16_24k(mulaw)
                _sync(self.assistant_audio_buffer, len(self.user_audio_buffer))
                self.user_audio_buffer.extend(pcm)
                samples = np.frombuffer(pcm, dtype=np.int16).astype(np.float32)
                if samples.size and float(np.sqrt(np.mean(samples * samples))) > VOICE_RMS:
                    now = _wall_ms()
                    self._last_voice_ms = now
                    if self._user_turn_ts is None:
                        self._user_turn_ts = now
                        self._flush_asst()
                        self._fw_log.turn_start(timestamp_ms=now)
                self._in_buf.extend(mulaw)
        except WebSocketDisconnect:
            pass
        except asyncio.CancelledError:
            pass

    # ── agent audio out ──────────────────────────────────────────────

    async def _pace(self, websocket: WebSocket, q: asyncio.Queue[bytes]) -> None:
        nxt = time.monotonic()
        try:
            while True:
                chunk = await q.get()
                await websocket.send_text(create_twilio_media_message(self._stream_sid, chunk))
                now = time.monotonic()
                nxt = max(nxt, now) + CHUNK_S
                await asyncio.sleep(max(0.0, nxt - time.monotonic()))
        except (asyncio.CancelledError, Exception):
            pass

    async def _on_audio(self, b64: str, q: asyncio.Queue[bytes]) -> None:
        # The Live API keeps streaming output frames between sentences, and those
        # gaps are pure digital silence. A phone line would carry them, but EVA's
        # caller treats any frame as "assistant still speaking", so only voiced
        # 20 ms chunks are forwarded. Recording keeps the real timeline via _sync.
        if self._hung_up:
            return
        mulaw = base64.b64decode(b64)
        for i in range(0, len(mulaw), CHUNK):
            chunk = mulaw[i : i + CHUNK]
            pcm = mulaw_8k_to_pcm16_24k(chunk)
            samples = np.frombuffer(pcm, dtype=np.int16).astype(np.float32)
            voiced = samples.size > 0 and float(np.sqrt(np.mean(samples * samples))) > OUT_VOICED_RMS
            if not voiced:
                continue
            if not self._asst_first_audio:
                self._asst_first_audio = True
                now = _wall_ms()
                self._flush_user()
                self._asst_turn_ts = now
                if self._last_voice_ms:
                    lat = (now - self._last_voice_ms) / 1000
                    if 0 < lat < 30:
                        self._metrics_log.write_latency("model_response", lat, self._model)
            _sync(self.user_audio_buffer, len(self.assistant_audio_buffer))
            self.assistant_audio_buffer.extend(pcm)
            await q.put(chunk)

    # ── turns into the audit log ─────────────────────────────────────

    def _flush_user(self) -> None:
        text = self._user_text.strip()
        if text:
            self.audit_log.append_user_input(text, timestamp_ms=str(self._user_turn_ts or _wall_ms()))
        self._user_text = ""
        self._user_turn_ts = None

    def _flush_asst(self) -> None:
        text = self._asst_text.strip()
        if text:
            self.audit_log.append_assistant_output(text, timestamp_ms=str(self._asst_turn_ts or _wall_ms()))
            self._fw_log.llm_response(text)
            self._fw_log.turn_end(was_interrupted=False)
        self._asst_text = ""
        self._asst_turn_ts = None
        self._asst_first_audio = False

    # ── live events ──────────────────────────────────────────────────

    async def _recv_live(self, q: asyncio.Queue[bytes]) -> None:
        try:
            async for raw in self._live:
                m = json.loads(raw)
                t = m.get("type")
                if t == "session.started":
                    # A phone line always carries audio; the Live API needs it flowing
                    # before injected context completes. Fill gaps with PCMU silence.
                    self._timers.append(asyncio.create_task(self._silence_feeder()))

                    async def greet():
                        await self._send({"type": self._greet_type, "event_id": "greet", "delegation_id": None, "content": self._cfg["greeting"]})
                        if self._cfg.get("caller_context"):
                            await self._send({"type": "session.thinking.append", "event_id": "caller", "delegation_id": None, "content": self._cfg["caller_context"]})

                    self._later(self._t["greet_after_s"], greet)
                elif t == "session.output_audio.delta":
                    await self._on_audio(m.get("delta", ""), q)
                elif t == "session.output_transcript.delta":
                    if not self._hung_up:
                        self._asst_text += str(m.get("delta") or "")
                        if self._ends_call and self._decided and self._kind not in self._cue_now and self._bye.search(self._asst_text):
                            self._later(self._t["hangup_delay_s"], self._hang_up)
                        # A cued closing line: hang up once it has been spoken.
                        if self._cued and self._bye.search(self._asst_text[self._cue_pos:]):
                            self._later(self._t["hangup_delay_s"], self._hang_up)
                        # Wrap-up nudge: the agent wrapped up and nothing was recorded.
                        tail = self._asst_text[self._nudge_from:]
                        wrapped = self._bye.search(tail) or (self._wrap and self._wrap.search(tail))
                        if self._nudge_on and not self._decided and self._nudges < self._t["nudge_max"] and wrapped:
                            if self._wrap_task:
                                self._wrap_task.cancel()
                            self._wrap_task = asyncio.create_task(self._wrap_nudge())
                            self._timers.append(self._wrap_task)
                elif t == "session.input_transcript.delta":
                    self._user_text += str(m.get("delta") or "")
                elif t == "response.event":
                    await self._on_response_event(m.get("event") or {})
                elif t == "error":
                    logger.warning(f"live error: {json.dumps(m)[:300]}")
                elif t == "session.closed":
                    usage = m.get("usage") or (m.get("session") or {}).get("usage")
                    if usage:
                        self._fw_log.write("live_usage", {"usage": usage})
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"live receive loop: {e}", exc_info=True)

    async def _silence_feeder(self) -> None:
        # Real-time input pacer: one 20 ms PCMU chunk per tick, caller audio if
        # queued, else line silence. The Live API rejects faster-than-real-time input.
        silence = b"\xff" * CHUNK
        nxt = time.monotonic()
        try:
            while True:
                if len(self._in_buf) >= CHUNK:
                    chunk = bytes(self._in_buf[:CHUNK])
                    del self._in_buf[:CHUNK]
                    if len(self._in_buf) > CHUNK * 50:  # >1 s behind: drop the backlog
                        del self._in_buf[: len(self._in_buf) - CHUNK * 10]
                else:
                    chunk = silence
                await self._send({"type": "session.input_audio.append", "audio": base64.b64encode(chunk).decode("ascii")})
                nxt = max(nxt, time.monotonic() - 0.1) + CHUNK_S
                await asyncio.sleep(max(0.0, nxt - time.monotonic()))
        except asyncio.CancelledError:
            pass

    def _later(self, delay: float, fn) -> None:
        async def run():
            await asyncio.sleep(delay)
            await fn()

        self._timers.append(asyncio.create_task(run()))

    async def _hang_up(self) -> None:
        if not self._hung_up:
            self._hung_up = True
            self._flush_asst()
            self._fw_log.write("hangup", {"why": "agent ended the call"})
            self._timers.append(asyncio.create_task(self._drop_line()))

    async def _drop_line(self) -> None:
        # A phone app hangs up the leg after the goodbye. Let the queued audio play
        # out, then close the caller socket so the simulator sees the hang-up.
        while self._out_q is not None and not self._out_q.empty():
            await asyncio.sleep(0.1)
        await asyncio.sleep(1.0)
        try:
            await self._sim_ws.close()
        except Exception:
            pass

    async def _wrap_nudge(self) -> None:
        await asyncio.sleep(self._t["nudge_after_s"])
        if self._decided or self._hung_up:
            return
        self._nudges += 1
        self._nudge_from = len(self._asst_text)
        self._fw_log.write("wrap_nudge", {"n": self._nudges})
        await self._send({"type": "response.create"})
        if self._nudges == 1:
            async def give_up():
                if not self._decided and not self._hung_up:
                    self._fw_log.write("hangup_unrecorded", {"why": "said goodbye, nothing recorded"})
                    await self._hang_up()
            self._later(self._t["unrecorded_hangup_s"], give_up)

    async def _cue(self) -> None:
        if self._cued or self._hung_up:
            return
        self._cued = True
        self._cue_pos = len(self._asst_text)
        # Fallbacks: hang up if the closing line never starts, or never finishes.
        async def fallback():
            if self._asst_text[self._cue_pos:].strip():
                self._later(self._t["closing_fallback_s"], self._hang_up)
            else:
                await self._hang_up()
        self._later(self._t["cue_fallback_s"], fallback)
        line = self._cfg.get("closing_instruction", {}).get(self._kind)
        if line:
            await self._send({"type": "session.instructions.append", "event_id": "closing", "delegation_id": None, "content": line})

    async def _on_response_event(self, inner: dict) -> None:
        if inner.get("type") != "response.output_item.done":
            return
        item = inner.get("item") or {}
        if item.get("type") != "function_call":
            return
        name = item.get("name")
        try:
            args = json.loads(item.get("arguments") or "{}")
        except json.JSONDecodeError:
            args = {}
        if self._decided and name == self._outcome_tool:
            return
        result = await self.execute_tool(name, args)
        self._fw_log.write("tool_call", {"frame": "tool_call", "tool_name": name, "arguments": args, "result": result})
        if name != self._outcome_tool:
            await self._send({"type": "response.item.create", "item": {"type": "function_call_output", "call_id": item.get("call_id"), "output": json.dumps(result)}})
            await self._send({"type": "response.create"})
            return
        self._kind = (result or {}).get("kind") or "default"
        self._decided = True
        notes = self._cfg.get("tool_output", {})
        note = notes.get(self._kind) or notes.get("default") or json.dumps({"recorded": True})
        await self._send({"type": "response.item.create", "item": {"type": "function_call_output", "call_id": item.get("call_id"), "output": note}})
        await self._send({"type": "response.create"})
        if not self._ends_call:
            return
        if self._kind in self._cue_now:
            await self._cue()
        elif self._bye.search(self._asst_text):
            self._later(self._t["hangup_delay_s"], self._hang_up)
        else:
            async def maybe_cue():
                if not self._hung_up and not self._bye.search(self._asst_text):
                    await self._cue()

            self._later(self._t["closing_cue_after_s"], maybe_cue)
