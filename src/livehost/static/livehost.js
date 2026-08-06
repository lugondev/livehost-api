import { el, restoreAndBind, savePref } from "./helpers.js";
import { STREAM_SAMPLE_RATE, createMicCapture } from "./audio-capture.js";

// This page is served by the plugin, cross-origin from the gateway that
// owns auth, profiles, and STT engines -- see Task 9's topology note: the
// gateway's own UI opens this page in a tab and is expected to pass both of
// these on the URL, the same convention the ticket exchange below assumes.
const _pageParams = new URLSearchParams(location.search);
const GATEWAY_ORIGIN = (_pageParams.get("gateway") || "").replace(/\/$/, "");
const GATEWAY_TOKEN = _pageParams.get("token") || "";

// Change 2 of 3 (see Task 9 brief step 6): every call this page makes to the
// gateway -- POST /v1/plugins/ticket, GET /v1/stt/engines, POST /v1/stt/warm
// -- goes through here instead of a same-origin fetch, so it lands on the
// gateway's own origin and carries the user's bearer.
function gatewayFetch(path, options = {}) {
  const headers = { ...(options.headers || {}), Authorization: `Bearer ${GATEWAY_TOKEN}` };
  return fetch(`${GATEWAY_ORIGIN}${path}`, { ...options, headers });
}

// Change 3 of 3: the three control calls (connect/disconnect/status) go to
// the plugin's own origin (this page's own origin -- a same-origin fetch is
// already correct there) but must carry `Authorization: Bearer <ticket>`,
// which is what `_get_owned_session` on the plugin introspects.
function pluginFetch(path, options = {}) {
  const headers = { ...(options.headers || {}), Authorization: `Bearer ${lh.ticket || ""}` };
  return fetch(path, { ...options, headers });
}

export const lh = {
  ws: null, capture: null, log: [], ctx: null, nextTime: 0, sources: [], chain: null,
  opusMode: false, opusDec: null, opusTs: 0, outRate: 24000, outCodec: "wav", audioGen: 0,
  sessionId: null, statusPollTimer: null, assistantBubble: null, pendingReplyIsSocial: false,
  // Set once per session from the /v1/plugins/ticket response (change 1 of
  // 3): the ticket this page's own control calls authenticate with.
  ticket: null,
};

const lhDetails = { stt: {}, sttAvailable: true };

// Called when the active LLM profile changes: manual STT/TTS overrides must
// not silently keep beating the newly-selected profile's own config, so drop
// back to "follow the profile" and forget the sticky localStorage value.
export function resetLhManualOverrides() {
  const ttsSel = el("lh-tts-profile");
  if (ttsSel) ttsSel.value = "";
  savePref("lh-tts-profile", "");
}

function setLhStatus(text, state) {
  const node = el("lh-status");
  node.textContent = text;
  node.className = state;
}
function lhLog(line) {
  lh.log.push(line);
  if (lh.log.length > 60) lh.log.shift();
  el("lh-log").textContent = lh.log.join("\n");
}
function lhAddBubble(role, text) {
  const div = document.createElement("div");
  div.className = `bubble ${role}`;
  div.textContent = text;
  el("lh-dialogue").appendChild(div);
  el("lh-dialogue").scrollTop = el("lh-dialogue").scrollHeight;
  return div;
}
function lhAddFeedRow(text) {
  const div = document.createElement("div");
  div.className = "bubble social";
  div.textContent = text;
  el("lh-dialogue").appendChild(div);
  el("lh-dialogue").scrollTop = el("lh-dialogue").scrollHeight;
}
function lhAudioCtx() {
  if (!lh.ctx) lh.ctx = new (window.AudioContext || window.webkitAudioContext)();
  return lh.ctx;
}
function lhIsSpeaking() {
  return !!lh.ctx && (lh.nextTime || 0) > lh.ctx.currentTime + 0.15;
}
function lhStopAudio() {
  lh.audioGen = (lh.audioGen || 0) + 1; // invalidate any in-flight lhEnqueueAudioBytes decode
  (lh.sources || []).forEach((s) => {
    try {
      s.stop();
    } catch {}
  });
  lh.sources = [];
  lh.nextTime = 0;
  lh.chain = Promise.resolve();
  lhResetOpus();
}
function lhEnqueueAudioBytes(bytes) {
  const gen = lh.audioGen;
  lh.chain = (lh.chain || Promise.resolve())
    .then(async () => {
      const ctx = lhAudioCtx();
      if (ctx.state === "suspended") await ctx.resume();
      const buf = await ctx.decodeAudioData(bytes);
      if (gen !== lh.audioGen) return; // superseded by a barge-in/reset while decoding
      lhScheduleBuffer(buf);
    })
    .catch((e) => lhLog("audio error: " + e));
}
function lhOpusSupported() {
  return typeof window.AudioDecoder === "function" && typeof window.EncodedAudioChunk === "function";
}
function lhScheduleBuffer(buf) {
  const ctx = lhAudioCtx();
  const src = ctx.createBufferSource();
  src.buffer = buf;
  src.connect(ctx.destination);
  const start = Math.max(ctx.currentTime + 0.05, lh.nextTime || 0);
  src.start(start);
  lh.nextTime = start + buf.duration;
  lh.sources.push(src);
  src.onended = () => {
    lh.sources = lh.sources.filter((s) => s !== src);
  };
}
function lhInitOpusDecoder() {
  if (lh.opusDec) {
    try {
      lh.opusDec.close();
    } catch {}
  }
  lh.opusTs = 0;
  const ctx = lhAudioCtx();
  const dec = new AudioDecoder({
    output: (audioData) => {
      try {
        const frames = audioData.numberOfFrames;
        const buf = ctx.createBuffer(1, frames, audioData.sampleRate);
        const arr = new Float32Array(frames);
        audioData.copyTo(arr, { planeIndex: 0, format: "f32-planar" });
        buf.copyToChannel(arr, 0);
        lhScheduleBuffer(buf);
      } catch (e) {
        lhLog("opus output error: " + e);
      } finally {
        audioData.close();
      }
    },
    error: (e) => lhLog("opus decoder error: " + e),
  });
  dec.configure({ codec: "opus", sampleRate: lh.outRate, numberOfChannels: 1 });
  lh.opusDec = dec;
}
function lhFeedOpus(data) {
  if (!lh.opusDec || lh.opusDec.state === "closed") lhInitOpusDecoder();
  try {
    lh.opusDec.decode(new EncodedAudioChunk({ type: "key", timestamp: lh.opusTs, data }));
    lh.opusTs += 60000;
  } catch (e) {
    lhLog("opus feed error: " + e);
  }
}
function lhResetOpus() {
  if (lh.opusDec) {
    try {
      lh.opusDec.close();
    } catch {}
    lh.opusDec = null;
  }
  lh.opusTs = 0;
}

function setLhSessionUI(state) {
  el("lh-session-start").disabled = state !== "idle";
  el("lh-session-stop").disabled = state === "idle";
}
function setLhTiktokControlsEnabled(enabled) {
  el("lh-tiktok-username").disabled = !enabled;
  el("lh-tiktok-connect").disabled = !enabled;
}

function tiktokStatusLabel(state) {
  return (
    { idle: "idle", connecting: "connecting…", live: "live", reconnecting: "reconnecting…", offline_waiting: "offline, waiting…", error: "error" }[state] ||
    state
  );
}
function tiktokStatusClass(state) {
  if (state === "live") return "status-rec";
  if (state === "error") return "status-error";
  return "status-idle";
}
function setLhTiktokBadge(state) {
  const node = el("lh-tiktok-status");
  node.textContent = tiktokStatusLabel(state);
  node.className = tiktokStatusClass(state);
}

function stopLhStatusPoll() {
  if (lh.statusPollTimer) {
    clearInterval(lh.statusPollTimer);
    lh.statusPollTimer = null;
  }
}
function startLhStatusPoll() {
  stopLhStatusPoll();
  lh.statusPollTimer = setInterval(async () => {
    if (!lh.sessionId) return;
    try {
      const resp = await pluginFetch(`/v1/livehost/${encodeURIComponent(lh.sessionId)}/status`);
      if (!resp.ok) {
        stopLhStatusPoll();
        setLhTiktokBadge("idle");
        return;
      }
      const body = await resp.json();
      setLhTiktokBadge(body.data.state);
    } catch {
      /* transient poll failure — try again next tick */
    }
  }, 2000);
}

export async function loadLivehostEngines() {
  try {
    const stt = await (await gatewayFetch("/v1/stt/engines")).json();
    stt.data.forEach((e) => (lhDetails.stt[e.engine] = e.detail));
    lhDetails.sttAvailable = stt.data.some((e) => e.available);
    restoreAndBind("lh-language");
    restoreAndBind("lh-opus");
  } catch (error) {
    lhLog(`engines error: ${error}`);
  }
}

export async function startLhSession() {
  setLhSessionUI("starting");
  lhStopAudio();
  lh.outCodec = "wav"; // re-announced by the first audio_start of the new session
  el("lh-dialogue").innerHTML = "";
  lh.log = [];
  el("lh-log").textContent = "";
  setLhTiktokBadge("idle");
  el("lh-tiktok-error").classList.add("hidden");

  if (!lhDetails.sttAvailable) {
    setLhStatus("No STT engine available", "status-error");
    setLhSessionUI("idle");
    return;
  }
  const profile = el("lh-profile")?.value;

  setLhStatus("⏳ starting STT engine…", "status-idle");
  try {
    const warmParams = profile ? `profile=${encodeURIComponent(profile)}` : "";
    const warmRes = await gatewayFetch(`/v1/stt/warm?${warmParams}`, { method: "POST" });
    if (!warmRes.ok) {
      setLhStatus("STT engine (server default) not ready", "status-error");
      setLhSessionUI("idle");
      return;
    }
  } catch {
    setLhStatus("Could not connect to STT engine", "status-error");
    setLhSessionUI("idle");
    return;
  }

  const soloMode = !!el("lh-mode-solo")?.checked;

  lh.sessionId = crypto.randomUUID();
  let params = `session_id=${encodeURIComponent(lh.sessionId)}`;
  params += `&sample_rate=${STREAM_SAMPLE_RATE}`;
  if (profile) params += `&profile=${encodeURIComponent(profile)}`;
  const ttsProfile = el("lh-tts-profile")?.value;
  if (ttsProfile) params += `&tts_profile=${encodeURIComponent(ttsProfile)}`;
  if (el("lh-language").value.trim()) params += `&language=${encodeURIComponent(el("lh-language").value.trim())}`;

  lh.opusMode = !!el("lh-opus")?.checked && lhOpusSupported();
  if (el("lh-opus")?.checked && !lh.opusMode) {
    lhLog("Opus downlink unsupported in this browser — using WAV.");
  }
  if (lh.opusMode) {
    lh.outRate = 24000;
    params += `&output=audio,text&audio_out=opus&output_sample_rate=${lh.outRate}`;
  }
  lhResetOpus();

  // Solo host: no streamer mic at all — the co-host runs purely off TikTok
  // chat/gifts (the backend's social-turn poll already fires freely whenever
  // no voice turn is active, so simply never sending audio is sufficient).
  let capture = null;
  if (!soloMode) {
    try {
      capture = createMicCapture({
        onframe: (pcm) => {
          if (!lh.ws || lh.ws.readyState !== WebSocket.OPEN) return;
          if (lhIsSpeaking()) return;
          lh.ws.send(pcm.buffer);
        },
      });
    } catch (error) {
      setLhStatus("mic error", "status-error");
      setLhSessionUI("idle");
      return;
    }
  }

  // Change 1 of 3: the WS URL is no longer a same-origin path -- this page
  // is served by the plugin, not the gateway, so it has to trade the user's
  // gateway bearer for a plugin ticket first, then build the plugin's own
  // WS URL (its origin, from the ticket response's `data.url`, plus the
  // unchanged mount path) with that ticket as `?ticket=`.
  let ticketBody;
  try {
    ticketBody = await (
      await gatewayFetch("/v1/plugins/ticket", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ plugin: "livehost" }),
      })
    ).json();
  } catch (error) {
    setLhStatus("could not reach gateway for a ticket", "status-error");
    setLhSessionUI("idle");
    return;
  }
  const pluginUrl = ticketBody?.data?.url;
  const ticket = ticketBody?.data?.ticket;
  if (!pluginUrl || !ticket) {
    setLhStatus("plugin ticket unavailable", "status-error");
    setLhSessionUI("idle");
    return;
  }
  lh.ticket = ticket;
  const wsScheme = pluginUrl.startsWith("https://") ? "wss" : "ws";
  const wsHost = pluginUrl.replace(/^https?:\/\//, "").replace(/\/$/, "");
  const ws = new WebSocket(
    `${wsScheme}://${wsHost}/v1/livehost/stream?${params}&ticket=${encodeURIComponent(ticket)}`
  );
  lh.ws = ws;
  ws.binaryType = "arraybuffer";

  ws.onopen = async () => {
    if (soloMode) {
      setLhStatus("● solo host (chat only)", "status-rec");
      setLhSessionUI("recording");
      return;
    }
    try {
      await capture.start();
      lh.capture = capture;
      setLhStatus("● listening", "status-rec");
      setLhSessionUI("recording");
    } catch (error) {
      setLhStatus("mic denied", "status-error");
      ws.close();
    }
  };

  ws.onmessage = (event) => {
    // Binary frames are reply audio: Opus packets when audio_out=opus, else a
    // complete WAV/MP3 per sentence -- routed by the codec the server
    // announced in the preceding audio_start event.
    if (typeof event.data !== "string") {
      if (lh.outCodec === "opus") lhFeedOpus(event.data);
      else lhEnqueueAudioBytes(event.data);
      return;
    }
    let d;
    try {
      d = JSON.parse(event.data);
    } catch {
      return;
    }
    lhLog(`${d.event}: ${d.text ? d.text.slice(0, 60) : JSON.stringify({ ...d, event: undefined })}`);
    switch (d.event) {
      case "session_started":
        if (d.output_sample_rate) lh.outRate = d.output_sample_rate;
        setLhTiktokControlsEnabled(true);
        if (d.stt_ready === false || d.tts_ready === false) {
          setLhStatus("⏳ engines warming up, please wait…", "status-idle");
        }
        break;
      case "engines_ready":
        setLhStatus("● listening", "status-rec");
        break;
      case "speech_start":
        setLhStatus("● you're speaking", "status-rec");
        break;
      case "speech_end":
      case "processing":
        setLhStatus("… thinking", "status-idle");
        break;
      case "user_transcript":
        if (d.text) lhAddBubble("user", d.text);
        break;
      case "social_event": {
        const label =
          d.kind === "gift"
            ? `🎁 ${d.user_name} gifted ${d.gift_name || ""}${d.gift_value ? ` (${d.gift_value})` : ""}`
            : d.kind === "follow"
              ? `${d.user_name} followed`
              : d.kind === "like"
                ? `${d.user_name} liked`
                : `${d.user_name}: ${d.text || ""}`;
        lhAddFeedRow(label);
        break;
      }
      case "social_reply":
        lh.pendingReplyIsSocial = true;
        break;
      case "response_text": {
        if (d.chunk_index === 0 || !lh.assistantBubble) {
          const prefix = lh.pendingReplyIsSocial ? "↳ replying to chat: " : "";
          lh.assistantBubble = lhAddBubble("assistant", prefix + d.text);
          lh.pendingReplyIsSocial = false;
        } else {
          lh.assistantBubble.textContent += " " + d.text;
          el("lh-dialogue").scrollTop = el("lh-dialogue").scrollHeight;
        }
        break;
      }
      case "audio_start":
        lh.outCodec = d.codec || "wav";
        if (d.codec === "opus" && d.sample_rate) lh.outRate = d.sample_rate;
        break;
      case "turn_done":
        lh.assistantBubble = null;
        lh.pendingReplyIsSocial = false;
        setLhStatus("● listening", "status-rec");
        break;
      case "aborted":
        lhStopAudio();
        break;
      case "tts_error":
        // Reply text already streamed via response_text; only audio failed. Flag it
        // on the bubble + log, without the fatal status-error state (turn continues).
        lhLog(`tts_error: ${d.message || ""}`);
        if (lh.assistantBubble) lh.assistantBubble.textContent += " 🔇 (lỗi TTS — chỉ hiển thị văn bản)";
        break;
      case "error":
        setLhStatus(`error: ${d.message || ""}`, "status-error");
        break;
    }
  };

  ws.onerror = () => setLhStatus("ws error", "status-error");
  ws.onclose = () => {
    setLhSessionUI("idle");
    setLhTiktokControlsEnabled(false);
    el("lh-tiktok-disconnect").disabled = true;
    stopLhStatusPoll();
    setLhTiktokBadge("idle");
    lh.sessionId = null;
    if (el("lh-status").className !== "status-error") setLhStatus("idle", "status-idle");
  };
}

export function stopLhSession() {
  if (lh.capture) {
    lh.capture.stop();
    lh.capture = null;
  }
  if (lh.ws) {
    if (lh.ws.readyState === WebSocket.OPEN) lh.ws.send(JSON.stringify({ type: "end" }));
    try {
      lh.ws.close();
    } catch {}
    lh.ws = null;
  }
  stopLhStatusPoll();
  setLhSessionUI("idle");
}

export async function connectTiktok() {
  const username = el("lh-tiktok-username").value.trim();
  const errEl = el("lh-tiktok-error");
  errEl.classList.add("hidden");
  if (!username) {
    errEl.textContent = "Enter a TikTok username";
    errEl.classList.remove("hidden");
    return;
  }
  if (!lh.sessionId) {
    errEl.textContent = "Start the session first";
    errEl.classList.remove("hidden");
    return;
  }
  try {
    const resp = await pluginFetch(`/v1/livehost/${encodeURIComponent(lh.sessionId)}/connect`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ unique_id: username }),
    });
    const body = await resp.json();
    if (!resp.ok) {
      errEl.textContent = body.detail || "Connect failed";
      errEl.classList.remove("hidden");
      return;
    }
    setLhTiktokBadge(body.data.state);
    el("lh-tiktok-disconnect").disabled = false;
    startLhStatusPoll();
  } catch (error) {
    errEl.textContent = String(error);
    errEl.classList.remove("hidden");
  }
}

export async function disconnectTiktok() {
  if (!lh.sessionId) return;
  try {
    const resp = await pluginFetch(`/v1/livehost/${encodeURIComponent(lh.sessionId)}/disconnect`, {
      method: "POST",
    });
    if (resp.ok) {
      const body = await resp.json();
      setLhTiktokBadge(body.data.state);
    }
  } catch {
    /* best-effort */
  } finally {
    stopLhStatusPoll();
    el("lh-tiktok-disconnect").disabled = true;
  }
}

if (el("lh-session-start")) el("lh-session-start").addEventListener("click", startLhSession);
if (el("lh-session-stop"))
  el("lh-session-stop").addEventListener("click", () => {
    lhStopAudio();
    stopLhSession();
  });
if (el("lh-tiktok-connect")) el("lh-tiktok-connect").addEventListener("click", connectTiktok);
if (el("lh-tiktok-disconnect")) el("lh-tiktok-disconnect").addEventListener("click", disconnectTiktok);
