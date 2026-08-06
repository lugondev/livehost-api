export const STREAM_SAMPLE_RATE = 16000;

// Average-decimate a float32 buffer from inputRate down to targetRate -> Int16.
export function downsampleToPcm16(input, inputRate, targetRate) {
  const ratio = inputRate / targetRate;
  const outLength = Math.floor(input.length / ratio);
  const pcm = new Int16Array(outLength);
  let pos = 0;
  for (let i = 0; i < outLength; i++) {
    const start = Math.floor(i * ratio);
    const end = Math.min(Math.floor((i + 1) * ratio), input.length);
    let sum = 0;
    let count = 0;
    for (let j = start; j < end; j++) {
      sum += input[j];
      count++;
    }
    const sample = count ? sum / count : input[start] || 0;
    const clamped = Math.max(-1, Math.min(1, sample));
    pcm[pos++] = clamped < 0 ? clamped * 0x8000 : clamped * 0x7fff;
  }
  return pcm;
}

export function writeStr(view, offset, str) {
  for (let i = 0; i < str.length; i++) view.setUint8(offset + i, str.charCodeAt(i));
}

// Encode a list of Int16Array chunks into a mono PCM16 WAV blob.
export function encodeWav(chunks, sampleRate) {
  const length = chunks.reduce((n, c) => n + c.length, 0);
  const buffer = new ArrayBuffer(44 + length * 2);
  const view = new DataView(buffer);
  writeStr(view, 0, "RIFF");
  view.setUint32(4, 36 + length * 2, true);
  writeStr(view, 8, "WAVE");
  writeStr(view, 12, "fmt ");
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true);
  view.setUint16(22, 1, true);
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, sampleRate * 2, true);
  view.setUint16(32, 2, true);
  view.setUint16(34, 16, true);
  writeStr(view, 36, "data");
  view.setUint32(40, length * 2, true);
  let offset = 44;
  for (const c of chunks) {
    for (let i = 0; i < c.length; i++) {
      view.setInt16(offset, c[i], true);
      offset += 2;
    }
  }
  return new Blob([view], { type: "audio/wav" });
}

// A reusable mic capture that yields PCM frames via onframe and can build a WAV.
export function createMicCapture({ onframe } = {}) {
  return {
    ctx: null,
    source: null,
    processor: null,
    stream: null,
    chunks: [],
    async start() {
      this.chunks = [];
      // Browser-side AEC + noise suppression + AGC: stops TTS playback bleeding
      // back into the mic (enables clean barge-in) and reduces room noise.
      this.stream = await navigator.mediaDevices.getUserMedia({
        audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true },
      });
      const AudioCtx = window.AudioContext || window.webkitAudioContext;
      this.ctx = new AudioCtx();
      this.source = this.ctx.createMediaStreamSource(this.stream);
      this.processor = this.ctx.createScriptProcessor(4096, 1, 1);
      this.processor.onaudioprocess = (event) => {
        const input = event.inputBuffer.getChannelData(0);
        const pcm = downsampleToPcm16(input, this.ctx.sampleRate, STREAM_SAMPLE_RATE);
        this.chunks.push(pcm);
        if (onframe) onframe(pcm);
      };
      this.source.connect(this.processor);
      this.processor.connect(this.ctx.destination);
    },
    durationSeconds() {
      const samples = this.chunks.reduce((n, c) => n + c.length, 0);
      return samples / STREAM_SAMPLE_RATE;
    },
    stop() {
      if (this.processor) {
        this.processor.disconnect();
        this.processor.onaudioprocess = null;
        this.processor = null;
      }
      if (this.source) {
        this.source.disconnect();
        this.source = null;
      }
      if (this.ctx) {
        this.ctx.close();
        this.ctx = null;
      }
      if (this.stream) {
        this.stream.getTracks().forEach((t) => t.stop());
        this.stream = null;
      }
    },
    toWavBlob() {
      return encodeWav(this.chunks, STREAM_SAMPLE_RATE);
    },
  };
}

