// VLESS-over-WebSocket на Durable Objects.
// Зачем DO: у обычного Worker лимит CPU 10 мс на вызов — поток режется в тысячу раз.
// У Durable Object лимит 30 с CPU на каждое входящее сообщение, wall time не ограничен,
// поэтому держим TCP-соединение открытым и качаем нормально.

import { connect } from "cloudflare:sockets";

const VLESSSUB = "/sub/9f9ff8dd-ccf1-47c2-ac6d-86754efff7b6";
const UUID = "9f9ff8dd-ccf1-47c2-ac6d-86754efff7b6";
const PATH = "/?ed=2560";

function uuidToBytes(u) {
  const h = u.replace(/-/g, "");
  const b = new Uint8Array(16);
  for (let i = 0; i < 16; i++) b[i] = parseInt(h.substr(i * 2, 2), 16);
  return b;
}

const UUID_BYTES = uuidToBytes(UUID);

function parseHeader(buf) {
  if (buf.length < 24) return null;
  for (let i = 0; i < 16; i++) if (buf[i] !== UUID_BYTES[i]) return null;
  let p = 16;
  const addonsLen = buf[p++];
  p += addonsLen;
  if (p + 2 > buf.length) return null;
  const port = (buf[p] << 8) | buf[p + 1];
  p += 2;
  const atyp = buf[p++];
  let hostname;
  if (atyp === 1) {
    hostname = `${buf[p]}.${buf[p + 1]}.${buf[p + 2]}.${buf[p + 3]}`;
    p += 4;
  } else if (atyp === 2) {
    const len = buf[p++];
    hostname = new TextDecoder().decode(buf.subarray(p, p + len));
    p += len;
  } else if (atyp === 3) {
    const a = [];
    for (let i = 0; i < 8; i++) a.push(((buf[p + i] << 8) | buf[p + i + 1]).toString(16));
    hostname = new TextDecoder().decode(buf.subarray(p, p + 16));
    p += 16;
  } else return null;
  if (p > buf.length) return null;
  return { port, hostname, payload: buf.subarray(p) };
}

export class VlessDO {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.socket = null;
    this.writer = null;
    this.alive = true;
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("VLESS endpoint (Durable Object)", { status: 200 });
    }
    let pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ started: false });
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const data = message instanceof ArrayBuffer ? new Uint8Array(message) : new Uint8Array(message.buffer);
    const st = ws.deserializeAttachment();
    if (!st || !st.started) {
      ws.serializeAttachment({ started: true });
      const h = parseHeader(data);
      if (!h) { ws.close(1002, "bad header"); return; }
      try {
        const sock = connect({ hostname: h.hostname, port: h.port });
        this.writer = sock.writable.getWriter();
        this.reader = sock.readable.getReader();
        await sock.opened;
        // ответ VLESS: версия 0x00, длина аддонов 0x00
        const head = new Uint8Array([0, 0]);
        const out = new Uint8Array(head.length + h.payload.length);
        out.set(head, 0);
        out.set(h.payload, head.length);
        if (h.payload.length) ws.send(out);
        else this.pending = true;
        this.pumpToWs(ws);
      } catch (e) {
        ws.close(1011, "connect failed");
      }
      return;
    }
    if (this.writer) {
      try { await this.writer.write(data); } catch (e) { }
    }
  }

  async pumpToWs(ws) {
    try {
      while (this.alive) {
        const { value, done } = await this.reader.read();
        if (done) break;
        if (!value || !value.length) continue;
        if (this.pending) {
          this.pending = false;
          const out = new Uint8Array(2 + value.length);
          out.set([0, 0], 0);
          out.set(value, 2);
          ws.send(out);
        } else {
          ws.send(value);
        }
      }
    } catch (e) { }
    try { ws.close(1000, "eof"); } catch (e) { }
  }

  async webSocketClose() {
    this.alive = false;
    try { this.writer && (await this.writer.close()); } catch (e) { }
    try { this.socket && this.socket.close(); } catch (e) { }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === VLESSSUB) return this.sub(request, env);
    if (url.pathname === "/sub") return this.sub(request, env);
    const id = env.DO.idFromName("vless-main");
    return env.DO.get(id).fetch(request);
  },

  sub(request, env) {
    const host = url_host(request);
    const lines = [
      `vless://${UUID}@${host}:443?encryption=none&security=tls&sni=${host}&type=ws&host=${host}&path=${encodeURIComponent(PATH)}&alpn=http%2F1.1&fp=chrome#CF-DO`,
    ];
    return new Response(lines.join("\n"), {
      headers: {
        "content-type": "text/plain; charset=utf-8",
        "profile-update-interval": "6",
      },
    });
  },
};

function url_host(request) {
  return new URL(request.url).hostname;
}
