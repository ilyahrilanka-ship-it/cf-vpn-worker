// VLESS-over-WebSocket на Durable Objects.
//
// Зачем DO: у обычного Cloudflare Worker лимит CPU — 10 мс на вызов, из-за чего поток
// режется в тысячу раз (замерено: 313 Б/с против 820 КБ/с напрямую). У Durable Object
// лимит — 30 секунд CPU на каждое входящее сообщение, wall time не ограничен, поэтому
// TCP-соединение можно держать открытым и качать нормально. Тариф при этом тот же Free.
//
// Важно: состояние держим на WebSocket, а не на объекте. Иначе старое соединение
// перехватывает reader() нового и всё ломается при втором клиенте.

import { connect } from "cloudflare:sockets";

const UUID = "9f9ff8dd-ccf1-47c2-ac6d-86754efff7b6";
const PATH = "/?ed=2560";

function uuidToBytes(u) {
  const h = u.replace(/-/g, "");
  const b = new Uint8Array(16);
  for (let i = 0; i < 16; i++) b[i] = parseInt(h.substr(i * 2, 2), 16);
  return b;
}
const UUID_BYTES = uuidToBytes(UUID);

const DOMAIN_RE = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$/i;
const IPV4_RE = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;

// Между addonsLen и портом клиент (sing-box) вставляет лишний байт, которого нет
// в описании VLESS. Поэтому жёстких смещений не используем: перебираем варианты и
// берём самый правдоподобный — сначала домен, потом IPv4, потом IPv6.
function parseHeader(buf) {
  if (buf.length < 24) return null;
  if (buf[0] !== 0x00) return null;
  for (let i = 0; i < 16; i++) if (buf[1 + i] !== UUID_BYTES[i]) return null;

  const cand = [];
  for (let start = 17; start < Math.min(buf.length - 2, 32); start++) {
    const port = (buf[start] << 8) | buf[start + 1];
    if (port < 1 || port > 65535) continue;
    let p = start + 2;
    const atyp = buf[p++];
    let hostname = null;
    if (atyp === 1) {
      if (p + 4 > buf.length) continue;
      hostname = `${buf[p]}.${buf[p + 1]}.${buf[p + 2]}.${buf[p + 3]}`;
      p += 4;
    } else if (atyp === 2) {
      if (p >= buf.length) continue;
      const len = buf[p++];
      if (len < 1 || p + len > buf.length) continue;
      hostname = new TextDecoder().decode(buf.subarray(p, p + len));
      p += len;
    } else if (atyp === 3) {
      if (p + 16 > buf.length) continue;
      hostname = new TextDecoder().decode(buf.subarray(p, p + 16));
      p += 16;
    } else continue;
    if (p > buf.length) continue;
    cand.push({ port, hostname, payload: buf.subarray(p), atyp });
  }
  if (!cand.length) return null;
  return (
    cand.find((c) => c.atyp === 2 && DOMAIN_RE.test(c.hostname)) ||
    cand.find((c) => c.atyp === 1 && IPV4_RE.test(c.hostname)) ||
    cand.find((c) => c.atyp === 3 && c.hostname.indexOf(":") > 0) ||
    cand[0]
  );
}

export class VlessDO {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.sessions = new Map();
  }

  async fetch(request) {
    const u = new URL(request.url);
    if (u.pathname === "/diag") {
      const d = (await this.ctx.storage.get("diag")) || [];
      return new Response(JSON.stringify(d), {
        headers: { "content-type": "application/json" },
      });
    }
    if (u.pathname === "/reset") {
      await this.ctx.storage.delete("diag");
      return new Response("reset ok");
    }
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("VLESS over Durable Object", { status: 200 });
    }
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  async diag(msg, extra) {
    try {
      const list = (await this.ctx.storage.get("diag")) || [];
      list.push({ msg, extra: String(extra || ""), at: Date.now() });
      await this.ctx.storage.put("diag", list.slice(-14));
    } catch (e) {}
  }

  session(ws) {
    let s = this.sessions.get(ws);
    if (!s) {
      s = { started: false, alive: true, writer: null, reader: null };
      this.sessions.set(ws, s);
    }
    return s;
  }

  async webSocketMessage(ws, message) {
    const s = this.session(ws);
    let data;
    try {
      data =
        message instanceof ArrayBuffer
          ? new Uint8Array(message)
          : new Uint8Array(message.buffer || message);
    } catch (e) {
      return;
    }

    if (!s.started) {
      s.started = true;
      const h = parseHeader(data);
      if (!h) {
        await this.diag("header-null", `len=${data.length}`);
        try { ws.close(1002, "bad header"); } catch (e) {}
        return;
      }
      try {
        const sock = connect({ hostname: h.hostname, port: h.port });
        await sock.opened;
        s.writer = sock.writable.getWriter();
        s.reader = sock.readable.getReader();

        // клиенту — только заголовок VLESS-ответа (версия 0x00, аддоны 0x00)
        ws.send(new Uint8Array([0, 0]));
        // первая порция клиента — вверх по TCP, иначе цель ждёт вечно
        if (h.payload.length) await s.writer.write(h.payload);

        await this.diag("open", `${h.hostname}:${h.port} up=${h.payload.length}`);
        this.pump(ws, s);
      } catch (e) {
        await this.diag("connect-fail", `${h.hostname}:${h.port} ${e && (e.message || e)}`.slice(0, 120));
        try { ws.close(1011, "connect failed"); } catch (e2) {}
      }
      return;
    }

    if (s.writer) {
      try {
        await s.writer.write(data);
      } catch (e) {
        await this.diag("write-fail", e && e.message);
      }
    }
  }

  async pump(ws, s) {
    let bytes = 0, chunks = 0;
    try {
      while (s.alive) {
        const { value, done } = await s.reader.read();
        if (done) break;
        if (!value || !value.length) continue;
        ws.send(value);
        bytes += value.length;
        if (++chunks === 1) await this.diag("first-upstream", `${value.length} Б`);
      }
    } catch (e) {
      await this.diag("pump-error", e && (e.message || e));
    }
    if (chunks) await this.diag("closed", `chunks=${chunks} bytes=${bytes}`);
    try { ws.close(1000, "eof"); } catch (e) {}
  }

  async webSocketClose(ws) {
    this.drop(ws);
  }
  async webSocketError(ws) {
    this.drop(ws);
  }

  drop(ws) {
    const s = this.sessions.get(ws);
    if (!s) return;
    s.alive = false;
    this.sessions.delete(ws);
    try { s.writer && s.writer.close(); } catch (e) {}
    try { s.reader && s.reader.cancel(); } catch (e) {}
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/sub") return sub(request);
    const id = env.DO.idFromName("vless-main");
    return env.DO.get(id).fetch(request);
  },
};

function sub(request) {
  const host = new URL(request.url).hostname;
  const line =
    `vless://${UUID}@${host}:443?encryption=none&security=tls&sni=${host}` +
    `&type=ws&host=${host}&path=${encodeURIComponent(PATH)}&alpn=http%2F1.1&fp=chrome#CF-DO`;
  return new Response(line, {
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "profile-update-interval": "6",
    },
  });
}
