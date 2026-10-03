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

const DOMAIN_RE = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$/i;
const IPV4_RE = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;

// Разбор VLESS-адреса. У клиента между addonsLen и портом может стоять лишний байт
// (наблюдалось у sing-box), поэтому вместо жёстких смещений перебираем варианты
// и выбираем самый правдоподобный: сначала домен, потом IPv4, потом IPv6.
function parseHeader(buf) {
  if (buf.length < 24) return null;
  if (buf[0] !== 0x00) return null;
  for (let i = 0; i < 16; i++) if (buf[1 + i] !== UUID_BYTES[i]) return null;

  const seen = [];
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
    seen.push({ port, hostname, payload: buf.subarray(p), atyp });
  }
  if (!seen.length) return null;

  let best = seen.find((c) => c.atyp === 2 && DOMAIN_RE.test(c.hostname));
  if (!best) best = seen.find((c) => c.atyp === 1 && IPV4_RE.test(c.hostname));
  if (!best) best = seen.find((c) => c.atyp === 3 && c.hostname.indexOf(":") > 0);
  if (!best) best = seen[0];
  return best;
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
    const u = new URL(request.url);
    if (u.pathname === "/diag") {
      const d = (await this.ctx.storage.get("diag")) || { msg: "нет данных" };
      return new Response(JSON.stringify(d), { headers: { "content-type": "application/json" } });
    }
    if (u.pathname === "/reset") {
      await this.ctx.storage.delete("diag");
      return new Response("reset ok");
    }
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

  async diag(msg, extra) {
    try {
      const list = (await this.ctx.storage.get("diag")) || [];
      list.push({ msg, extra: String(extra || ""), at: Date.now() });
      await this.ctx.storage.put("diag", list.slice(-12));
    } catch (e) { }
  }

  async webSocketMessage(ws, message) {
    let data;
    try {
      data = message instanceof ArrayBuffer ? new Uint8Array(message)
        : new Uint8Array(message.buffer || message);
    } catch (e) {
      await this.diag("convert-fail", e && e.message); return;
    }
    const st = ws.deserializeAttachment();
    if (!st || !st.started) {
      const hx = Array.from(data.slice(0, 48)).map((x,i)=>`${i}:${x.toString(16).padStart(2,"0")}`).join(" ");
      const asc = Array.from(data.slice(0, 48)).map(x=>x>=32&&x<127?String.fromCharCode(x):".").join("");
      await this.diag("bytes", hx);
      await this.diag("ascii", asc);
      await this.diag("want-uuid", Array.from(UUID_BYTES).map(x=>x.toString(16).padStart(2,"0")).join(" "));
      ws.serializeAttachment({ started: true });
      let h = null;
      try { h = parseHeader(data); }
      catch (e) { await this.diag("parse-throw", e && e.message); }
      if (!h) { await this.diag("header-null", `len=${data.length}`); ws.close(1002, "bad header"); return; }
      await this.diag("target", `${h.hostname}:${h.port} payload=${h.payload.length}`);
      try {
        const sock = connect({ hostname: h.hostname, port: h.port });
        await this.diag("connect-called", `${h.hostname}:${h.port}`);
        await sock.opened;
        this.writer = sock.writable.getWriter();
        this.reader = sock.readable.getReader();
        await this.diag("opened", `${h.hostname}:${h.port}`);

        // 1) клиенту уходит ТОЛЬКО заголовок VLESS-ответа (версия 0x00, аддоны 0x00).
        //    Дальше клиент ждёт чистый поток от цели — эхо его же данных здесь недопустимо.
        ws.send(new Uint8Array([0, 0]));
        await this.diag("vless-response-sent", "2 байта");

        // 2) первая порция клиента уходит вверх по TCP, иначе цель ждёт данных вечно
        if (h.payload.length) {
          await this.writer.write(h.payload);
          await this.diag("payload-forwarded", `${h.payload.length} байт вверх`);
        }
        this.pumpToWs(ws);
      } catch (e) {
        await this.diag("connect-fail", e && (e.message || e));
        try { ws.close(1011, "connect failed"); } catch (e2) { }
      }
      return;
    }
    if (this.writer) {
      try { await this.writer.write(data); }
      catch (e) { await this.diag("write-fail", e && e.message); }
    }
  }

  async pumpToWs(ws) {
    let chunks = 0, bytes = 0, firstLen = -1;
    try {
      await this.diag("pump-start", "reader=" + (!!this.reader));
      while (this.alive) {
        const { value, done } = await this.reader.read();
        if (done) { await this.diag("pump-done", `chunks=${chunks} bytes=${bytes}`); break; }
        if (!value || !value.length) continue;
        if (chunks === 0) { firstLen = value.length; await this.diag("upstream-first", `len=${firstLen} hex0=${Array.from(value.slice(0,6)).map(x=>x.toString(16).padStart(2,"0")).join(" ")}`); }
        if (this.pending) {
          this.pending = false;
          const out = new Uint8Array(2 + value.length);
          out.set([0, 0], 0);
          out.set(value, 2);
          ws.send(out);
        } else {
          ws.send(value);
        }
        chunks++; bytes += value.length;
        if (chunks === 1) await this.diag("first-sent", `total=${bytes}`);
      }
    } catch (e) { await this.diag("pump-error", e && (e.message || e)); }
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
    if (url.pathname === "/diag" || url.pathname === "/reset") {
      const id0 = env.DO.idFromName("vless-main");
      return env.DO.get(id0).fetch(request);
    }
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
