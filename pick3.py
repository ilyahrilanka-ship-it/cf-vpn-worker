#!/usr/bin/env python3
"""Пересборка подписки: воркер отдаёт ~6 вариантов, берём 3 лучших.

Правила:
  - CF-HOST (по имени) обязан попасть в выборку: он единственный переживает
    смену IP у Cloudflare.
  - остальные 2 — самые быстрые из оставшихся IP, желательно разные порты.
  - если что-то не измерилось — публикуем то, что работает; если не сработало
    ничего, старый cf3.txt не трогаем (подпишка лучше устаревшая, чем пустая).
"""
import base64, json, os, pathlib, platform, subprocess, sys, tarfile, time
import urllib.parse as up
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
WORK = HERE / "work"
WORK.mkdir(exist_ok=True)
SUB = HERE / "cf3.txt"
PLAIN = HERE / "cf3-plain.txt"
STATUS = HERE / "cf3.json"

PORT = 17911
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36"
SB_VER = "1.10.7"


def log(*a):
    print(*a, flush=True)


def fetch(url, tries=3):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            return urllib.request.urlopen(req, timeout=45).read()
        except Exception as e:
            log(f"  попытка {i+1}/{tries} не удалась: {type(e).__name__}")
            time.sleep(3)
    return b""


def _runs(p):
    """Можно ли реально запустить этот файл на текущей системе."""
    if not (p.exists() and os.access(p, os.X_OK)):
        return False
    try:
        subprocess.run([str(p), "version"], capture_output=True, timeout=20)
        return True
    except Exception:
        return False


def singbox_path():
    p = WORK / "sing-box"
    if _runs(p):
        return p
    plat = {"Darwin": "darwin", "Linux": "linux"}.get(platform.system(), "linux")
    arch = {"x86_64": "amd64", "AMD64": "amd64",
            "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine(), "amd64")
    tar = WORK / "sb.tgz"
    url = (f"https://github.com/SagerNet/sing-box/releases/download/"
           f"v{SB_VER}/sing-box-{SB_VER}-{plat}-{arch}.tar.gz")
    log(f"  качаю sing-box {SB_VER} для {plat}-{arch}")
    body = fetch(url)
    if not body:
        raise RuntimeError(f"не скачался sing-box: {url}")
    tar.write_bytes(body)
    with tarfile.open(tar) as t:
        m = [x for x in t.getmembers() if x.name.endswith("sing-box")]
        if not m:
            raise RuntimeError("sing-box не найден в архиве")
        m[0].name = "sing-box"
        t.extract(m[0], WORK)
    p.chmod(0o755)
    if not _runs(p):
        raise RuntimeError("sing-box скачался, но не запускается")
    return p


def worker_links():
    """Читаем все vless-ссылки, которые отдаёт сам воркер."""
    raw = WORK / "worker_sub.txt"
    body = fetch(f"https://{HOST}/sub/{UUID}")
    if not body:
        return []
    raw.write_bytes(body)
    txt = body.decode("utf-8", "ignore").strip()
    try:
        txt = base64.b64decode(txt + "=" * (-len(txt) % 4)).decode("utf-8", "ignore")
    except Exception:
        pass
    out = []
    for l in txt.split("\n"):
        l = l.strip()
        if not l.startswith("vless://"):
            continue
        u = up.urlparse(l)
        q = dict(up.parse_qsl(u.query))
        q["alpn"] = "http/1.1"          # h2 ломает WebSocket — принудительно
        out.append({
            "host": u.hostname, "port": u.port, "uuid": u.username,
            "sni": q.get("sni") or u.hostname, "fp": q.get("fp", "chrome"),
            "path": q.get("path", "/"), "host_h": q.get("host") or u.hostname,
        })
    seen, uniq = set(), []
    for n in out:
        k = f"{n['host']}:{n['port']}"
        if k not in seen:
            seen.add(k)
            uniq.append(n)
    return uniq


def test(node, sb, rounds=3):
    """Реальный запрос к YouTube через туннель. Возвращает (успехов, медиана мс)."""
    cfg = {
        "log": {"level": "warn"},
        "inbounds": [{"type": "mixed", "listen": "127.0.0.1", "listen_port": PORT}],
        "outbounds": [{
            "type": "vless", "server": node["host"], "server_port": node["port"],
            "uuid": node["uuid"],
            "transport": {"type": "ws", "path": node["path"],
                          "headers": {"Host": node["host_h"]}},
            "tls": {"enabled": True, "server_name": node["sni"], "alpn": ["http/1.1"],
                    "utls": {"enabled": True, "fingerprint": node["fp"]}},
        }],
    }
    cf = WORK / "one.json"
    cf.write_text(json.dumps(cfg))
    p = subprocess.Popen([str(sb), "run", "-c", str(cf)],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(3)
        vals = []
        for _ in range(rounds):
            r = subprocess.run(
                ["curl", "-s", "-4", "--max-time", "25", "-o", "/dev/null",
                 "-w", "%{http_code} %{time_total}",
                 "--proxy", f"socks5h://127.0.0.1:{PORT}",
                 "https://www.youtube.com/generate_204"],
                capture_output=True, text=True)
            try:
                code, t = r.stdout.split()
                if code == "204":
                    vals.append(float(t) * 1000)
            except Exception:
                pass
    finally:
        p.terminate()
        try:
            p.wait(timeout=10)
        except Exception:
            p.kill()
    if not vals:
        return 0, None
    vals.sort()
    return len(vals), vals[len(vals) // 2]


def main():
    host, uuid = HOST, UUID
    log(f"=== пересборка {time.strftime('%Y-%m-%d %H:%M')} ===")
    log(f"  воркер: {HOST}/sub/{UUID[:8]}…")
    nodes = worker_links()
    if not nodes:
        log("  воркер не отдал ссылок — оставляю прошлую подписку")
        return 1
    log(f"  кандидатов от воркера: {len(nodes)}")

    sb = singbox_path()
    good = []
    for n in nodes:
        ok, med = test(n, sb)
        key = f"{n['host']}:{n['port']}"
        if ok:
            log(f"  {key:36} {ok}/3  {med:6.0f} мс")
            good.append({**n, "ok": ok, "med": med})
        else:
            log(f"  {key:36} не отвечает — пропускаю")

    if not good:
        log("  ни один узел не прошёл — старую подписку не трогаю")
        return 2

    pick = []
    by_name = next((n for n in good if n["host"] == HOST), None)
    if by_name:
        pick.append(by_name)
    rest = sorted((n for n in good if n is not by_name), key=lambda n: n["med"])
    # разные порты = разные пути блокировки
    used = {n["port"] for n in pick}
    for n in rest:
        if len(pick) >= 3:
            break
        if n["port"] not in used or len([p for p in pick if p["port"] == n["port"]]) < 2:
            pick.append(n)
            used.add(n["port"])
    for n in rest:                      # добор если порты совпали
        if len(pick) >= 3:
            break
        if n not in pick:
            pick.append(n)

    pick = pick[:3]
    names = ["CF-HOST"] + [f"CF-FAST-{i}" for i in range(1, len(pick))]
    links = []
    for nm, n in zip(names, pick):
        q = {"encryption": "none", "security": "tls", "sni": n["sni"], "fp": n["fp"],
             "alpn": "http/1.1", "type": "ws", "host": n["host_h"], "path": n["path"]}
        links.append(f"vless://{n['uuid']}@{n['host']}:{n['port']}?{up.urlencode(q)}#{nm}")

    body = "\n".join(links) + "\n"
    SUB.write_text(base64.b64encode(body.encode()).decode())
    PLAIN.write_text(body)
    STATUS.write_text(json.dumps({
        "updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(links),
        "worker": f"{HOST}/sub/{UUID}",
        "nodes": [{"name": nm, "server": f"{n['host']}:{n['port']}",
                   "median_ms": round(n["med"]), "ok": f"{n['ok']}/3"}
                  for nm, n in zip(names, pick)],
    }, indent=1, ensure_ascii=False), encoding="utf-8")

    log("  опубликовано:")
    for nm, n in zip(names, pick):
        log(f"    {nm:10} {n['host']}:{n['port']:6} {n['med']:6.0f} мс")
    return 0


if __name__ == "__main__":
    HOST = os.environ.get("CF_WORKER_HOST", "vpn-test.ilyahrilanka.workers.dev")
    UUID = os.environ.get("CF_WORKER_UUID", "")
    if not UUID:
        p = HERE / "uuid.txt"
        UUID = p.read_text().strip() if p.exists() else ""
    if not UUID:
        log("нет UUID — задай CF_WORKER_UUID")
        sys.exit(1)
    sys.exit(main())
