import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import URLError
from urllib.request import Request, urlopen

log = logging.getLogger(__name__)
_PORT = int(os.environ.get("PORT", 8080))
_PING_INTERVAL = 300
_SERVER: HTTPServer | None = None
_PING_THREAD: threading.Thread | None = None
_LOCK = threading.Lock()


import urllib.parse
import threading as _threading

# ── Stream-proxy registry ────────────────────────────────────────────────────
# Maps short code -> {"url": cdn_url, "name": filename, "size": int, "ts": float}
# Populated by register_stream_proxy() called from main.py before sending the
# stream link to the user. Entries expire after STREAM_PROXY_TTL seconds.
_stream_registry: dict = {}
_stream_registry_lock = _threading.Lock()
STREAM_PROXY_TTL = 3600  # 1 hour — Flezen CDN signed URLs are valid ~1h

_MIME_MAP = {
    "mp4": "video/mp4", "mkv": "video/x-matroska", "webm": "video/webm",
    "mov": "video/quicktime", "avi": "video/x-msvideo", "m4v": "video/x-m4v",
    "ts":  "video/mp2t",     "flv": "video/x-flv",     "m2ts": "video/mp2t",
    "mp3": "audio/mpeg",     "m4a": "audio/mp4",        "aac": "audio/aac",
    "ogg": "audio/ogg",      "flac": "audio/flac",      "wav": "audio/wav",
}


def register_stream_proxy(code: str, cdn_url: str, filename: str, size: int = 0):
    """Register a CDN URL under a short code so /stream/<code> can proxy it.
    Call this from main.py right before building the stream button URL."""
    entry = {"url": cdn_url, "name": filename, "size": size, "ts": time.time()}
    with _stream_registry_lock:
        _stream_registry[code] = entry
        # Purge expired entries while we're here
        now = time.time()
        expired = [k for k, v in _stream_registry.items() if now - v["ts"] > STREAM_PROXY_TTL]
        for k in expired:
            del _stream_registry[k]
    log.info("Registered stream proxy: /stream/%s -> %s…", code, cdn_url[:80])


def get_stream_public_url(code: str) -> str:
    """Return the public /stream/<code> URL — fully auto-detected.
    No env vars need to be set manually. Priority order:

    1. RENDER_EXTERNAL_HOSTNAME  — auto-set by Render
    2. RAILWAY_STATIC_URL        — auto-set by Railway
    3. KOYEB_PUBLIC_DOMAIN       — auto-set by Koyeb
    4. FLY_APP_NAME              — auto-set by Fly.io
    5. HTTP request to ipify.org — detects VPS public IP automatically
    6. Fallback: localhost
    """
    # ── Render ──────────────────────────────────────────────────────
    host = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "").strip().strip("/")
    if host:
        return f"https://{host}/stream/{code}"

    # ── Railway ─────────────────────────────────────────────────────
    railway = os.environ.get("RAILWAY_STATIC_URL", "").strip().rstrip("/")
    if railway:
        return f"https://{railway}/stream/{code}"

    # ── Koyeb ───────────────────────────────────────────────────────
    koyeb = os.environ.get("KOYEB_PUBLIC_DOMAIN", "").strip().strip("/")
    if koyeb:
        return f"https://{koyeb}/stream/{code}"

    # ── Fly.io ──────────────────────────────────────────────────────
    fly_app = os.environ.get("FLY_APP_NAME", "").strip()
    if fly_app:
        return f"https://{fly_app}.fly.dev/stream/{code}"

    # ── VPS — auto-detect public IP via ipify ───────────────────────
    # Cache the result so we don't hit ipify on every stream link
    public_ip = _get_cached_public_ip()
    if public_ip:
        return f"http://{public_ip}:{_PORT}/stream/{code}"

    # ── Fallback ────────────────────────────────────────────────────
    return f"http://127.0.0.1:{_PORT}/stream/{code}"


_cached_public_ip: str | None = None
_cached_public_ip_ts: float = 0
_IP_CACHE_TTL = 3600  # re-fetch every hour


def _get_cached_public_ip() -> str | None:
    """Fetch and cache the server's public IP via ipify.org."""
    global _cached_public_ip, _cached_public_ip_ts
    now = time.time()
    if _cached_public_ip and (now - _cached_public_ip_ts) < _IP_CACHE_TTL:
        return _cached_public_ip
    try:
        import urllib.request as _ur
        # Try multiple IP detection services in order
        for api_url in [
            "https://api.ipify.org",
            "https://api4.my-ip.io/ip",
            "https://checkip.amazonaws.com",
        ]:
            try:
                req = _ur.Request(api_url, headers={"User-Agent": "curl/7.0"})
                with _ur.urlopen(req, timeout=5) as resp:
                    ip = resp.read().decode().strip()
                    if ip and not ip.startswith("127.") and not ip.startswith("10."):
                        _cached_public_ip = ip
                        _cached_public_ip_ts = now
                        log.info("Auto-detected public IP: %s", ip)
                        return ip
            except Exception:
                continue
    except Exception as e:
        log.debug("Public IP detection failed: %s", e)
    return None


class _StreamingHandler(BaseHTTPRequestHandler):
    """HTTP handler that:
      • GET /health, HEAD /health → 200 keep-alive (unchanged)
      • GET /stream/<code>        → reverse-proxy the registered CDN URL with
                                    proper Content-Type, Content-Disposition
                                    (inline), and Range pass-through so
                                    browsers and VLC can seek inside the video.
      • HEAD /stream/<code>       → same but no body (for duration probe)
      Any other path             → 200 "alive" (backward-compat health check)
    """

    def log_message(self, *a):
        pass  # suppress per-request noise in Render logs

    def _send_error_response(self, code: int, msg: str):
        body = msg.encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_stream(self, is_head: bool):
        # Parse /stream/<code>
        path = urllib.parse.unquote(self.path.split("?")[0])
        parts = path.strip("/").split("/")
        if len(parts) < 2 or parts[0] != "stream":
            # Not a stream path — fall through to health response
            self.send_response(200)
            self.end_headers()
            if not is_head:
                self.wfile.write(b"Anujkumar alive")
            return

        code = parts[1]
        with _stream_registry_lock:
            entry = _stream_registry.get(code)

        if not entry:
            self._send_error_response(404, "Stream not found or expired.")
            return

        cdn_url  = entry["url"]
        filename = entry["name"]
        size     = entry.get("size", 0)

        # Determine Content-Type from extension
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        content_type = _MIME_MAP.get(ext, "video/mp4")

        # Pass Range header through to the CDN so seeking works
        range_header = self.headers.get("Range", "")
        req_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Referer": "https://flezen.com/",
        }
        if range_header:
            req_headers["Range"] = range_header

        try:
            import requests as _req
            upstream = _req.get(
                cdn_url, headers=req_headers, stream=True, timeout=(10, 300),
                allow_redirects=True,
            )
        except Exception as e:
            log.warning("Stream proxy fetch failed for %s: %s", code, e)
            self._send_error_response(502, f"Upstream fetch failed: {e}")
            return

        # Mirror the upstream status code (206 Partial Content if Range was used)
        self.send_response(upstream.status_code)
        self.send_header("Content-Type", content_type)
        # "inline" makes the browser play it instead of downloading
        safe_name = filename.replace('"', "").replace("\\", "")
        self.send_header(
            "Content-Disposition",
            f'inline; filename="{safe_name}"',
        )
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Cache-Control", "no-cache")

        # Forward content-length and content-range from upstream
        for hdr in ("Content-Length", "Content-Range"):
            val = upstream.headers.get(hdr)
            if val:
                self.send_header(hdr, val)
            elif hdr == "Content-Length" and size and not range_header:
                self.send_header("Content-Length", str(size))

        self.end_headers()

        if is_head:
            upstream.close()
            return

        # Stream bytes to client in 256 KB chunks
        try:
            for chunk in upstream.iter_content(chunk_size=256 * 1024):
                if chunk:
                    self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass  # client disconnected mid-stream — normal
        except Exception as e:
            log.warning("Stream proxy write error for %s: %s", code, e)
        finally:
            upstream.close()

    def do_GET(self):
        if self.path.strip("/").startswith("stream/"):
            self._handle_stream(is_head=False)
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Anujkumar alive")

    def do_HEAD(self):
        if self.path.strip("/").startswith("stream/"):
            self._handle_stream(is_head=True)
        else:
            self.send_response(200)
            self.end_headers()


def _ping_target() -> str:
    for env_name in ("PING_URL", "HEALTHCHECK_URL", "RENDER_EXTERNAL_URL", "APP_URL"):
        value = os.environ.get(env_name, "").strip()
        if value:
            return value.rstrip("/")

    render_host = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "").strip().strip("/")
    if render_host:
        return f"https://{render_host}"

    return f"http://127.0.0.1:{_PORT}"


def _ping_loop():
    target = _ping_target()
    log.info("Keep-alive ping target: %s (every %ss)", target, _PING_INTERVAL)

    while True:
        try:
            req = Request(target, method="HEAD")
            with urlopen(req, timeout=20) as resp:
                log.info("Keep-alive ping ok: %s", getattr(resp, "status", 200))
        except URLError as exc:
            log.warning("Keep-alive ping failed: %s", exc)
        except Exception as exc:
            log.warning("Keep-alive ping error: %s", exc)

        time.sleep(_PING_INTERVAL)


keep_alive = None  # alias set below


def Anujkumar_keep_alive(real_server_started: bool = False):
    """real_server_started=True means something else (Akbots/filetolink's
    server) already bound $PORT — on single-port hosts (Render/Railway/
    Replit) that's the same _PORT this would try to bind too, which would
    just fail with "Address already in use". So in that case, skip binding
    our own HTTP server entirely and only start the self-ping thread —
    that's the part that actually matters for beating Render's free-tier
    inactivity spin-down (see keep_alive.py module docstring / bot.py's
    caller comment), and it doesn't need a port of its own."""
    global _SERVER, _PING_THREAD

    with _LOCK:
        if not real_server_started and _SERVER is None:
            try:
                from http.server import ThreadingHTTPServer
                _SERVER = ThreadingHTTPServer(("0.0.0.0", _PORT), _StreamingHandler)
            except OSError as exc:
                log.warning("Health server unavailable on :%s: %s", _PORT, exc)
                # Fall through — still start the self-ping thread below even
                # though the health-check HTTP server itself didn't bind.
            else:
                thread = threading.Thread(
                    target=_SERVER.serve_forever,
                    daemon=True,
                    name="Anujkumar-health",
                )
                thread.start()

        if _PING_THREAD is None or not _PING_THREAD.is_alive():
            _PING_THREAD = threading.Thread(
                target=_ping_loop,
                daemon=True,
                name="Anujkumar-self-ping",
            )
            _PING_THREAD.start()

    if real_server_started:
        log.info("Keep-alive: reusing the already-bound port, self-ping thread started.")
    else:
        log.info("Health server on :%s", _PORT)
    return True


# Alias so bot.py can import `keep_alive` directly
keep_alive = Anujkumar_keep_alive
