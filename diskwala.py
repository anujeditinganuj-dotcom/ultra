import re
import time
import json
import html
import logging
import os
import requests
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup

logger = logging.getLogger("diskwala_bot")

HTML_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
)

# Optional: a logged-in Flezen account cookie. Without it, the Flezen
# fallback can still confirm the file exists and report its name/size, but
# cannot produce an actual download URL (Flezen only serves direct links to
# saved/logged-in accounts). Set this in .env if you have one.
FLEZEN_COOKIE = os.getenv("FLEZEN_COOKIE") or os.getenv("FLEZEN_ACCOUNT_COOKIE")


class DiskwalaAuthError(Exception):
    """Raised when the Diskwala miniapp API rejects the bearer token itself
    (HTTP 401/403) — distinct from a normal 'not found' / processing error,
    so callers know a fresh token (not a retry) is what's needed."""
    pass

API_DOWNLOAD = "https://api2.diskwala.net/api/diskwala/download"
API_STATUS = "https://api2.diskwala.net/api/diskwala/status"
ENCRYPTION_KEY = "e7109544dab612bd5b80b8a427ac474ba5541b9efff7a4ca1c8ef85df2489c23"


def _get_endpoints(link: str) -> tuple[str, str]:
    """Return (download_api, status_api) based on which service the link belongs to."""
    if "flezen.com" in link.lower():
        return (
            "https://api2.diskwala.net/api/flezen/download",
            "https://api2.diskwala.net/api/flezen/status?link=",
        )
    return (
        API_DOWNLOAD,
        API_STATUS + "?link=",
    )


def decrypt_file(file_data: dict) -> dict:
    """Decrypt AES-GCM encrypted file response from Diskwala API.

    The API has been observed using two different byte orderings:
      - Old:  ciphertext = p_hex + h_hex  (p=ciphertext, h=auth-tag)
      - New:  ciphertext = p_hex, tag appended separately as h_hex
    We try the new (ct + tag) order first; if AESGCM rejects it we fall
    back to the old (p + h) order so both API versions work.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    key = bytes.fromhex(ENCRYPTION_KEY)
    iv  = bytes.fromhex(file_data["s"])
    p   = bytes.fromhex(file_data["p"])
    h   = bytes.fromhex(file_data["h"])
    aesgcm = AESGCM(key)

    # Try new order: ct + tag
    try:
        plaintext = aesgcm.decrypt(iv, p + h, None)
        return json.loads(plaintext.decode("utf-8"))
    except Exception:
        pass

    # Fallback: old order p_hex + h_hex treated as one blob
    try:
        plaintext = aesgcm.decrypt(iv, bytes.fromhex(file_data["p"] + file_data["h"]), None)
        return json.loads(plaintext.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"AES-GCM decryption failed (tried both byte orderings): {e}") from e


def extract_diskwala_links(text: str) -> list[str]:
    """Extract Diskwala/Flezen URLs from text.

    Supports all known URL patterns:
      diskwala.com/app|playlist|d|v|f|s|share|view/<id>
      filecrush.com/...  filesadda.com/...  miniapp.diskwala.net/...
      t.me/<botname>?startapp=<id>  (Telegram Mini App deep links)
      flezen.com/s|share|f|v|d/<id>
    """
    patterns = [
        # Diskwala + aliases — all known path styles
        r"https?://(?:[\w.-]+\.)?(?:diskwala\.[a-z]{2,}|filecrush\.[a-z]{2,}|filesadda\.[a-z]{2,})"
        r"/(?:app|playlist|sharing/link|share|d|view|v|f|s)/[A-Za-z0-9_-]+\S*",
        # Diskwala Mini App (browser)
        r"https?://miniapp\.diskwala\.net/\S+",
        # Telegram Mini App deep links (t.me/<bot>?startapp=<id>)
        r"https?://t\.me/(?:sky577bot|diskwalabot|[\w]+bot)(?:/[a-zA-Z0-9_-]+)?\?(?:startapp|start)=\S+",
        # Flezen share links
        r"https?://(?:www\.)?flezen\.com/(?:s|share|f|v|d)/[A-Za-z0-9_-]+",
        r"https?://(?:www\.)?flezen\.com/[A-Za-z0-9_-]+",
    ]
    links = []
    for pattern in patterns:
        links.extend(re.findall(pattern, text))
    links = list(dict.fromkeys(links))
    # Drop prefix-only matches (e.g. "flezen.com/s" subset of "flezen.com/s/abc123")
    links = [l for l in links if not any(other != l and other.startswith(l) for other in links)]
    return links


FLEZEN_ID_RE = re.compile(
    r"flezen\.[a-z]{2,}/(?:s|share|f|v|d)/([a-zA-Z0-9_-]+)|flezen\.[a-z]{2,}/([a-zA-Z0-9_-]+)",
    re.IGNORECASE,
)


def _extract_flezen_id(link: str) -> str | None:
    m = FLEZEN_ID_RE.search(link)
    if m:
        return m.group(1) or m.group(2)
    return None


def _flezen_save_and_resolve(share_id: str, session: requests.Session) -> tuple[str | None, str | None]:
    """If a logged-in FLEZEN_COOKIE is configured, save the file to that
    account and pull the resulting direct download and stream links.

    Returns (download_url, stream_url) — matched SEPARATELY, each against
    its own keyword, rather than one combined "download|stream|file"
    regex that just took whichever of the three happened to appear first
    in the page. That's what caused the Stream button to trigger a
    forced browser download instead of playing inline: if the files
    page's "download" link came before its "stream" link in the HTML
    (the more common layout — download is usually the primary/first
    action), the old regex grabbed the download link for BOTH purposes,
    every time, regardless of a genuine stream link existing right below
    it. Falls back to whichever one link was found for both fields if
    the page only exposes one kind."""
    try:
        session.get(f"https://flezen.com/user/save?id={share_id}", allow_redirects=True, timeout=15)
        files_page = session.get("https://flezen.com/user/files", timeout=15)
        if files_page.status_code != 200:
            return None, None
        text = files_page.text

        stream_match = re.search(r"href=['\"](https?://[^'\"]*stream[^'\"]*)['\"]", text)
        download_match = re.search(r"href=['\"](https?://[^'\"]*download[^'\"]*)['\"]", text)
        if not (stream_match or download_match):
            # Neither specific keyword matched — last resort, the old
            # generic "file" pattern, same link for both.
            generic_match = re.search(r"href=['\"](https?://[^'\"]*file[^'\"]*)['\"]", text)
            if generic_match:
                return generic_match.group(1), generic_match.group(1)
            return None, None

        download_url = download_match.group(1) if download_match else None
        stream_url = stream_match.group(1) if stream_match else None
        return (download_url or stream_url), (stream_url or download_url)
    except Exception as e:
        logger.info(f"Flezen account save/resolve failed: {e}")
        return None, None


def resolve_flezen_html(link: str) -> dict:
    """Flezen-specific fallback: scrape the flezen.com share page directly.
    Gives a clear 'link deleted/expired' error when the page itself says so
    (instead of a generic API 'not found'), and — if FLEZEN_COOKIE is
    configured — resolves real download AND stream URLs via the logged-in
    account.
    """
    share_id = _extract_flezen_id(link)
    if not share_id:
        raise Exception(f"Could not extract Flezen share ID from: {link}")

    page_url = f"https://flezen.com/s/{share_id}"
    headers = {
        "User-Agent": HTML_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://flezen.com/",
    }

    session = requests.Session()
    session.headers.update(headers)
    if FLEZEN_COOKIE:
        session.headers["Cookie"] = FLEZEN_COOKIE.strip()

    r = session.get(page_url, timeout=15)
    if r.status_code == 404:
        raise Exception("This Flezen link does not exist or has been deleted by the uploader.")
    if r.status_code != 200:
        # Original share URL as given might use a different path style — retry with it directly
        r = session.get(link, timeout=15)
        if r.status_code != 200:
            raise Exception(f"Flezen returned HTTP {r.status_code}")

    page_html = r.text

    title_match = re.search(r"<h1[^>]*>(.*?)</h1>", page_html, re.DOTALL)
    if not title_match:
        title_match = re.search(
            r'<p[^>]*class=["\'][^"\']*text-gray-600 break-all[^"\']*["\'][^>]*>(.*?)</p>',
            page_html, re.DOTALL,
        )

    bytes_match = re.search(r'data-bytes=["\'](\d+)["\']', page_html)
    size = int(bytes_match.group(1)) if bytes_match else 0

    if (not title_match and not bytes_match):
        raise Exception("This Flezen link does not exist or has been deleted by the uploader.")

    if title_match:
        raw_title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip()
        filename = html.unescape(raw_title).strip()
    else:
        filename = f"flezen_{share_id}.mp4"

    if "can't find this file" in filename.lower() or "file not found" in filename.lower():
        raise Exception("This Flezen link does not exist or has been deleted by the uploader.")

    _ALL_KNOWN_EXTS = (
        ".mp4", ".mkv", ".webm", ".mov", ".avi", ".flv", ".wmv", ".m4v", ".ts", ".3gp",
        ".mp3", ".flac", ".aac", ".ogg", ".m4a", ".wav", ".opus", ".wma",
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".heic", ".avif",
        ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".txt",
        ".zip", ".rar", ".7z", ".tar", ".gz",
    )
    if not any(filename.lower().endswith(ext) for ext in _ALL_KNOWN_EXTS):
        filename += ".mp4"

    # Extract upload_date and views (from TeraBox-Video-Downloader project)
    upload_date = None
    dt_match = re.search(r'data-datetime=["\']([^"\']+)["\']', page_html)
    if dt_match:
        upload_date = dt_match.group(1).strip()

    views = None
    views_match = re.search(
        r'<i class=["\']ri-eye-line["\'][^>]*>.*?<p class=["\']text-gray-600["\']>(\d+)</p>',
        page_html, re.DOTALL
    )
    if views_match:
        views = int(views_match.group(1))

    download_url = None
    stream_url = None
    if FLEZEN_COOKIE:
        download_url, stream_url = _flezen_save_and_resolve(share_id, session)

    if not download_url:
        raise Exception(
            f"Flezen file '{filename}' ({size} bytes) exists, but a direct download URL "
            f"could not be obtained — Flezen only serves direct links to a logged-in account. "
            f"Set FLEZEN_COOKIE in .env to enable this."
        )

    logger.info(f"Flezen HTML fallback resolved: {filename} -> download={download_url[:120]} stream={(stream_url or download_url)[:120]}")

    # Extension from filename
    _flezen_ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else None

    # Category from extension
    _V = {"mp4","mkv","avi","mov","webm","flv","wmv","m4v","ts","3gp","m2ts","vob"}
    _A = {"mp3","flac","aac","ogg","m4a","wav","opus","wma","alac","aiff"}
    _I = {"jpg","jpeg","png","gif","webp","bmp","tiff","tif","svg","heic","heif","avif"}
    _D = {"pdf","doc","docx","ppt","pptx","xls","xlsx","txt","zip","rar","7z","tar","gz"}
    _flezen_cat = ("Video"    if _flezen_ext in _V else
                   "Audio"    if _flezen_ext in _A else
                   "Photo"    if _flezen_ext in _I else
                   "Document" if _flezen_ext in _D else None)

    return {
        "name":        filename,
        "extension":   _flezen_ext,
        "category":    _flezen_cat,
        "size":        size,
        "downloadUrl": download_url,
        "streamUrl":   stream_url or download_url,
        "thumb":       None,
        "upload_date": upload_date,
        "views":       views,
    }


def resolve_diskwala_html(link: str) -> dict:
    """Fallback resolver: scrape the Diskwala/Flezen share page directly for
    a video URL, bypassing the bearer-token miniapp API entirely. Used when
    api2.diskwala.net returns an error (e.g. 404 "not found") for a link
    that otherwise loads fine in a browser.
    """
    headers = {
        "User-Agent": HTML_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.diskwala.com/",
        "Origin": "https://www.diskwala.com",
    }

    r = requests.get(link, headers=headers, timeout=30, allow_redirects=True)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    name = None
    thumb = None
    download_url = None

    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        name = og_title["content"].strip()

    og_image = soup.find("meta", property="og:image")
    if og_image and og_image.get("content"):
        thumb = urljoin(link, og_image["content"].strip())

    # <video>/<source> tags
    for tag in soup.find_all(["video", "source"]):
        src = tag.get("src")
        if src:
            download_url = urljoin(link, src.strip().strip("\"'"))
            break

    # Embedded JSON / JS blobs
    if not download_url:
        json_url_patterns = [
            r'"downloadUrl"\s*:\s*"([^"]+)"',
            r'"download_url"\s*:\s*"([^"]+)"',
            r'"directUrl"\s*:\s*"([^"]+)"',
            r'"direct_url"\s*:\s*"([^"]+)"',
            r'"contentUrl"\s*:\s*"([^"]+)"',
            r'"content_url"\s*:\s*"([^"]+)"',
        ]
        for script in soup.find_all("script"):
            content = script.string
            if not content:
                continue
            matched = False
            for pattern in json_url_patterns:
                m = re.search(pattern, content)
                if m:
                    download_url = urljoin(link, m.group(1))
                    matched = True
                    break
            if not matched:
                m2 = re.search(r'(https?://[^\s"\'<>]+\.(?:mp4|mkv|webm|m3u8)[^\s"\'<>]*)', content)
                if m2:
                    download_url = m2.group(1)
                    matched = True
            if matched:
                break

    # Last resort: raw media-URL scan of the full page text
    if not download_url:
        m3 = re.search(r'(https?://[^\s"\'<>]+\.(?:mp4|mkv|webm|m3u8)[^\s"\'<>]*)', html)
        if m3:
            download_url = m3.group(1)

    # Last-ditch: internal metadata endpoint discovered via reverse engineering.
    # Only ever attempted after every HTML-based method above has failed.
    if not download_url:
        file_id_match = re.search(r"diskwala\.com/app/([A-Za-z0-9]+)", link)
        if file_id_match:
            try:
                api_resp = requests.post(
                    # Was "dudadapid.diskwala.com" — that host no longer
                    # resolves at all (NameResolutionError in production
                    # logs), and appears to have been a stale/wrong
                    # domain to begin with. api2.diskwala.net is the
                    # confirmed-working host for every other Diskwala
                    # endpoint in this file (API_DOWNLOAD/API_STATUS
                    # above), so this now targets that instead — same
                    # path, since there's no evidence the path itself
                    # was ever wrong, only the domain.
                    "https://api2.diskwala.net/api/v1/file/temp_info",
                    json={"id": file_id_match.group(1)},
                    headers=headers, timeout=15,
                )
                if api_resp.status_code == 200:
                    data = api_resp.json()
                    payload = data.get("data") if isinstance(data.get("data"), dict) else data
                    for key in ("downloadUrl", "download_url", "url", "video_url", "streamUrl"):
                        if payload.get(key):
                            download_url = payload[key]
                            break
            except Exception as e:
                logger.info(f"HTML-fallback metadata endpoint also failed: {e}")

    if not download_url:
        raise Exception("not found (HTML fallback also found no media URL)")

    if not name:
        fn_match = re.search(r"/([^/?#]+?)(?:\?|#|$)", download_url)
        name = fn_match.group(1) if fn_match else "video.mp4"
    if "." not in name:
        ext_match = re.search(r"\.([a-zA-Z0-9]{2,5})(?:\?|#|$)", download_url)
        name += "." + ext_match.group(1) if ext_match else ".mp4"

    logger.info(f"HTML fallback resolved: {name} -> {download_url[:120]}")

    return {
        "name": name,
        "size": 0,
        "downloadUrl": download_url,
        "streamUrl": download_url,
        "thumb": thumb,
    }


def fetch_diskwala_video(link: str, auth: str) -> dict:
    """Fetch video info, routing Flezen and Diskwala links differently:

    - Flezen links go straight to resolve_flezen_html() FIRST — scraping
      flezen.com directly, not through Diskwala's infrastructure at all.
      The token-API path only reaches Flezen via _get_endpoints()'s
      api2.diskwala.net/api/flezen/* proxy of the same site, which is a
      second-hand route through someone else's infra rather than the
      site itself — trying that first meant Flezen links almost always
      fell through to the (correct, direct) HTML path anyway, just after
      a wasted round-trip and a delay every time. Only falls back to the
      token-API/generic HTML path if the direct scrape itself fails, so
      there's still a fallback — it's just no longer tried first.

    - Diskwala links are unaffected — still token-API first, with the
      existing HTML-scrape fallback if that fails, same as before."""
    if "flezen." in link.lower():
        try:
            return resolve_flezen_html(link)
        except Exception as flezen_error:
            logger.warning(f"Flezen direct HTML fetch failed ({flezen_error}), trying token-API fallback...")
            try:
                return _fetch_diskwala_video_via_api(link, auth)
            except Exception as api_error:
                logger.warning(f"Token-API fallback also failed ({api_error}), trying generic HTML fallback...")
                try:
                    return resolve_diskwala_html(link)
                except Exception:
                    # All three attempts failed — the direct Flezen-
                    # specific error (dead link / needs FLEZEN_COOKIE) is
                    # more useful to the user than the generic API error.
                    raise flezen_error

    try:
        return _fetch_diskwala_video_via_api(link, auth)
    except Exception as api_error:
        logger.warning(f"Token-API fetch failed ({api_error}), trying HTML fallback...")
        try:
            return resolve_diskwala_html(link)
        except Exception as html_error:
            logger.warning(f"HTML fallback also failed: {html_error}")
            raise api_error


def _fetch_diskwala_video_via_api(link: str, auth: str) -> dict:
    """Original bearer-token miniapp API path."""
    headers = {
        "Authorization": f"Bearer {auth}",
        "X-Bot-Id": "diskwala",
        "Content-Type": "application/json",
        "Origin": "https://miniapp.diskwala.net",
        "Referer": "https://miniapp.diskwala.net/",
        "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36",
    }

    download_api, status_api_prefix = _get_endpoints(link)

    logger.info(f"Calling API: {download_api}")

    r = requests.post(download_api, headers=headers, json={"link": link}, timeout=60)
    logger.info(f"Download response: {r.status_code} - {r.text[:200]}")
    if r.status_code in (401, 403):
        raise DiskwalaAuthError(f"Diskwala auth token rejected (HTTP {r.status_code})")
    data = r.json()

    if not data.get("ok"):
        raise Exception(data.get("error", f"API Error: {data}"))

    status_url = status_api_prefix + quote(link, safe="")

    poll_interval = 0.5   # adaptive backoff: 0.5s → 1s → 2s (capped)
    for _ in range(90):
        r = requests.get(status_url, headers=headers, timeout=60)
        if r.status_code in (401, 403):
            raise DiskwalaAuthError(f"Diskwala auth token rejected while polling (HTTP {r.status_code})")
        data = r.json()

        if not data.get("ok"):
            raise Exception(data.get("error", f"API Error: {data}"))

        status = data.get("status", "").lower()

        if status == "pending":
            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, 2.0)
            continue

        if status == "done":
            file = data.get("file")
            if not file:
                raise Exception(f"No file returned: {data}")

            # Decrypt if encrypted
            if file.get("_x"):
                logger.info("File is encrypted, decrypting...")
                file = decrypt_file(file)
                logger.info(f"Decrypted file: {json.dumps(file)[:300]}")

            def _pick(d, *keys):
                for k in keys:
                    if k in d and d[k] not in (None, ""):
                        return d[k]
                return None

            def _parse_duration(raw):
                """Parse duration from int (seconds) or 'HH:MM:SS' string."""
                if raw is None:
                    return None
                try:
                    if isinstance(raw, str) and ":" in raw:
                        parts = [int(p) for p in raw.split(":")]
                        return sum(p * 60 ** i for i, p in enumerate(reversed(parts)))
                    return int(float(raw))
                except (ValueError, TypeError):
                    return None

            # Also check top-level data dict for metadata (some API versions
            # put creator/duration at root level, not inside 'file')
            meta_src = {**file, **{k: v for k, v in data.items()
                                   if k not in ("file", "ok", "status")}}

            # Extension — present in decrypted API response as "extension": "mp4"
            raw_name_api = _pick(file, "name", "fileName", "filename", "title") or "video.mp4"
            api_ext = _pick(file, "extension", "ext", "fileExtension", "file_extension")
            if not api_ext and "." in raw_name_api:
                api_ext = raw_name_api.rsplit(".", 1)[-1].lower()
            api_ext = api_ext.lower().strip(".") if api_ext else None

            # Category — infer from extension if API doesn't provide it
            _VIDEO_EXTS = {"mp4", "mkv", "avi", "mov", "webm", "flv", "wmv", "m4v", "ts", "3gp", "m2ts", "vob"}
            _AUDIO_EXTS = {"mp3", "flac", "aac", "ogg", "m4a", "wav", "opus", "wma", "alac", "aiff"}
            _IMAGE_EXTS = {"jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "tif", "svg", "heic", "heif", "avif"}
            _DOC_EXTS   = {"pdf", "doc", "docx", "ppt", "pptx", "xls", "xlsx", "txt", "zip", "rar", "7z", "tar", "gz"}
            api_category = _pick(meta_src, "category", "genre", "tag", "type")
            if not api_category and api_ext:
                if api_ext in _VIDEO_EXTS:
                    api_category = "Video"
                elif api_ext in _AUDIO_EXTS:
                    api_category = "Audio"
                elif api_ext in _IMAGE_EXTS:
                    api_category = "Photo"
                elif api_ext in _DOC_EXTS:
                    api_category = "Document"

            return {
                "name":             raw_name_api,
                "extension":        api_ext,
                "size":             _pick(file, "size", "fileSize", "length") or 0,
                "downloadUrl":      _pick(file, "downloadUrl", "download_url", "url", "link"),
                "streamUrl":        _pick(file, "streamUrl", "stream_url", "hls")
                                    or _pick(file, "downloadUrl", "download_url", "url", "link"),
                "thumb":            _pick(meta_src, "thumb", "thumbnail", "thumbnailUrl", "poster", "image"),
                # ── Extra metadata from API response ───────────────────────
                "creator":          _pick(meta_src, "creator", "uploader", "author", "uploaderName",
                                          "uploader_name", "channel", "creatorName", "username"),
                "duration_seconds": _parse_duration(
                                        _pick(meta_src, "duration", "duration_seconds",
                                              "durationSeconds", "video_duration", "length_seconds")
                                    ),
                "views":            _pick(meta_src, "views", "view_count", "viewCount",
                                          "watchCount", "watch_count"),
                "likes":            _pick(meta_src, "likes", "like_count", "likeCount"),
                "description":      _pick(meta_src, "description", "desc", "caption", "details"),
                "upload_date":      _pick(meta_src, "upload_date", "uploadDate", "created_at",
                                          "createdAt", "date", "uploadedAt"),
                "category":         api_category,
            }

        raise Exception(f"Unexpected status: {status} - {data}")

    raise Exception("Timeout waiting for Diskwala API response")

# ─────────────────────────────────────────────────────────────────────────────
#  PLAYLIST SUPPORT
# ─────────────────────────────────────────────────────────────────────────────

API_PLAYLIST_INFO = "https://api2.diskwala.net/api/diskwala/playlist"

PLAYLIST_ID_RE = re.compile(
    r"diskwala\.com/playlist/([A-Za-z0-9]{24})",
    re.IGNORECASE,
)


def extract_playlist_id(url: str) -> str | None:
    m = PLAYLIST_ID_RE.search(url)
    return m.group(1) if m else None


def is_playlist_link(url: str) -> bool:
    return bool(PLAYLIST_ID_RE.search(url))


def fetch_playlist_info(playlist_url: str, auth: str) -> dict:
    """Fetch playlist metadata + list of file links from Diskwala API.
    Returns:
      {
        "title": str,
        "thumb": str | None,
        "files": [{"name": str, "link": str}, ...]
      }
    """
    playlist_id = extract_playlist_id(playlist_url)
    if not playlist_id:
        raise Exception(f"Could not extract playlist ID from: {playlist_url}")

    headers = {
        "Authorization": f"Bearer {auth}",
        "X-Bot-Id": "diskwala",
        "Content-Type": "application/json",
        "Origin": "https://miniapp.diskwala.net",
        "Referer": "https://miniapp.diskwala.net/",
        "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36",
    }

    # Try POST with link first (same pattern as single file)
    try:
        r = requests.post(
            API_PLAYLIST_INFO,
            headers=headers,
            json={"link": playlist_url},
            timeout=30,
        )
        if r.status_code in (401, 403):
            raise DiskwalaAuthError(f"Auth token rejected (HTTP {r.status_code})")
        data = r.json()
        if data.get("ok"):
            return _parse_playlist_response(data, playlist_url)
    except DiskwalaAuthError:
        raise
    except Exception as e:
        logger.info(f"Playlist POST failed: {e}, trying GET...")

    # Try GET with playlist id
    r = requests.get(
        f"{API_PLAYLIST_INFO}/{playlist_id}",
        headers=headers,
        timeout=30,
    )
    if r.status_code in (401, 403):
        raise DiskwalaAuthError(f"Auth token rejected (HTTP {r.status_code})")
    data = r.json()
    if not data.get("ok"):
        # Last resort: scrape the playlist page HTML
        return _scrape_playlist_html(playlist_url)

    return _parse_playlist_response(data, playlist_url)


def _parse_playlist_response(data: dict, playlist_url: str) -> dict:
    """Parse API response into standard playlist dict."""
    payload = data.get("data") or data.get("playlist") or data
    if isinstance(payload, list):
        # Some APIs return a bare list of files
        files_raw = payload
        title = "Diskwala Playlist"
        thumb = None
    else:
        title = payload.get("title") or payload.get("name") or "Diskwala Playlist"
        thumb = payload.get("thumb") or payload.get("thumbnail")
        files_raw = (
            payload.get("files") or payload.get("items") or
            payload.get("videos") or payload.get("links") or []
        )

    files = []
    for f in files_raw:
        if isinstance(f, str):
            # bare URL
            files.append({"name": f.split("/")[-1] or "video.mp4", "link": f})
        elif isinstance(f, dict):
            link = (
                f.get("link") or f.get("url") or f.get("downloadUrl") or
                f.get("shareLink") or f.get("appLink") or ""
            )
            name = f.get("name") or f.get("title") or f.get("fileName") or link.split("/")[-1] or "video"
            if link:
                files.append({"name": name, "link": link})

    if not files:
        raise Exception("Playlist API returned no files")

    return {"title": title, "thumb": thumb, "files": files}


def _scrape_playlist_html(playlist_url: str) -> dict:
    """HTML fallback: scrape playlist page for app links."""
    headers = {
        "User-Agent": HTML_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.diskwala.com/",
    }
    r = requests.get(playlist_url, headers=headers, timeout=30, allow_redirects=True)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    title_tag = soup.find("h1") or soup.find("meta", property="og:title")
    title = (
        (title_tag.get("content") if title_tag and title_tag.name == "meta" else
         title_tag.get_text(strip=True) if title_tag else None)
        or "Diskwala Playlist"
    )

    thumb_tag = soup.find("meta", property="og:image")
    thumb = thumb_tag.get("content") if thumb_tag else None

    # Find all /app/ links in the page
    file_links = []
    seen = set()
    for a in soup.find_all("a", href=re.compile(r"/app/[A-Za-z0-9]{24}")):
        href = a.get("href", "")
        if not href.startswith("http"):
            href = urljoin("https://www.diskwala.com", href)
        if href not in seen:
            seen.add(href)
            name = a.get_text(strip=True) or href.split("/")[-1]
            file_links.append({"name": name, "link": href})

    # Also scan script tags for JSON with app links
    if not file_links:
        for script in soup.find_all("script"):
            text = script.string or ""
            for m in re.finditer(r'diskwala\.com/app/([A-Za-z0-9]{24})', text):
                link = f"https://www.diskwala.com/app/{m.group(1)}"
                if link not in seen:
                    seen.add(link)
                    file_links.append({"name": f"video_{m.group(1)[:8]}.mp4", "link": link})

    if not file_links:
        raise Exception("Could not find any video links in playlist page")

    return {"title": title, "thumb": thumb, "files": file_links}


# ─────────────────────────────────────────────────────────────────────────────
#  PREVIEW SCRAPER — title, author, duration, thumb from HTML (no auth needed)
# ─────────────────────────────────────────────────────────────────────────────

# Generic/site-level titles to ignore
_GENERIC_TITLES = {
    "diskwala", "flezen", "free unlimited cloud", "cloud storage",
    "creator platform", "upload files", "share with",
}

def _is_generic_title(t: str) -> bool:
    if not t:
        return True
    tl = t.lower()
    return any(g in tl for g in _GENERIC_TITLES)


def _scrape_preview_from_html(link: str, html_text: str) -> dict:
    """Extract title/author/duration/thumb.
    Priority: __NEXT_DATA__ > JSON-LD > og:meta > page text.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    title = None
    author = None
    duration_seconds = 0
    thumb = None
    size = 0

    # 1. __NEXT_DATA__ (Next.js) — most reliable for Diskwala
    next_script = soup.find("script", id="__NEXT_DATA__")
    if next_script and next_script.string:
        try:
            nd = json.loads(next_script.string)
            page_props = nd.get("props", {}).get("pageProps", {})
            file_data = (
                page_props.get("file")
                or page_props.get("video")
                or page_props.get("data")
                or page_props.get("fileData")
                or {}
            )
            if not file_data:
                for v in page_props.values():
                    if isinstance(v, dict) and (v.get("name") or v.get("title")):
                        file_data = v
                        break

            nd_title = (file_data.get("title") or file_data.get("name") or file_data.get("fileName"))
            if nd_title and not _is_generic_title(nd_title):
                title = str(nd_title).strip()

            nd_author = (
                file_data.get("creator") or file_data.get("author")
                or file_data.get("uploader") or file_data.get("username")
                or file_data.get("creatorName")
                or (file_data.get("user") or {}).get("username")
                or (file_data.get("user") or {}).get("name")
            )
            if nd_author and isinstance(nd_author, str):
                author = nd_author.strip()

            nd_dur = file_data.get("duration") or file_data.get("videoDuration")
            if nd_dur:
                try:
                    duration_seconds = int(float(nd_dur))
                except Exception:
                    pass

            nd_thumb = (
                file_data.get("thumb") or file_data.get("thumbnail")
                or file_data.get("coverImage") or file_data.get("poster")
            )
            if nd_thumb:
                thumb = urljoin(link, str(nd_thumb))

            nd_size = file_data.get("size") or file_data.get("fileSize")
            if nd_size:
                try:
                    size = int(nd_size)
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"__NEXT_DATA__ parse failed: {e}")

    # 2. JSON-LD: fill gaps
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            if not title:
                ld_t = data.get("name") or data.get("headline")
                if ld_t and not _is_generic_title(ld_t):
                    title = str(ld_t).strip()
            if not author:
                ao = data.get("author") or data.get("creator") or data.get("uploadedBy")
                if ao:
                    author = (ao.get("name") if isinstance(ao, dict) else str(ao)).strip()
            if not duration_seconds:
                dur_str = data.get("duration")
                if dur_str:
                    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", str(dur_str))
                    if m:
                        h, mi, s = (int(x or 0) for x in m.groups())
                        duration_seconds = h * 3600 + mi * 60 + s
            if not thumb:
                img = data.get("thumbnailUrl") or data.get("image")
                if img:
                    thumb = urljoin(link, img if isinstance(img, str) else img[0])
        except Exception:
            pass

    # 3. og:image for thumb (og:title skipped — usually generic on Diskwala)
    if not thumb:
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            thumb = urljoin(link, og_img["content"].strip())

    # 4. Flezen: data-bytes + h1
    if not size:
        bm = re.search(r"data-bytes=[\x27\"](\d+)[\x27\"]", html_text)
        if bm:
            size = int(bm.group(1))
    if not title:
        h1 = soup.find("h1")
        if h1:
            t = h1.get_text(strip=True)
            if not _is_generic_title(t):
                title = t

    # 5. Author / duration from raw JSON blobs in page scripts
    if not author:
        for pat in [
            r'"creatorName"\s*:\s*"([^"]+)"',
            r'"username"\s*:\s*"([^"]+)"',
            r'"creator"\s*:\s*"([^"]+)"',
            r'"author"\s*:\s*"([^"]+)"',
            r'"uploader"\s*:\s*"([^"]+)"',
        ]:
            am = re.search(pat, html_text)
            if am:
                c = am.group(1).strip()
                if c.lower() not in ("admin","system","bot","diskwala","flezen","null","undefined"):
                    author = c
                    break

    if not duration_seconds:
        dm = re.search(r'"duration"\s*:\s*(\d+)', html_text)
        if dm:
            duration_seconds = int(dm.group(1))

    return {"title": title, "author": author, "duration_seconds": duration_seconds, "thumb": thumb, "size": size}


def fetch_diskwala_temp_info(link: str) -> dict:
    """Calls the same api2.diskwala.net/api/v1/file/temp_info
    endpoint already used elsewhere in this file (as a last-resort
    download-URL fallback in resolve_diskwala_html) — same request shape,
    just reading more of the payload this time: creator, duration, view/
    like counts, description, upload date, category, size, and thumbnail,
    not just the media URL.

    This is an undocumented internal endpoint (found via reverse
    engineering, not an official API) with no confirmed field-naming —
    each attribute below tries several plausible key spellings the way
    the existing downloadUrl/download_url/url/... fallback chain already
    does, since that's the only naming convention evidence available for
    this endpoint from within this codebase. Fields the response doesn't
    have (or that don't match any of the tried spellings) just come back
    None — callers already treat any None field as "unknown", same as
    the HTML-scrape path.

    Returns a dict with keys: creator, duration_seconds, views, likes,
    description, upload_date, category, size, thumb. Returns all-None on
    any failure (link doesn't look like a diskwala.com/app/<id> link,
    network error, non-200, unexpected JSON shape, etc.) rather than
    raising — this is meant to be a best-effort enrichment layered on
    top of fetch_diskwala_preview's existing HTML scrape, not something
    that should ever block showing what the HTML scrape already found."""
    empty = {"creator": None, "duration_seconds": None, "views": None, "likes": None,
             "description": None, "upload_date": None, "category": None,
             "size": None, "thumb": None}

    file_id_match = re.search(r"diskwala\.com/app/([A-Za-z0-9]+)", link)
    if file_id_match:
        file_id = file_id_match.group(1)
    else:
        # Flezen shares the same api2.diskwala.net backend as Diskwala
        # (see _get_endpoints() above) — worth trying its share id here
        # too rather than assuming this endpoint is Diskwala-only just
        # because that's the only pattern this file's other temp_info
        # call site (in resolve_diskwala_html) happened to check for.
        file_id = _extract_flezen_id(link)
    if not file_id:
        return empty

    headers = {
        "User-Agent": HTML_USER_AGENT,
        "Accept": "application/json",
        "Referer": "https://www.diskwala.com/",
        "Origin": "https://www.diskwala.com",
    }

    # Try multiple endpoints — api2.diskwala.net has several info paths,
    # and which one works depends on the file type and API version.
    # temp_info returns 404 for some files; info/meta/file_info may work instead.
    _INFO_ENDPOINTS = [
        ("POST", "https://api2.diskwala.net/api/v1/file/temp_info", {"id": file_id}),
        ("POST", "https://api2.diskwala.net/api/v1/file/info",      {"id": file_id}),
        ("POST", "https://api2.diskwala.net/api/v1/file/meta",      {"id": file_id}),
        ("GET",  f"https://api2.diskwala.net/api/v1/file/{file_id}", None),
        ("POST", "https://api2.diskwala.net/api/diskwala/file_info", {"id": file_id}),
    ]

    data = None
    for method, endpoint, body in _INFO_ENDPOINTS:
        try:
            if method == "POST":
                resp = requests.post(endpoint, json=body, headers=headers, timeout=15)
            else:
                resp = requests.get(endpoint, headers=headers, timeout=15)

            if resp.status_code == 200:
                data = resp.json()
                break
            else:
                logger.info(f"temp_info returned status {resp.status_code} for {endpoint[:60]}")
        except Exception as e:
            logger.info(f"temp_info call failed for {endpoint[:40]}: {e}")

    if data is None:
        return empty

    payload = data.get("data") if isinstance(data.get("data"), dict) else data

    def _first(*keys):
        for key in keys:
            val = payload.get(key)
            if val not in (None, ""):
                return val
        return None

    duration_raw = _first("duration", "duration_seconds", "durationSeconds", "length", "video_duration")
    duration_seconds = None
    if duration_raw is not None:
        try:
            # Some of these endpoints hand back "HH:MM:SS" instead of a
            # raw number — this covers both without needing to know in
            # advance which shape this particular response used.
            if isinstance(duration_raw, str) and ":" in duration_raw:
                parts = [int(p) for p in duration_raw.split(":")]
                duration_seconds = sum(p * 60 ** i for i, p in enumerate(reversed(parts)))
            else:
                duration_seconds = int(float(duration_raw))
        except (ValueError, TypeError):
            duration_seconds = None

    return {
        "creator": _first("creator", "uploader", "author", "uploaderName", "uploader_name", "channel"),
        "duration_seconds": duration_seconds,
        "views": _first("views", "view_count", "viewCount", "watchCount", "watch_count"),
        "likes": _first("likes", "like_count", "likeCount"),
        "description": _first("description", "desc", "caption"),
        "upload_date": _first("upload_date", "uploadDate", "created_at", "createdAt", "date"),
        "category": _first("category", "genre", "tag", "type"),
        "size": _first("size", "fileSize", "file_size"),
        "thumb": _first("thumbnail", "thumb", "thumbnailUrl", "thumbnail_url", "poster", "image"),
    }


def fetch_diskwala_preview(link: str) -> dict:
    """Scrape Diskwala OR Flezen share page to get title, author, duration,
    and thumbnail WITHOUT needing a bearer token.  Returns a dict with keys:
        title, author, duration_seconds, thumb, size,
        views, likes, description, upload_date, category
    Any field that can't be found is None / 0.

    The last five keys come from fetch_diskwala_temp_info() (Option B —
    the api2.diskwala.net file-info endpoint) layered on top of this
    function's own HTML scrape below — that scrape has no way to surface
    view/like counts, a description, upload date, or category at all, so
    those five are always temp_info's alone; author/duration_seconds/
    thumb/size only get overwritten by temp_info's version if the HTML
    scrape came back empty for that particular field.
    """
    headers = {
        "User-Agent": HTML_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    # Route Flezen links to their own page URL
    if "flezen" in link.lower():
        share_id = _extract_flezen_id(link)
        if share_id:
            link = f"https://flezen.com/s/{share_id}"
        headers["Referer"] = "https://flezen.com/"
    else:
        headers["Referer"] = "https://www.diskwala.com/"

    try:
        r = requests.get(link, headers=headers, timeout=20, allow_redirects=True)
        r.raise_for_status()
        html_text = r.text
        result = _scrape_preview_from_html(link, html_text)
    except Exception as e:
        logger.warning(f"fetch_diskwala_preview GET failed: {e}")
        result = {"title": None, "author": None, "duration_seconds": 0, "thumb": None, "size": 0}

    temp_info = fetch_diskwala_temp_info(link)
    result["views"] = temp_info["views"]
    result["likes"] = temp_info["likes"]
    result["description"] = temp_info["description"]
    result["upload_date"] = temp_info["upload_date"]
    result["category"] = temp_info["category"]
    if not result.get("author"):
        result["author"] = temp_info["creator"]
    if not result.get("duration_seconds"):
        result["duration_seconds"] = temp_info["duration_seconds"] or 0
    if not result.get("thumb"):
        result["thumb"] = temp_info["thumb"]
    if not result.get("size"):
        result["size"] = temp_info["size"] or 0

    logger.info(f"Preview scraped ({link[:60]}): title={result['title']!r} author={result['author']!r} dur={result['duration_seconds']}s")
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  AUTO FLEZEN COOKIE GENERATOR
#  (ported from TeraBox-Video-Downloader/scripts/auto_flezen_cookie.py)
#
#  Creates a disposable mail.tm inbox → registers on Flezen → verifies email
#  → completes onboarding → returns a ready-to-use cookie string.
#
#  Used by the /refresh_flezen_cookie admin command in main.py so that
#  FLEZEN_COOKIE never has to be refreshed manually.
# ─────────────────────────────────────────────────────────────────────────────

def generate_flezen_cookie() -> tuple[str, str] | None:
    """
    Fully automated Flezen account creation + cookie extraction.

    Returns (cookie_string, email) on success, None on failure.

    Flow:
      1. Create disposable mailbox via mail.tm
      2. Register on flezen.com with that email
      3. Poll for verification email, extract token
      4. Verify account, complete onboarding
      5. Extract + return session cookie string
    """
    import random, string

    session = requests.Session()
    session.headers.update({
        "User-Agent": HTML_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    # ── Step 1: Disposable mailbox ──────────────────────────────────────────
    try:
        dom_res = requests.get("https://api.mail.tm/domains", timeout=10)
        domains = [d["domain"] for d in dom_res.json().get("hydra:member", []) if d.get("isActive")]
        if not domains:
            logger.error("[flezen-cookie] No active mail.tm domains available")
            return None
        domain = domains[0]
    except Exception as e:
        logger.error(f"[flezen-cookie] mail.tm domain fetch failed: {e}")
        return None

    rand_id  = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    email    = f"fbot_{rand_id}@{domain}"
    password = "P@ss" + "".join(random.choices(string.digits, k=8))

    logger.info(f"[flezen-cookie] Creating mailbox: {email}")
    try:
        requests.post("https://api.mail.tm/accounts",
                      json={"address": email, "password": password}, timeout=10)
        tok_res    = requests.post("https://api.mail.tm/token",
                                   json={"address": email, "password": password}, timeout=10)
        mail_token = tok_res.json().get("token")
        if not mail_token:
            logger.error("[flezen-cookie] Could not get mail.tm token")
            return None
        mail_headers = {"Authorization": f"Bearer {mail_token}"}
    except Exception as e:
        logger.error(f"[flezen-cookie] Mailbox creation failed: {e}")
        return None

    # ── Step 2: Register on Flezen ──────────────────────────────────────────
    session.headers.update({"Referer": "https://flezen.com/auth/register",
                             "Origin": "https://flezen.com"})
    logger.info("[flezen-cookie] Registering on flezen.com...")
    try:
        reg = session.post("https://flezen.com/auth/register",
                           data={"email": email, "password": password,
                                 "confirm_password": password}, timeout=15)
        if reg.status_code not in (200, 201, 302):
            logger.error(f"[flezen-cookie] Registration failed: HTTP {reg.status_code}")
            return None
    except Exception as e:
        logger.error(f"[flezen-cookie] Registration request failed: {e}")
        return None

    # ── Step 3: Poll for verification email ────────────────────────────────
    logger.info("[flezen-cookie] Waiting for verification email...")
    verify_url = None
    for attempt in range(20):
        time.sleep(3)
        try:
            msgs = requests.get("https://api.mail.tm/messages",
                                headers=mail_headers, timeout=10).json()
            for msg in msgs.get("hydra:member", []):
                msg_body = requests.get(f"https://api.mail.tm/messages/{msg['id']}",
                                        headers=mail_headers, timeout=10).json()
                body = msg_body.get("text", "") or msg_body.get("html", "")
                m = re.search(r"https?://flezen\.com/auth/verify\?token=([a-zA-Z0-9_-]+)", body)
                if m:
                    verify_url = m.group(0)
                    break
        except Exception:
            pass
        if verify_url:
            break

    if not verify_url:
        logger.error("[flezen-cookie] Timed out waiting for verification email")
        return None

    # ── Step 4: Verify + onboard ────────────────────────────────────────────
    logger.info(f"[flezen-cookie] Verifying: {verify_url}")
    try:
        session.get(verify_url, allow_redirects=True, timeout=15)
    except Exception as e:
        logger.warning(f"[flezen-cookie] Verify request failed: {e}")

    logger.info("[flezen-cookie] Completing onboarding...")
    session.headers.update({"Referer": "https://flezen.com/user/onboard",
                             "Origin": "https://flezen.com"})
    try:
        session.post("https://flezen.com/user/onboard", data={
            "first_name": "Bot", "last_name": "User",
            "display_name": f"BotUser_{rand_id}",
            "ref_code": "", "traffic_sources": "https://t.me/",
        }, allow_redirects=False, timeout=15)
    except Exception as e:
        logger.warning(f"[flezen-cookie] Onboarding failed (non-fatal): {e}")

    # ── Step 5: Extract cookies ─────────────────────────────────────────────
    cookies = session.cookies.get_dict()
    if not cookies:
        logger.error("[flezen-cookie] No cookies after registration — likely blocked or captcha")
        return None

    cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
    logger.info(f"[flezen-cookie] ✅ Cookie generated for {email}")
    return cookie_str, email
