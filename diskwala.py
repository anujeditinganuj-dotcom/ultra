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
    """Decrypt AES-GCM encrypted file response from Diskwala API."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    key = bytes.fromhex(ENCRYPTION_KEY)
    iv = bytes.fromhex(file_data["s"])
    ciphertext = bytes.fromhex(file_data["p"] + file_data["h"])

    aesgcm = AESGCM(key)
    plaintext = aesgcm.decrypt(iv, ciphertext, None)
    return json.loads(plaintext.decode("utf-8"))


def extract_diskwala_links(text: str) -> list[str]:
    """Extract Diskwala/Flezen URLs from text."""
    patterns = [
        r"https?://(?:www\.)?diskwala\.com/(?:app|playlist)/[A-Za-z0-9]{24}",
        # Flezen share links come as either flezen.com/<id> or
        # flezen.com/s/<id> (also seen: /share/, /f/, /v/, /d/), and the id
        # itself can contain underscores/hyphens (e.g. "daejre...w_kmczy") —
        # the old [A-Za-z0-9]+-only pattern truncated at the first "/" or "_"
        # and silently mismatched real links.
        r"https?://(?:www\.)?flezen\.com/(?:s|share|f|v|d)/[A-Za-z0-9_-]+",
        r"https?://(?:www\.)?flezen\.com/[A-Za-z0-9_-]+",
    ]
    links = []
    for pattern in patterns:
        links.extend(re.findall(pattern, text))
    links = list(dict.fromkeys(links))
    # The generic flezen.com/[id] pattern can also produce a truncated
    # partial match (e.g. "flezen.com/s") for /s/<id>-style links already
    # captured in full by the more specific pattern above — drop any match
    # that's just a prefix of a longer match we already found.
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

    if not filename.lower().endswith((".mp4", ".mkv", ".webm", ".mov", ".avi", ".zip", ".rar")):
        filename += ".mp4"

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

    return {
        "name": filename,
        "size": size,
        "downloadUrl": download_url,
        "streamUrl": stream_url or download_url,
        "thumb": None,
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
                    "https://dudadapid.diskwala.com/api/v1/file/temp_info",
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

    for _ in range(90):
        r = requests.get(status_url, headers=headers, timeout=60)
        if r.status_code in (401, 403):
            raise DiskwalaAuthError(f"Diskwala auth token rejected while polling (HTTP {r.status_code})")
        data = r.json()

        if not data.get("ok"):
            raise Exception(data.get("error", f"API Error: {data}"))

        status = data.get("status", "").lower()

        if status == "pending":
            time.sleep(2)
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

            return {
                "name": _pick(file, "name", "fileName", "filename", "title") or "video.mp4",
                "size": _pick(file, "size", "fileSize", "length") or 0,
                "downloadUrl": _pick(file, "downloadUrl", "download_url", "url", "link"),
                "streamUrl": _pick(file, "streamUrl", "stream_url", "hls") or _pick(file, "downloadUrl", "download_url", "url", "link"),
                "thumb": file.get("thumb"),
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

