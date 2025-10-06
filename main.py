# main.py
import asyncio
import aiofiles
import json
import logging
import os
import re
import time
import uuid
import tempfile
import glob
from typing import Optional

import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, Security
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyQuery
import uvicorn

# Google Drive client libs
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# -------------------- CONFIG --------------------
CLIENT_SECRET_PATH = "client_secret.json"
TOKEN_PATH = "token.json"
DRIVE_CACHE_PATH = "drive_cache.json"
METADATA_DRIVE_FILENAME = "api_metadata.json"
DRIVE_FOLDER_ID = None
SCOPES = ['https://www.googleapis.com/auth/drive.file']

API_KEY = "ShrutiMusic"
api_key_query = APIKeyQuery(name="api_key", auto_error=True)

# -------------------- Logging --------------------
logging.basicConfig(
    format="%(asctime)s [%(name)s]:: %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logs = logging.getLogger("main")

# -------------------- App & Stores --------------------
app = FastAPI()
database = {}       # active stream entries: stream_id -> {file_url, file_name, created_time}
cache_db = {}       # cached metadata responses
ip_address_cache = {}

# chunk size for streaming
CHUNK_SIZE = 1024 * 1024

# -------------------- Helpers --------------------
def extract_video_id(query: str) -> str:
    """Extract YouTube ID or return input if looks like ID."""
    if re.match(r'^[A-Za-z0-9_-]{11}$', query):
        return query
    patterns = [
        r'(?:v=|/(?:embed|v|shorts|live)/|youtu.be/)([A-Za-z0-9_-]{11})',
        r'youtube.com/watch\?v=([A-Za-z0-9_-]{11})',
        r'youtu.be/([A-Za-z0-9_-]{11})'
    ]
    for p in patterns:
        m = re.search(p, query)
        if m:
            return m.group(1)
    return query

async def get_public_ip() -> str:
    if ip_address_cache.get("ip"):
        return ip_address_cache["ip"]
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get("https://api.ipify.org")
            ip_address_cache["ip"] = r.text.strip()
            return ip_address_cache["ip"]
    except Exception:
        return "localhost"

def load_metadata() -> dict:
    if os.path.exists(METADATA_DRIVE_FILENAME):
        try:
            with open(METADATA_DRIVE_FILENAME, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logs.warning(f"Failed loading metadata file: {e}")
            return {}
    return {}

def save_metadata(data: dict):
    tmp = METADATA_DRIVE_FILENAME + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, METADATA_DRIVE_FILENAME)

def get_cookie_files():
    cookies_dir = "cookies"
    if not os.path.exists(cookies_dir):
        os.makedirs(cookies_dir, exist_ok=True)
        return []
    return glob.glob(os.path.join(cookies_dir, "*.txt"))

# -------------------- Google Drive --------------------
def get_drive_service():
    if not os.path.exists(TOKEN_PATH):
        raise RuntimeError("Token file not found. Place valid token.json with Drive credentials.")
    creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    return service, creds

def upload_file_to_drive(local_path: str, filename: str) -> Optional[str]:
    """
    Uploads local_path to Drive folder (if DRIVE_FOLDER_ID set) and returns drive_file_id.
    Blocking call (intended to be run in executor).
    """
    try:
        service, _ = get_drive_service()
        media = MediaFileUpload(local_path, resumable=True)
        body = {"name": filename}
        if DRIVE_FOLDER_ID:
            body["parents"] = [DRIVE_FOLDER_ID]
        req = service.files().create(body=body, media_body=media, fields="id,size,mimeType").execute()
        file_id = req.get("id")
        logs.info(f"Uploaded to Drive: {file_id}")
        return file_id
    except Exception as e:
        logs.error(f"Drive upload failed: {e}")
        return None

# -------------------- yt-dlp metadata extraction --------------------
def ytdlp_extract_info_sync(url: str, video: bool = False):
    format_type = "best" if video else "bestaudio/best"
    cookie_files = get_cookie_files()

    base_opts = {
        "format": format_type,
        "no_warnings": True,
        "quiet": True,
        "noplaylist": True,
        "skip_download": True,
        "ignoreerrors": True,
    }

    # try cookie files first
    for cookie in cookie_files:
        try:
            opts = base_opts.copy()
            opts["cookiefile"] = cookie
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if info:
                    return info
        except Exception as e:
            logs.debug(f"cookie {cookie} failed: {e}")
            continue

    # fallback no cookies
    try:
        with yt_dlp.YoutubeDL(base_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return info
    except Exception as e:
        logs.error(f"yt-dlp metadata extraction failed: {e}")
        return None

# -------------------- Streaming utilities --------------------
class HTTPRangeStreamer:
    """Stream an external file URL supporting Range requests (used for Drive alt=media)."""
    def __init__(self, url: str, creds: Optional[Credentials] = None):
        self.url = url
        self.creds = creds

    async def iter_bytes(self, range_header: Optional[str] = None):
        headers = {"User-Agent": "Mozilla/5.0"}
        if self.creds and getattr(self.creds, "token", None):
            headers["Authorization"] = f"Bearer {self.creds.token}"
        if range_header:
            headers["Range"] = range_header

        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            async with client.stream("GET", self.url, headers=headers) as resp:
                if resp.status_code not in (200, 206):
                    logs.error(f"Range stream failed status {resp.status_code}")
                    raise HTTPException(status_code=502, detail="Upstream stream failed")
                async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                    yield chunk

# -------------------- Background upload task --------------------
async def background_download_and_upload(youtube_url: str, video_id: str, title: str, video: bool = False):
    """
    Downloads via yt-dlp to temp file, uploads to Drive, updates metadata file.
    Runs in background (async task).
    """
    loop = asyncio.get_running_loop()
    tmpdir = tempfile.mkdtemp(prefix="ytdl_")
    ext = "mp4" if video else "mp3"
    local_filename = f"{video_id}.{ext}"
    local_path = os.path.join(tmpdir, local_filename)

    # Prepare yt-dlp to download best audio/video and convert if needed
    ytdl_opts = {
        "format": "best" if video else "bestaudio/best",
        "outtmpl": local_path,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        # postprocessors could be added if you want mp3 conversion using ffmpeg
    }

    # Download in thread (blocking)
    def sync_download():
        try:
            with yt_dlp.YoutubeDL(ytdl_opts) as ydl:
                ydl.download([youtube_url])
                return True
        except Exception as e:
            logs.error(f"yt-dlp download failed for {youtube_url}: {e}")
            return False

    success = await loop.run_in_executor(None, sync_download)
    if not success or not os.path.exists(local_path):
        logs.error(f"Download failed or file missing: {local_path}")
        return

    # Upload to Drive (blocking)
    drive_file_id = await loop.run_in_executor(None, upload_file_to_drive, local_path, local_filename)
    if not drive_file_id:
        logs.error("Upload failed, will not update metadata.")
        try:
            os.remove(local_path)
        except:
            pass
        return

    # get file size
    file_size = None
    try:
        file_size = os.path.getsize(local_path)
    except:
        file_size = None

    # Update metadata json
    meta = load_metadata()
    meta_entry = {
        "drive_file_id": drive_file_id,
        "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        "format": ext,
        "file_size": file_size,
        "title": title or "Unknown"
    }
    meta[video_id] = meta_entry
    try:
        save_metadata(meta)
        logs.info(f"Metadata updated for {video_id}")
    except Exception as e:
        logs.error(f"Failed saving metadata: {e}")

    # cleanup local
    try:
        os.remove(local_path)
    except:
        pass

# -------------------- API key dependency --------------------
async def get_user(api_key: str = Security(api_key_query)):
    if api_key == API_KEY:
        return "user"
    raise HTTPException(status_code=403, detail="Invalid API key")

# -------------------- Main endpoints --------------------
@app.get("/youtube")
async def get_youtube_info(id: str, video: bool = False, user: str = Security(get_user)):
    start_time = time.time()

    # cleanup expired cache (simple TTL)
    for k, v in list(cache_db.items()):
        if v.get("expiry_time", 0) < time.time():
            cache_db.pop(k, None)

    video_id = extract_video_id(id)
    cache_key = f"{video_id}_{'video' if video else 'audio'}"
    if cache_key in cache_db and cache_db[cache_key].get("expiry_time", 0) > time.time():
        logs.info(f"Returning cached for {video_id}")
        return cache_db[cache_key]["response"]

    # check metadata (drive)
    meta = load_metadata()
    if video_id in meta and meta[video_id].get("drive_file_id"):
        # prepare response using drive file
        entry = meta[video_id]
        stream_type = "Video" if video else "Audio"
        ip = await get_public_ip()
        stream_id = str(uuid.uuid4())
        # create a stream entry that will let /stream fetch from drive
        database[stream_id] = {
            "drive_file_id": entry["drive_file_id"],
            "file_name": f"{video_id}.{entry.get('format', 'dat')}",
            "created_time": time.time(),
            "from_drive": True
        }
        response_data = {
            "id": video_id,
            "title": entry.get("title", "Unknown"),
            "duration": None,
            "link": f"https://www.youtube.com/watch?v={video_id}",
            "channel": None,
            "views": None,
            "thumbnail": None,
            "stream_url": f"http://{ip}:8000/stream/{stream_id}",
            "stream_type": stream_type
        }
        cache_db[cache_key] = {"response": response_data, "expiry_time": time.time() + 3600}
        logs.info(f"Returning drive-backed response for {video_id}")
        return response_data

    # else: extract metadata via yt-dlp (sync in executor)
    try:
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, ytdlp_extract_info_sync, f"https://www.youtube.com/watch?v={video_id}", video)
        if not info:
            raise HTTPException(status_code=500, detail="Could not fetch info from yt-dlp")

        stream_url = info.get("url")
        if not stream_url:
            # sometimes url is inside formats
            formats = info.get("formats") or []
            if formats:
                stream_url = formats[-1].get("url")

        if not stream_url:
            raise HTTPException(status_code=500, detail="No stream URL from yt-dlp")

        extension = "mp4" if video else "mp3"
        file_name = f"{info.get('id')}.{extension}"
        stream_id = str(uuid.uuid4())
        database[stream_id] = {
            "file_url": stream_url,
            "file_name": file_name,
            "created_time": time.time(),
            "from_drive": False,
            "source_info": {
                "youtube_url": info.get("webpage_url") or f"https://www.youtube.com/watch?v={video_id}",
                "title": info.get("title")
            }
        }

        ip = await get_public_ip()
        response_data = {
            "id": info.get("id"),
            "title": info.get("title"),
            "duration": info.get("duration"),
            "link": info.get("webpage_url"),
            "channel": info.get("channel"),
            "views": info.get("view_count"),
            "thumbnail": info.get("thumbnail"),
            "stream_url": f"http://{ip}:8000/stream/{stream_id}",
            "stream_type": "Video" if video else "Audio",
        }

        cache_db[cache_key] = {"response": response_data, "expiry_time": time.time() + 1800}

        # Start background upload task to Drive so next time it serves from Drive
        # This task will download full file via yt-dlp and upload to Drive and then update metadata.
        try:
            youtube_url = info.get("webpage_url") or f"https://www.youtube.com/watch?v={video_id}"
            title = info.get("title", "Unknown")
            asyncio.create_task(background_download_and_upload(youtube_url, video_id, title, video=video))
            logs.info(f"Started background upload task for {video_id}")
        except Exception as e:
            logs.error(f"Failed to start background upload task: {e}")

        elapsed = time.time() - start_time
        logs.info(f"Generated response for {video_id} in {elapsed:.2f}s")
        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logs.error(f"Error in /youtube: {e}")
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/stream/{stream_id}")
async def stream_from_stream_url(stream_id: str):
    entry = database.get(stream_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Stream not found")

    # If from_drive -> use Drive direct download (alt=media) with Authorization
    if entry.get("from_drive") and entry.get("drive_file_id"):
        try:
            service, creds = get_drive_service()
        except Exception as e:
            logs.error(f"Drive creds error: {e}")
            raise HTTPException(status_code=500, detail="Drive credentials error")

        file_id = entry["drive_file_id"]
        # Drive direct URL to download content
        drive_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        streamer = HTTPRangeStreamer(drive_url, creds=creds)

        async def stream_gen():
            # No incoming range header support here because FastAPI's StreamingResponse cannot expose client's Range easily.
            # But we support streaming the whole file in chunks.
            async for chunk in streamer.iter_bytes():
                yield chunk

        headers = {"Content-Disposition": f"attachment; filename=\"{entry.get('file_name')}\""}
        return StreamingResponse(stream_gen(), media_type="application/octet-stream", headers=headers)

    # else stream direct URL (yt-dlp url)
    file_url = entry.get("file_url")
    if not file_url:
        raise HTTPException(status_code=404, detail="No file URL found")

    async def stream_remote():
        # Stream remote with parallel chunk prefetch for speed (simple implementation)
        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            async with client.stream("GET", file_url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
                if resp.status_code not in (200, 206):
                    logs.error(f"Upstream returned {resp.status_code}")
                    raise HTTPException(status_code=502, detail="Upstream returned error")
                async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                    yield chunk

    headers = {"Content-Disposition": f"attachment; filename=\"{entry.get('file_name')}\""}
    return StreamingResponse(stream_remote(), media_type="application/octet-stream", headers=headers)

# -------------------- cleanup background --------------------
@app.on_event("startup")
async def startup_event():
    # cleanup old streams periodically
    async def cleanup_old_streams():
        while True:
            try:
                now = time.time()
                to_del = []
                for sid, d in list(database.items()):
                    if d.get("created_time", 0) + 7200 < now:
                        to_del.append(sid)
                for sid in to_del:
                    database.pop(sid, None)
                    logs.info(f"Cleaned up {sid}")
            except Exception as e:
                logs.error(f"cleanup loop error: {e}")
            await asyncio.sleep(3600)
    asyncio.create_task(cleanup_old_streams())

# -------------------- Run --------------------
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
