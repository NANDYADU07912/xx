import os
import io
import re
import json
import time
import uuid
import glob
import logging
import asyncio
import httpx
import yt_dlp
import aiohttp
import uvicorn
from fastapi import FastAPI, HTTPException, Security
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyQuery
from logging.handlers import RotatingFileHandler
from youtubesearchpython.__future__ import VideosSearch
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request

# ----------------- Logging -----------------
logging.basicConfig(
    format="%(asctime)s [%(name)s]:: %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        RotatingFileHandler("api.log", maxBytes=(1024 * 1024 * 5), backupCount=10),
        logging.StreamHandler(),
    ],
)
logging.getLogger("asyncio").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)
logs = logging.getLogger(__name__)

# ----------------- Constants -----------------
CLIENT_SECRET_PATH = "client_secret.json"
TOKEN_PATH = "token.json"
DRIVE_CACHE_PATH = "drive_cache.json"
METADATA_DRIVE_FILENAME = "api_metadata.json"
DRIVE_FOLDER_ID = None
SCOPES = ['https://www.googleapis.com/auth/drive.file']
API_KEY = "ShrutiMusic"

# ----------------- Globals -----------------
app = FastAPI()
database = {}
ip_address = {}
cache_db = {}
metadata = {}  # {video_id: {...}}
drive_service = None

api_key_query = APIKeyQuery(name="api_key", auto_error=True)

# ----------------- API Key -----------------
async def get_user(api_key: str = Security(api_key_query)):
    if api_key == API_KEY:
        return "user"
    raise HTTPException(status_code=403, detail="Invalid API key")

# ----------------- Helpers -----------------
async def new_uid() -> str:
    return str(uuid.uuid4())

async def get_public_ip() -> str:
    if ip_address.get("ip_address"):
        return ip_address["ip_address"]
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            response = await client.get('https://api.ipify.org')
            public_ip = response.text
            ip_address["ip_address"] = public_ip
            return public_ip
    except:
        return "localhost"

def extract_video_id(query: str) -> str:
    if re.match(r'^[A-Za-z0-9_-]{11}$', query):
        return query
    patterns = [
        r'(?:v=|/(?:embed|v|shorts|live)/|youtu.be/)([A-Za-z0-9_-]{11})',
        r'youtube.com/watch\?v=([A-Za-z0-9_-]{11})',
        r'youtu.be/([A-Za-z0-9_-]{11})'
    ]
    for pattern in patterns:
        match = re.search(pattern, query)
        if match:
            return match.group(1)
    return query

async def get_youtube_url(video_id: str) -> str:
    if re.match(r'^[A-Za-z0-9_-]{11}$', video_id):
        return f"https://www.youtube.com/watch?v={video_id}"
    try:
        search = VideosSearch(video_id, limit=1)
        result = await asyncio.wait_for(search.next(), timeout=10)
        return result["result"][0]["link"]
    except:
        return ""

def get_cookie_files():
    cookies_dir = "cookies"
    if not os.path.exists(cookies_dir):
        os.makedirs(cookies_dir)
    return glob.glob(os.path.join(cookies_dir, "*.txt"))

# ----------------- yt-dlp Metadata -----------------
async def extract_metadata(url: str, video: bool = False):
    if not url:
        return {}
    format_type = "best" if video else "bestaudio/best"
    cookie_files = get_cookie_files()

    def sync_extract():
        ydl_opts = {
            "format": format_type,
            "no_warnings": True,
            "simulate": True,
            "quiet": True,
            "noplaylist": True,
            "extract_flat": True,
            "skip_download": True,
            "force_generic_extractor": True,
            "ignoreerrors": True,
        }
        for cookie_file in cookie_files:
            try:
                opts = ydl_opts.copy()
                opts["cookiefile"] = cookie_file
                with yt_dlp.YoutubeDL(opts) as ydl:
                    data = ydl.extract_info(url, download=False)
                    if data and data.get('url'):
                        logs.info(f"Used cookie file: {cookie_file}")
                        return data
            except Exception as e:
                logs.warning(f"Cookie file {cookie_file} failed: {e}")
                continue
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)
        except Exception as e:
            logs.error(f"yt-dlp metadata error: {e}")
            return {}

    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, sync_extract)
    if not data:
        return {}
    return {
        "id": data.get("id"),
        "title": data.get("title"),
        "duration": data.get("duration"),
        "link": data.get("webpage_url"),
        "channel": data.get("channel", "Unknown"),
        "views": data.get("view_count"),
        "thumbnail": data.get("thumbnail"),
        "stream_url": data.get("url"),
        "stream_type": "Video" if video else "Audio",
        "expiry_time": time.time() + 3600,
    }

# ----------------- Cache Cleanup -----------------
async def cleanup_expired_cache():
    current_time = time.time()
    expired = [k for k, v in cache_db.items() if v.get("expiry_time",0)<current_time]
    for k in expired:
        del cache_db[k]
        logs.info(f"Removed expired cache for ID: {k}")

# ----------------- Streamer -----------------
class Streamer:
    def __init__(self):
        self.chunk_size = 1*1024*1024

    async def get_total_chunks(self, file_url):
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.head(file_url)
            file_size = response.headers.get("Content-Length")
            return (int(file_size)+self.chunk_size-1)//self.chunk_size if file_size else None

    async def fetch_chunk(self, file_url, chunk_id):
        start_byte = chunk_id*self.chunk_size
        async with httpx.AsyncClient(follow_redirects=True, timeout=10) as client:
            headers = {
                "Range": f"bytes={start_byte}-{start_byte+self.chunk_size-1}",
                "User-Agent": "Mozilla/5.0"
            }
            response = await client.get(file_url, headers=headers)
            if response.status_code in {200,206}:
                return response.content
            return None

    async def stream_file(self, file_url):
        total_chunks = await self.get_total_chunks(file_url)
        received = set()
        chunk_id = 0
        while total_chunks is None or chunk_id<total_chunks:
            next_chunk_task = asyncio.create_task(self.fetch_chunk(file_url, chunk_id+1))
            current_chunk_task = asyncio.create_task(self.fetch_chunk(file_url, chunk_id))
            current_chunk = await current_chunk_task
            if current_chunk:
                received.add(chunk_id)
                yield current_chunk
            next_chunk = await next_chunk_task
            if next_chunk:
                received.add(chunk_id+1)
                yield next_chunk
            chunk_id+=2
        if total_chunks:
            for i in range(total_chunks):
                if i not in received:
                    missing = await self.fetch_chunk(file_url,i)
                    if missing:
                        yield missing

# ----------------- Google Drive -----------------
def init_drive_service():
    global drive_service
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH,"w") as f:
            f.write(creds.to_json())
    drive_service = build('drive','v3',credentials=creds)

def download_metadata_from_drive():
    global metadata
    if not drive_service:
        init_drive_service()
    try:
        results = drive_service.files().list(q=f"name='{METADATA_DRIVE_FILENAME}'",
                                             spaces='drive',
                                             fields="files(id,name)").execute()
        files = results.get('files',[])
        if not files:
            metadata = {}
            return
        file_id = files[0]['id']
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, drive_service.files().get_media(fileId=file_id))
        done=False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        metadata = json.load(fh)
        logs.info(f"Loaded metadata from Drive, {len(metadata)} entries")
    except Exception as e:
        logs.error(f"Metadata load error: {e}")
        metadata={}

def save_metadata_to_drive():
    global metadata
    if not drive_service:
        init_drive_service()
    try:
        fh = io.BytesIO(json.dumps(metadata, indent=2).encode())
        results = drive_service.files().list(q=f"name='{METADATA_DRIVE_FILENAME}'", spaces='drive',
                                             fields="files(id,name)").execute()
        files = results.get('files',[])
        media = MediaIoBaseUpload(fh, mimetype="application/json", resumable=True)
        if files:
            drive_service.files().update(fileId=files[0]['id'], media_body=media).execute()
        else:
            file_metadata = {'name': METADATA_DRIVE_FILENAME,'parents':[DRIVE_FOLDER_ID] if DRIVE_FOLDER_ID else []}
            drive_service.files().create(body=file_metadata, media_body=media).execute()
        logs.info("Metadata saved to Drive")
    except Exception as e:
        logs.error(f"Metadata save error: {e}")

async def upload_file_to_drive(file_path:str):
    if not drive_service:
        init_drive_service()
    try:
        file_name = os.path.basename(file_path)
        media = MediaIoBaseUpload(io.FileIO(file_path,'rb'), mimetype='application/octet-stream', resumable=True)
        file_metadata = {'name': file_name,'parents':[DRIVE_FOLDER_ID] if DRIVE_FOLDER_ID else []}
        file = drive_service.files().create(body=file_metadata, media_body=media).execute()
        return file.get('id')
    except Exception as e:
        logs.error(f"Drive upload error: {e}")
        return None

# ----------------- YouTube API Endpoint -----------------
@app.get("/youtube")
async def get_youtube_info(id: str, video: bool = False, user: str = Security(get_user)):
    start_time = time.time()
    await cleanup_expired_cache()
    video_id = extract_video_id(id)
    cache_key = f"{video_id}_{'video' if video else 'audio'}"
    if cache_key in cache_db:
        cached = cache_db[cache_key]
        if cached.get("expiry_time",0)>time.time():
            logs.info(f"Returning cached response for ID:{video_id}")
            return cached["response"]

    source = "yt_dlp"
    file_url = None
    file_name = None

    # ----------------- Audio from Drive -----------------
    if not video and video_id in metadata:
        file_id = metadata[video_id]["drive_file_id"]
        file_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        file_name = f"{video_id}.{metadata[video_id].get('format','mp3')}"
        source = "drive"

    # ----------------- Otherwise fetch via yt-dlp -----------------
    if file_url is None:
        url = await asyncio.wait_for(get_youtube_url(video_id), timeout=12)
        if not url:
            return {"error": "Invalid YouTube ID"}
        meta = await asyncio.wait_for(extract_metadata(url, video), timeout=12)
        if not meta or not meta.get("stream_url"):
            return {"error": "Could not fetch stream URL"}

        file_url = meta["stream_url"]
        file_name = f"{meta['id']}.{'mp4' if video else 'mp3'}"

        # If audio, upload to Drive + metadata update
        if not video:
            temp_file = f"/tmp/{file_name}"
            # Download temporarily
            ydl_opts = {
                "format":"bestaudio/best",
                "outtmpl":temp_file,
                "quiet":True
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            drive_file_id = await upload_file_to_drive(temp_file)
            os.remove(temp_file)
            if drive_file_id:
                metadata[video_id] = {
                    "drive_file_id": drive_file_id,
                    "uploaded_at": datetime.utcnow().isoformat(),
                    "format":"mp3",
                    "file_size": os.path.getsize(temp_file) if os.path.exists(temp_file) else 0,
                    "title": meta["title"]
                }
                save_metadata_to_drive()
                source="yt_dlp"

        meta["source"]=source
        meta["stream_url"]=file_url
    else:
        meta = {
            "id": video_id,
            "title": metadata[video_id]["title"],
            "duration": metadata[video_id].get("duration"),
            "link": f"https://www.youtube.com/watch?v={video_id}",
            "channel": metadata[video_id].get("channel","Unknown"),
            "views": metadata[video_id].get("views"),
            "thumbnail": metadata[video_id].get("thumbnail"),
            "stream_url": file_url,
            "stream_type":"Audio",
            "source":source
        }

    ip = await get_public_ip()
    stream_id = await new_uid()
    stream_url = f"http://{ip}:8000/stream/{stream_id}"
    database[stream_id] = {"file_url":file_url,"file_name":file_name,"created_time":time.time()}

    response_data = meta.copy()
    response_data["stream_url"]=stream_url

    cache_db[cache_key]={"response":response_data,"expiry_time":meta.get("expiry_time",time.time()+3600)}
    return response_data

# ----------------- Stream Endpoint -----------------
@app.get("/stream/{stream_id}")
async def stream_from_stream_url(stream_id: str):
    file_data = database.get(stream_id)
    if not file_data or not file_data.get("file_url") or not file_data.get("file_name"):
        return {"error":"Invalid stream request!"}
    streamer = Streamer()
    headers={"Content-Disposition":f"attachment; filename=\"{file_data.get('file_name')}\""}
    return StreamingResponse(streamer.stream_file(file_data.get("file_url")),
                             media_type="application/octet-stream", headers=headers)

# ----------------- Cleanup Old Streams -----------------
async def cleanup_old_streams():
    while True:
        await asyncio.sleep(3600)
        current_time = time.time()
        expired_streams = [sid for sid,data in database.items() if data.get("created_time",0)+7200<current_time]
        for sid in expired_streams:
            del database[sid]
            logs.info(f"Cleaned up old stream: {sid}")

@app.on_event("startup")
async def startup_event():
    init_drive_service()
    download_metadata_from_drive()
    asyncio.create_task(cleanup_old_streams())

if __name__=="__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
