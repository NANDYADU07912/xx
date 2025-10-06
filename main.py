import aiohttp, asyncio, httpx, logging, re, uuid, uvicorn, yt_dlp, os, glob, time, json, io
from fastapi import FastAPI, HTTPException, Security
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyQuery
from logging.handlers import RotatingFileHandler
from youtubesearchpython.__future__ import VideosSearch
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from datetime import datetime

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

app = FastAPI()
database = {}
ip_address = {}
cache_db = {}

# Google Drive Configuration
CLIENT_SECRET_PATH = "client_secret.json"
TOKEN_PATH = "token.json"
DRIVE_CACHE_PATH = "drive_cache.json"
METADATA_DRIVE_FILENAME = "api_metadata.json"
DRIVE_FOLDER_ID = None
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Single API key
API_KEY = "ShrutiMusic"

api_key_query = APIKeyQuery(name="api_key", auto_error=True)

# Global drive service
drive_service = None
drive_metadata = {}

async def get_user(api_key: str = Security(api_key_query)):
    if api_key == API_KEY:
        return "user"
    raise HTTPException(status_code=403, detail="Invalid API key")

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

def get_drive_service():
    """Initialize and return Google Drive service"""
    global drive_service
    if drive_service:
        return drive_service
    
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    
    drive_service = build('drive', 'v3', credentials=creds)
    return drive_service

def load_drive_metadata():
    """Load metadata from local JSON file"""
    global drive_metadata
    if os.path.exists(DRIVE_CACHE_PATH):
        try:
            with open(DRIVE_CACHE_PATH, 'r', encoding='utf-8') as f:
                drive_metadata = json.load(f)
            logs.info(f"Loaded {len(drive_metadata)} entries from drive metadata")
        except Exception as e:
            logs.error(f"Error loading drive metadata: {e}")
            drive_metadata = {}
    else:
        drive_metadata = {}

def save_drive_metadata():
    """Save metadata to local JSON file"""
    try:
        with open(DRIVE_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump(drive_metadata, f, indent=2, ensure_ascii=False)
        logs.info(f"Saved {len(drive_metadata)} entries to drive metadata")
    except Exception as e:
        logs.error(f"Error saving drive metadata: {e}")

async def upload_to_drive(video_id: str, file_content: bytes, title: str, format_ext: str):
    """Upload file to Google Drive and update metadata"""
    try:
        service = get_drive_service()
        
        file_metadata = {
            'name': f"{video_id}.{format_ext}",
            'mimeType': 'audio/webm' if format_ext == 'webm' else 'audio/mpeg'
        }
        
        if DRIVE_FOLDER_ID:
            file_metadata['parents'] = [DRIVE_FOLDER_ID]
        
        media = MediaIoBaseUpload(io.BytesIO(file_content), 
                                 mimetype=file_metadata['mimeType'],
                                 resumable=True)
        
        loop = asyncio.get_running_loop()
        file = await loop.run_in_executor(
            None,
            lambda: service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        )
        
        drive_file_id = file.get('id')
        
        # Make file accessible
        await loop.run_in_executor(
            None,
            lambda: service.permissions().create(
                fileId=drive_file_id,
                body={'type': 'anyone', 'role': 'reader'}
            ).execute()
        )
        
        # Update metadata
        drive_metadata[video_id] = {
            "drive_file_id": drive_file_id,
            "uploaded_at": datetime.now().isoformat(),
            "format": format_ext,
            "file_size": len(file_content),
            "title": title
        }
        
        save_drive_metadata()
        logs.info(f"Uploaded {video_id} to Drive: {drive_file_id}")
        
        return drive_file_id
        
    except Exception as e:
        logs.error(f"Error uploading to Drive: {e}")
        return None

async def download_from_drive(drive_file_id: str) -> bytes:
    """Download file from Google Drive"""
    try:
        service = get_drive_service()
        request = service.files().get_media(fileId=drive_file_id)
        
        file_content = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content, request)
        
        done = False
        loop = asyncio.get_running_loop()
        
        while not done:
            status, done = await loop.run_in_executor(None, downloader.next_chunk)
        
        return file_content.getvalue()
        
    except Exception as e:
        logs.error(f"Error downloading from Drive: {e}")
        return None

def extract_video_id(query: str) -> str:
    """Extract YouTube video ID from various URL formats or return as is"""
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
    """Get YouTube URL from video ID"""
    if re.match(r'^[A-Za-z0-9_-]{11}$', video_id):
        return f"https://www.youtube.com/watch?v={video_id}"
    
    try:  
        search = VideosSearch(video_id, limit=1)  
        result = await asyncio.wait_for(search.next(), timeout=10)
        return result["result"][0]["link"]  
    except Exception:  
        return ""

def get_cookie_files():
    """Get all .txt files from cookies folder"""
    cookies_dir = "cookies"
    if not os.path.exists(cookies_dir):
        os.makedirs(cookies_dir)
        return []
    
    cookie_files = glob.glob(os.path.join(cookies_dir, "*.txt"))
    return cookie_files

async def extract_metadata(url: str, video: bool = False):
    if not url:
        return {}

    format_type = "best" if video else "bestaudio/best"  
    cookie_files = get_cookie_files()

    def sync_extract_metadata():  
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
                current_ydl_opts = ydl_opts.copy()
                current_ydl_opts["cookiefile"] = cookie_file
                
                with yt_dlp.YoutubeDL(current_ydl_opts) as ydl:  
                    metadata = ydl.extract_info(url, download=False)
                    if metadata and metadata.get('url'):
                        logging.info(f"Successfully used cookie file: {cookie_file}")
                        return metadata
            except Exception as e:
                logging.warning(f"Cookie file {cookie_file} failed: {e}")
                continue
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:  
                return ydl.extract_info(url, download=False)
        except Exception as e:
            logging.error(f"Metadata extraction error without cookies: {e}")
            return {}

    loop = asyncio.get_running_loop()  
    metadata = await loop.run_in_executor(None, sync_extract_metadata)  

    if metadata:  
        return {  
            "id": metadata.get("id"),  
            "title": metadata.get("title"),  
            "duration": metadata.get("duration"),  
            "link": metadata.get("webpage_url"),  
            "channel": metadata.get("channel", "Unknown"),  
            "views": metadata.get("view_count"),  
            "thumbnail": metadata.get("thumbnail"),  
            "stream_url": metadata.get("url"),  
            "stream_type": "Video" if video else "Audio",  
            "expiry_time": time.time() + 3600,
        }  

    return {}

async def download_audio_file(url: str) -> tuple:
    """Download audio file using yt-dlp and return content + format"""
    try:
        cookie_files = get_cookie_files()
        
        def sync_download():
            ydl_opts = {
                "format": "bestaudio/best",
                "outtmpl": "-",
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
            }
            
            for cookie_file in cookie_files:
                try:
                    current_ydl_opts = ydl_opts.copy()
                    current_ydl_opts["cookiefile"] = cookie_file
                    
                    with yt_dlp.YoutubeDL(current_ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=False)
                        file_url = info.get('url')
                        ext = info.get('ext', 'webm')
                        
                        # Download the file
                        import requests
                        response = requests.get(file_url, stream=True, timeout=30)
                        if response.status_code == 200:
                            content = response.content
                            return content, ext
                except Exception as e:
                    continue
            
            # Try without cookies
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                file_url = info.get('url')
                ext = info.get('ext', 'webm')
                
                import requests
                response = requests.get(file_url, stream=True, timeout=30)
                if response.status_code == 200:
                    return response.content, ext
            
            return None, None
        
        loop = asyncio.get_running_loop()
        content, ext = await loop.run_in_executor(None, sync_download)
        return content, ext
        
    except Exception as e:
        logs.error(f"Error downloading audio: {e}")
        return None, None

async def cleanup_expired_cache():
    """Remove expired cache entries"""
    current_time = time.time()
    expired_keys = []
    for key, data in cache_db.items():
        if data.get("expiry_time", 0) < current_time:
            expired_keys.append(key)
    
    for key in expired_keys:
        del cache_db[key]
        logs.info(f"Removed expired cache for ID: {key}")

class DriveStreamer:
    def __init__(self):
        self.chunk_size = 1 * 1024 * 1024

    async def stream_from_bytes(self, content: bytes):
        """Stream file content from bytes"""
        for i in range(0, len(content), self.chunk_size):
            yield content[i:i + self.chunk_size]
            await asyncio.sleep(0)

class Streamer:
    def __init__(self):
        self.chunk_size = 1 * 1024 * 1024

    async def get_total_chunks(self, file_url):  
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.head(file_url)  
            file_size = response.headers.get("Content-Length")  
            return (int(file_size) + self.chunk_size - 1) // self.chunk_size if file_size else None  

    async def fetch_chunk(self, file_url, chunk_id):  
        start_byte = chunk_id * self.chunk_size  
        async with httpx.AsyncClient(follow_redirects=True, timeout=10) as client:
            headers = {  
                "Range": f"bytes={start_byte}-{start_byte + self.chunk_size - 1}",  
                "User-Agent": "Mozilla/5.0"  
            }  
            response = await client.get(file_url, headers=headers)  
            return response.content if response.status_code in {206, 200} else None  

    async def stream_file(self, file_url):  
        total_chunks = await self.get_total_chunks(file_url)  
        received_chunks = set()  
        chunk_id = 0  

        while total_chunks is None or chunk_id < total_chunks:  
            next_chunk_task = asyncio.create_task(self.fetch_chunk(file_url, chunk_id + 1))  
            current_chunk_task = asyncio.create_task(self.fetch_chunk(file_url, chunk_id))  

            current_chunk = await current_chunk_task  
            if current_chunk:  
                received_chunks.add(chunk_id)  
                yield current_chunk  

            next_chunk = await next_chunk_task  
            if next_chunk:  
                received_chunks.add(chunk_id + 1)  
                yield next_chunk  

            chunk_id += 2  

        if total_chunks:  
            for chunk_id in range(total_chunks):  
                if chunk_id not in received_chunks:  
                    missing_chunk = await self.fetch_chunk(file_url, chunk_id)  
                    if missing_chunk:  
                        yield missing_chunk

@app.get("/youtube")
async def get_youtube_info(id: str, video: bool = False, user: str = Security(get_user)):
    start_time = time.time()
    
    try:
        await cleanup_expired_cache()
        video_id = extract_video_id(id)
        
        # For video requests, use old method (yt-dlp direct stream)
        if video:
            cache_key = f"{video_id}_video"
            if cache_key in cache_db:
                cached_data = cache_db[cache_key]
                if cached_data.get("expiry_time", 0) > time.time():
                    elapsed_time = time.time() - start_time
                    logs.info(f"Returning cached video response for ID: {video_id} in {elapsed_time:.2f}s")
                    return cached_data["response"]
            
            url = await asyncio.wait_for(get_youtube_url(video_id), timeout=12)
            if not url:
                return {"error": "Invalid YouTube ID"}
            
            metadata = await asyncio.wait_for(extract_metadata(url, video=True), timeout=12)
            
            if not metadata or not metadata.get("stream_url"):
                return {"error": "Could not fetch stream URL"}
            
            file_url = metadata.get("stream_url")
            file_name = f"{metadata.get('id')}.mp4"
            ip = await get_public_ip()
            stream_id = await new_uid()
            stream_url = f"http://{ip}:8000/stream/{stream_id}"
            
            database[stream_id] = {
                "file_url": file_url, 
                "file_name": file_name,
                "created_time": time.time(),
                "stream_type": "video"
            }
            
            response_data = {
                "id": metadata.get("id"),
                "title": metadata.get("title"),
                "duration": metadata.get("duration"),
                "link": metadata.get("link"),
                "channel": metadata.get("channel"),
                "views": metadata.get("views"),
                "thumbnail": metadata.get("thumbnail"),
                "stream_url": stream_url,
                "stream_type": "Video",
            }
            
            cache_db[cache_key] = {
                "response": response_data,
                "expiry_time": metadata.get("expiry_time", time.time() + 3600)
            }
            
            elapsed_time = time.time() - start_time
            logs.info(f"Video response generated in {elapsed_time:.2f}s")
            return response_data
        
        # For audio requests, check Drive first
        if video_id in drive_metadata:
            logs.info(f"Found {video_id} in Drive metadata")
            drive_file_id = drive_metadata[video_id]["drive_file_id"]
            
            ip = await get_public_ip()
            stream_id = await new_uid()
            stream_url = f"http://{ip}:8000/stream/{stream_id}"
            
            database[stream_id] = {
                "drive_file_id": drive_file_id,
                "file_name": f"{video_id}.{drive_metadata[video_id]['format']}",
                "created_time": time.time(),
                "stream_type": "drive"
            }
            
            # Get fresh metadata from YouTube
            url = await asyncio.wait_for(get_youtube_url(video_id), timeout=12)
            metadata = await asyncio.wait_for(extract_metadata(url, video=False), timeout=12) if url else {}
            
            response_data = {
                "id": video_id,
                "title": metadata.get("title") or drive_metadata[video_id].get("title", "Unknown"),
                "duration": metadata.get("duration"),
                "link": metadata.get("link") or f"https://www.youtube.com/watch?v={video_id}",
                "channel": metadata.get("channel", "Unknown"),
                "views": metadata.get("views"),
                "thumbnail": metadata.get("thumbnail"),
                "stream_url": stream_url,
                "stream_type": "Audio (Drive)",
            }
            
            elapsed_time = time.time() - start_time
            logs.info(f"Drive audio response generated in {elapsed_time:.2f}s")
            return response_data
        
        # If not in Drive, download and upload
        logs.info(f"{video_id} not in Drive, downloading and uploading...")
        url = await asyncio.wait_for(get_youtube_url(video_id), timeout=12)
        if not url:
            return {"error": "Invalid YouTube ID"}
        
        metadata = await asyncio.wait_for(extract_metadata(url, video=False), timeout=12)
        if not metadata:
            return {"error": "Could not fetch metadata"}
        
        # Download audio file
        audio_content, audio_format = await download_audio_file(url)
        if not audio_content:
            return {"error": "Could not download audio"}
        
        # Upload to Drive in background
        asyncio.create_task(upload_to_drive(
            video_id, 
            audio_content, 
            metadata.get("title", "Unknown"),
            audio_format
        ))
        
        # For now, stream from yt-dlp (next request will use Drive)
        file_url = metadata.get("stream_url")
        file_name = f"{video_id}.{audio_format}"
        ip = await get_public_ip()
        stream_id = await new_uid()
        stream_url = f"http://{ip}:8000/stream/{stream_id}"
        
        database[stream_id] = {
            "file_url": file_url,
            "file_name": file_name,
            "created_time": time.time(),
            "stream_type": "ytdlp"
        }
        
        response_data = {
            "id": video_id,
            "title": metadata.get("title"),
            "duration": metadata.get("duration"),
            "link": metadata.get("link"),
            "channel": metadata.get("channel"),
            "views": metadata.get("views"),
            "thumbnail": metadata.get("thumbnail"),
            "stream_url": stream_url,
            "stream_type": "Audio (Uploading to Drive...)",
        }
        
        elapsed_time = time.time() - start_time
        logs.info(f"New audio response generated in {elapsed_time:.2f}s")
        return response_data
            
    except asyncio.TimeoutError:
        logs.error("Request timeout - taking more than 12 seconds")
        return {"error": "Request timeout - taking too long"}
    except Exception as e:  
        logs.error(f"Error fetching YouTube info: {e}")  
        return {"error": "Something went wrong"}

@app.get("/stream/{stream_id}")
async def stream_from_stream_url(stream_id: str):
    file_data = database.get(stream_id)
    if not file_data:
        return {"error": "Invalid stream request!"}
    
    try:
        # Stream from Drive
        if file_data.get("stream_type") == "drive":
            drive_file_id = file_data.get("drive_file_id")
            logs.info(f"Streaming from Drive: {drive_file_id}")
            
            content = await download_from_drive(drive_file_id)
            if not content:
                return {"error": "Could not download from Drive"}
            
            streamer = DriveStreamer()
            headers = {"Content-Disposition": f"attachment; filename=\"{file_data.get('file_name')}\""}
            return StreamingResponse(
                streamer.stream_from_bytes(content),
                media_type="application/octet-stream",
                headers=headers
            )
        
        # Stream from yt-dlp or video
        file_url = file_data.get("file_url")
        if not file_url:
            return {"error": "Invalid stream request!"}
        
        streamer = Streamer()
        headers = {"Content-Disposition": f"attachment; filename=\"{file_data.get('file_name')}\""}
        return StreamingResponse(
            streamer.stream_file(file_url),
            media_type="application/octet-stream",
            headers=headers
        )
        
    except Exception as e:
        logging.error(f"Stream Error: {e}")
        return {"error": "Something went wrong!"}

async def cleanup_old_streams():
    """Clean up old stream entries"""
    while True:
        await asyncio.sleep(3600)
        current_time = time.time()
        expired_streams = []
        for stream_id, data in database.items():
            if data.get("created_time", 0) + 7200 < current_time:
                expired_streams.append(stream_id)
        
        for stream_id in expired_streams:
            del database[stream_id]
            logs.info(f"Cleaned up old stream: {stream_id}")

@app.on_event("startup")
async def startup_event():
    """Initialize Drive service and load metadata on startup"""
    try:
        get_drive_service()
        load_drive_metadata()
        logs.info("Drive service initialized successfully")
    except Exception as e:
        logs.error(f"Failed to initialize Drive service: {e}")
    
    asyncio.create_task(cleanup_old_streams())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
