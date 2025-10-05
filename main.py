import aiohttp, asyncio, httpx, logging, re, uuid, uvicorn, yt_dlp, os, glob, time, json
from fastapi import FastAPI, HTTPException, Security
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyQuery
from logging.handlers import RotatingFileHandler
from youtubesearchpython.__future__ import VideosSearch
from datetime import datetime

try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
    import io
    DRIVE_AVAILABLE = True
except ImportError:
    DRIVE_AVAILABLE = False

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

# API Configuration
API_KEY = "ShrutiMusic"

# Drive Configuration
CLIENT_SECRET_PATH = "client_secret.json"
TOKEN_PATH = "token.json"
DRIVE_CACHE_PATH = "drive_cache.json"
DRIVE_FOLDER_ID = None
SCOPES = ['https://www.googleapis.com/auth/drive.file']

api_key_query = APIKeyQuery(name="api_key", auto_error=True)

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

def extract_video_id(query: str) -> str:
    """Extract YouTube video ID from various URL formats"""
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

# ========== DRIVE FUNCTIONS ==========

def get_drive_service():
    if not DRIVE_AVAILABLE:
        return None
    
    creds = None
    if os.path.exists(TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        except Exception as e:
            logs.error(f"Token load error: {e}")
            creds = None
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                logs.info("Token refreshed successfully")
            except Exception as e:
                logs.error(f"Token refresh failed: {e}")
                creds = None
        else:
            if not os.path.exists(CLIENT_SECRET_PATH):
                logs.error(f"client_secret.json not found")
                return None
            try:
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
                auth_url, _ = flow.authorization_url(prompt='consent')
                logs.info("Authorize this URL:")
                logs.info(auth_url)
                code = input("Enter authorization code: ").strip()
                flow.fetch_token(code=code)
                creds = flow.credentials
                logs.info("Authorization completed")
            except Exception as e:
                logs.error(f"OAuth flow failed: {e}")
                return None
        
        try:
            with open(TOKEN_PATH, "w") as f:
                f.write(creds.to_json())
            logs.info(f"Token saved")
        except Exception as e:
            logs.error(f"Token save failed: {e}")
    
    try:
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        return service
    except Exception as e:
        logs.error(f"Drive service build failed: {e}")
        return None

def search_drive_by_video_id(video_id):
    """Direct search in Drive by video ID"""
    service = get_drive_service()
    if not service:
        return None
    
    try:
        query = f"name contains '{video_id}' and trashed=false"
        if DRIVE_FOLDER_ID:
            query += f" and '{DRIVE_FOLDER_ID}' in parents"
        
        results = service.files().list(
            q=query,
            fields="files(id, name, size, mimeType)",
            pageSize=10
        ).execute()
        
        files = results.get('files', [])
        if not files:
            return None
        
        exact_matches = [f for f in files if f['name'].startswith(f"{video_id}.")]
        if exact_matches:
            return exact_matches[0]['id']
        
        return None
    except Exception as e:
        logs.error(f"Drive search error: {e}")
        return None

def download_from_drive(drive_file_id, dest_path):
    """Download file from Drive"""
    service = get_drive_service()
    if not service:
        return False
        
    try:
        service.files().get(fileId=drive_file_id).execute()
            
        request = service.files().get_media(fileId=drive_file_id)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        
        with open(dest_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
                
        logs.info(f"Downloaded from Drive: {dest_path}")
        return True
    except Exception as e:
        logs.error(f"Drive download failed: {e}")
        return False

def upload_to_drive(file_path, video_id):
    """Upload file to Drive with 120MB limit"""
    service = get_drive_service()
    if not service or not os.path.exists(file_path):
        return None
    
    file_size = os.path.getsize(file_path)
    file_size_mb = file_size / (1024 * 1024)
    
    if file_size_mb > 120:
        logs.warning(f"File size {file_size_mb:.2f} MB exceeds 120MB limit")
        return None
        
    try:
        file_metadata = {"name": f"{video_id}.mp3"}
        if DRIVE_FOLDER_ID:
            file_metadata["parents"] = [DRIVE_FOLDER_ID]
        
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(
            body=file_metadata, 
            media_body=media, 
            fields="id"
        ).execute()
        drive_id = file.get("id")
        logs.info(f"Uploaded to Drive: {video_id} ({file_size_mb:.2f} MB)")
        return drive_id
    except Exception as e:
        logs.error(f"Drive upload failed: {e}")
        return None

def load_drive_cache():
    """Load cache from file"""
    if not DRIVE_AVAILABLE or not os.path.exists(DRIVE_CACHE_PATH):
        return {}
    
    try:
        with open(DRIVE_CACHE_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        logs.error(f"Cache load error: {e}")
        return {}

def save_drive_cache(cache_data):
    """Save cache to file"""
    if not DRIVE_AVAILABLE:
        return False
        
    try:
        os.makedirs(os.path.dirname(DRIVE_CACHE_PATH), exist_ok=True)
        with open(DRIVE_CACHE_PATH, 'w') as f:
            json.dump(cache_data, f, indent=2)
        return True
    except Exception as e:
        logs.error(f"Cache save error: {e}")
        return False

# ========== END DRIVE FUNCTIONS ==========

async def download_with_cookies(url: str, video_id: str, is_video: bool = False):
    """Download using yt-dlp with cookies - same as your working code"""
    cookie_files = get_cookie_files()
    if not cookie_files:
        logs.error("No cookie files available")
        return None
        
    download_folder = "downloads"
    os.makedirs(download_folder, exist_ok=True)
    
    # Check if file already exists with any extension
    for ext in ["mp4", "mp3", "m4a", "webm"]:
        existing_path = f"{download_folder}/{video_id}.{ext}"
        if os.path.exists(existing_path):
            logs.info(f"File already exists: {existing_path}")
            return existing_path
    
    def sync_download():
        """Synchronous download function"""
        for cookie_file in cookie_files:
            try:
                if is_video:
                    ydl_opts = {
                        "format": "best[height<=?720][width<=?1280]",
                        "outtmpl": f"{download_folder}/{video_id}.%(ext)s",
                        "geo_bypass": True,
                        "nocheckcertificate": True,
                        "quiet": True,
                        "no_warnings": True,
                        "cookiefile": cookie_file,
                        "socket_timeout": 30,
                        "retries": 3,
                        "http_headers": {
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        }
                    }
                else:
                    ydl_opts = {
                        "format": "bestaudio/best",
                        "outtmpl": f"{download_folder}/{video_id}.%(ext)s",
                        "geo_bypass": True,
                        "nocheckcertificate": True,
                        "quiet": True,
                        "no_warnings": True,
                        "cookiefile": cookie_file,
                        "socket_timeout": 30,
                        "retries": 3,
                        "http_headers": {
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        }
                    }
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    logs.info(f"Downloading with cookie: {cookie_file}")
                    info = ydl.extract_info(url, download=False)
                    expected_ext = info.get('ext', 'mp4' if is_video else 'mp3')
                    expected_path = f"{download_folder}/{video_id}.{expected_ext}"
                    
                    if os.path.exists(expected_path):
                        logs.info(f"File already exists after check: {expected_path}")
                        return expected_path
                    
                    ydl.download([url])
                    
                    # Check for downloaded file
                    if os.path.exists(expected_path):
                        logs.info(f"Download successful: {expected_path}")
                        return expected_path
                    
                    # Sometimes extension might be different, check all possibilities
                    for ext in ["mp4", "webm", "mkv", "mp3", "m4a"]:
                        alt_path = f"{download_folder}/{video_id}.{ext}"
                        if os.path.exists(alt_path):
                            logs.info(f"Download successful (alt format): {alt_path}")
                            return alt_path
                        
            except Exception as e:
                logs.warning(f"Cookie {cookie_file} failed: {e}")
                continue
        
        return None
    
    try:
        # Run in executor to avoid blocking
        loop = asyncio.get_running_loop()
        downloaded_file = await loop.run_in_executor(None, sync_download)
        
        if downloaded_file and not is_video and DRIVE_AVAILABLE:
            # Upload audio to Drive
            try:
                file_size = os.path.getsize(downloaded_file)
                file_size_mb = file_size / (1024 * 1024)
                
                if file_size_mb <= 120:
                    drive_id = upload_to_drive(downloaded_file, video_id)
                    if drive_id:
                        cache = load_drive_cache()
                        cache[video_id] = {
                            "drive_file_id": drive_id,
                            "uploaded_at": datetime.now().isoformat(),
                            "file_size": file_size
                        }
                        save_drive_cache(cache)
                        logs.info(f"Uploaded to Drive: {video_id}")
            except Exception as e:
                logs.error(f"Drive upload failed (non-critical): {e}")
        
        return downloaded_file
        
    except Exception as e:
        logs.error(f"Download failed: {e}")
        return None

async def extract_metadata(url: str, video: bool = False):
    """Extract basic metadata"""
    if not url:
        return {}

    try:
        search = VideosSearch(url, limit=1)
        result = await asyncio.wait_for(search.next(), timeout=5)
        if result and result.get("result"):
            info = result["result"][0]
            return {
                "id": info.get("id"),
                "title": info.get("title"),
                "duration": info.get("duration"),
                "link": info.get("link"),
                "channel": info.get("channel", {}).get("name", "Unknown"),
                "views": info.get("viewCount", {}).get("text", "0"),
                "thumbnail": info.get("thumbnails", [{}])[0].get("url", "").split("?")[0],
            }
    except Exception as e:
        logs.error(f"Metadata extraction failed: {e}")
    
    return {}

class Streamer:
    def __init__(self):
        self.chunk_size = 1 * 1024 * 1024

    async def stream_file_from_path(self, file_path):
        """Stream from local file"""
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(self.chunk_size):
                    yield chunk
        except Exception as e:
            logs.error(f"File stream error: {e}")

@app.get("/youtube")
async def get_youtube_info(id: str, video: bool = False, user: str = Security(get_user)):
    start_time = time.time()
    
    try:
        video_id = extract_video_id(id)
        logs.info(f"Processing request for: {video_id} (video={video})")
        
        # For AUDIO: Check Drive first (fastest path)
        if not video and DRIVE_AVAILABLE:
            logs.info(f"Checking Drive for audio: {video_id}")
            
            # Direct Drive search
            drive_file_id = search_drive_by_video_id(video_id)
            if drive_file_id:
                download_folder = "downloads"
                os.makedirs(download_folder, exist_ok=True)
                local_path = f"{download_folder}/{video_id}.mp3"
                
                if download_from_drive(drive_file_id, local_path):
                    logs.info(f"Found in Drive: {video_id}")
                    
                    # Create stream response
                    ip = await get_public_ip()
                    stream_id = await new_uid()
                    stream_url = f"http://{ip}:8000/stream/{stream_id}"
                    
                    database[stream_id] = {
                        "file_path": local_path,
                        "file_name": f"{video_id}.mp3",
                        "is_local": True
                    }
                    
                    # Get metadata
                    url = await get_youtube_url(video_id)
                    metadata = await extract_metadata(url, video) if url else {}
                    
                    elapsed = time.time() - start_time
                    logs.info(f"Drive response: {elapsed:.2f}s")
                    
                    return {
                        "id": video_id,
                        "title": metadata.get("title", "Unknown"),
                        "duration": metadata.get("duration", "0:00"),
                        "link": f"https://www.youtube.com/watch?v={video_id}",
                        "channel": metadata.get("channel", "Unknown"),
                        "views": metadata.get("views", "0"),
                        "thumbnail": metadata.get("thumbnail", ""),
                        "stream_url": stream_url,
                        "stream_type": "Audio",
                        "source": "drive"
                    }
        
        # Not in Drive or Video request - Download with cookies
        logs.info(f"Downloading with cookies: {video_id}")
        url = await get_youtube_url(video_id)
        if not url:
            return {"error": "Invalid YouTube ID"}
        
        downloaded_file = await download_with_cookies(url, video_id, video)
        
        if not downloaded_file:
            return {"error": "Download failed"}
        
        # Create stream response
        ip = await get_public_ip()
        stream_id = await new_uid()
        stream_url = f"http://{ip}:8000/stream/{stream_id}"
        
        database[stream_id] = {
            "file_path": downloaded_file,
            "file_name": f"{video_id}.{'mp4' if video else 'mp3'}",
            "is_local": True
        }
        
        # Get metadata
        metadata = await extract_metadata(url, video)
        
        elapsed = time.time() - start_time
        logs.info(f"Cookie download response: {elapsed:.2f}s")
        
        return {
            "id": video_id,
            "title": metadata.get("title", "Unknown"),
            "duration": metadata.get("duration", "0:00"),
            "link": url,
            "channel": metadata.get("channel", "Unknown"),
            "views": metadata.get("views", "0"),
            "thumbnail": metadata.get("thumbnail", ""),
            "stream_url": stream_url,
            "stream_type": "Video" if video else "Audio",
            "source": "cookies"
        }
            
    except Exception as e:
        logs.error(f"Error: {e}")
        return {"error": str(e)}

@app.get("/stream/{stream_id}")
async def stream_from_stream_url(stream_id: str):
    file_data = database.get(stream_id)
    if not file_data:
        return {"error": "Invalid stream request"}

    file_path = file_data.get("file_path")
    file_name = file_data.get("file_name")
    
    if not file_path or not os.path.exists(file_path):
        return {"error": "File not found"}
    
    def iterfile():
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                yield chunk
    
    media_type = "audio/mpeg" if file_name.endswith('.mp3') else "video/mp4"
    headers = {"Content-Disposition": f"attachment; filename=\"{file_name}\""}
    
    return StreamingResponse(
        iterfile(),
        media_type=media_type,
        headers=headers
    )

@app.on_event("startup")
async def startup_event():
    if DRIVE_AVAILABLE:
        service = get_drive_service()
        if service:
            logs.info("Drive integration initialized")
            cache = load_drive_cache()
            logs.info(f"Loaded cache with {len(cache)} entries")
        else:
            logs.warning("Drive integration failed")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
