import aiohttp, asyncio, httpx, logging, re, uuid, uvicorn, yt_dlp, os, glob, time, json, random
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

# Single API key
API_KEY = "ShrutiMusic"

# Drive Configuration
CLIENT_SECRET_PATH = "client_secret.json"
TOKEN_PATH = "token.json"
DRIVE_CACHE_PATH = "drive_cache.json"
METADATA_DRIVE_FILENAME = "music_metadata.json"
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
    """Extract YouTube video ID from various URL formats or return as is"""
    # If it's already an 11-character YouTube ID
    if re.match(r'^[A-Za-z0-9_-]{11}$', query):
        return query
    
    # Try to extract from YouTube URLs
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
    # If it's a valid YouTube ID, return the URL directly
    if re.match(r'^[A-Za-z0-9_-]{11}$', video_id):
        return f"https://www.youtube.com/watch?v={video_id}"
    
    # If it's not a valid ID, try to search (fallback) - with 10 second timeout
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
                logs.error(f"client_secret.json not found at {CLIENT_SECRET_PATH}")
                return None
            try:
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
                auth_url, _ = flow.authorization_url(prompt='consent')
                logs.info("Authorize this URL in your browser and paste the code here:")
                logs.info(auth_url)
                code = input("Enter authorization code: ").strip()
                flow.fetch_token(code=code)
                creds = flow.credentials
                logs.info("New authorization completed")
            except Exception as e:
                logs.error(f"OAuth flow failed: {e}")
                return None
        
        try:
            os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)
            with open(TOKEN_PATH, "w") as f:
                f.write(creds.to_json())
            logs.info(f"Token saved to {TOKEN_PATH}")
        except Exception as e:
            logs.error(f"Token save failed: {e}")
    
    try:
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        logs.info("Drive service initialized successfully")
        return service
    except Exception as e:
        logs.error(f"Drive service build failed: {e}")
        return None

def search_drive_by_video_id(video_id):
    """Direct search in Drive by video ID without using cache"""
    service = get_drive_service()
    if not service:
        return None
    
    try:
        # Search directly in Drive for files containing the video_id in name
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
        
        # Filter for exact matches and get the first one
        exact_matches = [f for f in files if f['name'].startswith(f"{video_id}.")]
        if exact_matches:
            return exact_matches[0]['id']
        
        return None
    except Exception as e:
        logs.error(f"Drive search error for {video_id}: {e}")
        return None

def download_from_drive(drive_file_id, dest_path):
    """Download file from Drive with better error handling"""
    service = get_drive_service()
    if not service:
        logs.error("Drive service not available for download")
        return False
        
    try:
        try:
            service.files().get(fileId=drive_file_id).execute()
        except Exception as e:
            logs.error(f"File not found on Drive: {drive_file_id} - {e}")
            return False
            
        request = service.files().get_media(fileId=drive_file_id)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        
        with open(dest_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
                
        logs.info(f"Successfully downloaded from Drive: {drive_file_id} -> {dest_path}")
        return True
    except Exception as e:
        logs.error(f"Drive download failed for {drive_file_id}: {e}")
        return False

def upload_to_drive(file_path, video_id):
    """Upload file to Drive with size check"""
    service = get_drive_service()
    if not service:
        logs.error("Drive service not available for upload")
        return None
        
    if not os.path.exists(file_path):
        logs.error(f"File not found for upload: {file_path}")
        return None
    
    # Check file size before upload (120MB limit)
    file_size = os.path.getsize(file_path)
    file_size_mb = file_size / (1024 * 1024)
    
    if file_size_mb > 120:
        logs.error(f"File size {file_size_mb:.2f} MB exceeds 120MB limit. Skipping upload.")
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
        logs.info(f"Successfully uploaded to Drive: {video_id} -> {drive_id} ({file_size_mb:.2f} MB)")
        return drive_id
    except Exception as e:
        logs.error(f"Drive upload failed for {video_id}: {e}")
        return None

def load_drive_cache():
    """Load cache with proper error handling"""
    if not DRIVE_AVAILABLE:
        logs.info("Drive not available, using empty cache")
        return {}
    
    cache_data = {}
    
    if os.path.exists(DRIVE_CACHE_PATH):
        try:
            with open(DRIVE_CACHE_PATH, 'r') as f:
                cache_data = json.load(f)
                logs.info(f"Loaded local cache with {len(cache_data)} entries")
        except Exception as e:
            logs.error(f"Local cache load error: {e}")
            cache_data = {}
    
    return cache_data

def save_drive_cache(cache_data):
    """Save cache with proper error handling"""
    if not DRIVE_AVAILABLE:
        logs.info("Drive not available, cache not saved")
        return False
        
    try:
        os.makedirs(os.path.dirname(DRIVE_CACHE_PATH), exist_ok=True)
        with open(DRIVE_CACHE_PATH, 'w') as f:
            json.dump(cache_data, f, indent=2)
        logs.info(f"Local cache saved with {len(cache_data)} entries")
        return True
    except Exception as e:
        logs.error(f"Critical cache save error: {e}")
        return False

async def download_from_drive_with_fallback(video_id: str, is_video: bool = False):
    """
    Try to download from Drive first. If not found or for videos, return None to use normal API.
    Returns file path if downloaded from Drive, None otherwise.
    """
    # For videos, always use normal API (as per requirement)
    if is_video:
        return None
        
    # For audio, first check Drive
    if not DRIVE_AVAILABLE:
        return None
        
    logs.info(f"Checking Drive for audio: {video_id}")
    
    # First try direct Drive search (fastest)
    drive_file_id = search_drive_by_video_id(video_id)
    if drive_file_id:
        download_folder = "downloads"
        os.makedirs(download_folder, exist_ok=True)
        local_path = f"{download_folder}/{video_id}.mp3"
        
        if download_from_drive(drive_file_id, local_path):
            logs.info(f"Successfully retrieved from Drive (direct search): {local_path}")
            
            # Update cache with the found file
            try:
                cache = load_drive_cache()
                if video_id not in cache:
                    file_size = os.path.getsize(local_path)
                    cache[video_id] = {
                        "drive_file_id": drive_file_id,
                        "uploaded_at": datetime.now().isoformat(),
                        "format": "mp3",
                        "file_size": file_size,
                        "title": "Unknown",
                        "found_via_direct_search": True
                    }
                    save_drive_cache(cache)
                    logs.info(f"Updated cache with direct search result: {video_id}")
            except Exception as e:
                logs.error(f"Cache update after direct search failed: {e}")
            
            return local_path
    
    # Fallback to cache-based approach if direct search fails
    cache = load_drive_cache()
    if video_id in cache:
        drive_file_id = cache[video_id].get("drive_file_id")
        if drive_file_id:
            download_folder = "downloads"
            local_path = f"{download_folder}/{video_id}.mp3"
            
            if download_from_drive(drive_file_id, local_path):
                logs.info(f"Successfully retrieved from Drive (cache): {local_path}")
                return local_path
            else:
                logs.error("Drive download failed, removing from cache")
                try:
                    del cache[video_id]
                    save_drive_cache(cache)
                except Exception as e:
                    logs.error(f"Cache cleanup error: {e}")
    
    return None

async def upload_to_drive_if_needed(file_path: str, video_id: str, is_video: bool = False):
    """
    Upload file to Drive if it's audio and doesn't exist in Drive
    """
    # Only upload audio files (as per requirement)
    if is_video:
        return
        
    if not DRIVE_AVAILABLE:
        return
        
    # Check if already exists in Drive
    existing_file_id = search_drive_by_video_id(video_id)
    if existing_file_id:
        logs.info(f"File already exists in Drive: {video_id}")
        return
        
    # Upload to Drive
    try:
        cache = load_drive_cache()
        if video_id not in cache:
            # Check file size before uploading
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                file_size_mb = file_size / (1024 * 1024)
                
                if file_size_mb <= 120:
                    logs.info(f"Uploading to Drive: {video_id} ({file_size_mb:.2f} MB)")
                    drive_file_id = upload_to_drive(file_path, video_id)
                    if drive_file_id:
                        cache[video_id] = {
                            "drive_file_id": drive_file_id,
                            "uploaded_at": datetime.now().isoformat(),
                            "format": "mp3",
                            "file_size": file_size,
                            "title": "Unknown"
                        }
                        save_drive_cache(cache)
                        logs.info(f"Successfully cached to Drive: {video_id}")
                else:
                    logs.warning(f"Skipping Drive upload - file size {file_size_mb:.2f} MB exceeds 120MB limit")
    except Exception as e:
        logs.error(f"Drive upload failed: {e}")

# ========== END DRIVE FUNCTIONS ==========

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

        # Try each cookie file until one works
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
        
        # If no cookie files worked, try without cookies
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
            "expiry_time": time.time() + 3600,  # Cache for 1 hour
        }  

    return {}

async def download_using_cookies(url: str, video_id: str, is_video: bool = False):
    """Download using yt-dlp with cookies as fallback"""
    cookie_files = get_cookie_files()
    if not cookie_files:
        return None
        
    download_folder = "downloads"
    os.makedirs(download_folder, exist_ok=True)
    
    try:
        if is_video:
            ydl_opts = {
                "format": "(bestvideo[height<=?720][width<=?1280][ext=mp4])+(bestaudio[ext=m4a])",
                "outtmpl": f"{download_folder}/{video_id}.%(ext)s",
                "geo_bypass": True,
                "nocheckcertificate": True,
                "quiet": True,
                "no_warnings": True,
            }
        else:
            ydl_opts = {
                "format": "bestaudio/best",
                "outtmpl": f"{download_folder}/{video_id}.%(ext)s",
                "geo_bypass": True,
                "nocheckcertificate": True,
                "quiet": True,
                "no_warnings": True,
            }
        
        # Try each cookie file
        for cookie_file in cookie_files:
            try:
                current_ydl_opts = ydl_opts.copy()
                current_ydl_opts["cookiefile"] = cookie_file
                
                with yt_dlp.YoutubeDL(current_ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    expected_ext = info.get('ext', 'mp4' if is_video else 'mp3')
                    expected_path = f"{download_folder}/{video_id}.{expected_ext}"
                    
                    if os.path.exists(expected_path):
                        logs.info(f"File already exists: {expected_path}")
                        return expected_path
                    
                    ydl.download([url])
                    
                    if os.path.exists(expected_path):
                        logs.info(f"Download successful: {expected_path}")
                        
                        # For audio files, upload to Drive
                        if not is_video:
                            await upload_to_drive_if_needed(expected_path, video_id, is_video)
                        
                        return expected_path
                        
            except Exception as e:
                logs.warning(f"Download with cookie file {cookie_file} failed: {e}")
                continue
                
    except Exception as e:
        logs.error(f"All cookie downloads failed: {e}")
        
    return None

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
        # Clean expired cache first
        await cleanup_expired_cache()
        
        # Extract video ID from input
        video_id = extract_video_id(id)
        
        # Check cache first - agar cache hai to turant return (fast response)
        cache_key = f"{video_id}_{'video' if video else 'audio'}"
        if cache_key in cache_db:
            cached_data = cache_db[cache_key]
            # Check if cache is still valid
            if cached_data.get("expiry_time", 0) > time.time():
                elapsed_time = time.time() - start_time
                logs.info(f"Returning cached response for ID: {video_id} in {elapsed_time:.2f}s")
                return cached_data["response"]
        
        # ========== DRIVE INTEGRATION ==========
        # For audio: First try to get from Drive
        downloaded_file_path = None
        if not video:  # Audio case only
            downloaded_file_path = await download_from_drive_with_fallback(video_id, video)
            
            if downloaded_file_path:
                # File found in Drive, create streaming response
                ip = await get_public_ip()  
                stream_id = await new_uid()  
                stream_url = f"http://{ip}:8000/stream/{stream_id}"  
                
                # Store file path in database for streaming
                database[stream_id] = {
                    "file_path": downloaded_file_path, 
                    "file_name": f"{video_id}.mp3",
                    "created_time": time.time(),
                    "is_local_file": True  # Mark as local file
                }
                
                # Get basic metadata
                url = await get_youtube_url(video_id)
                metadata = await extract_metadata(url, video) if url else {}
                
                response_data = {  
                    "id": video_id,  
                    "title": metadata.get("title", "Unknown"),  
                    "duration": metadata.get("duration", 0),  
                    "link": metadata.get("link", f"https://www.youtube.com/watch?v={video_id}"),  
                    "channel": metadata.get("channel", "Unknown"),  
                    "views": metadata.get("views", 0),  
                    "thumbnail": metadata.get("thumbnail", ""),  
                    "stream_url": stream_url,  
                    "stream_type": "Audio",  
                    "source": "drive"  # Indicate it came from Drive
                }
                
                # Cache the response
                cache_db[cache_key] = {
                    "response": response_data,
                    "expiry_time": time.time() + 3600
                }
                
                elapsed_time = time.time() - start_time
                logs.info(f"Drive response generated in {elapsed_time:.2f}s")
                return response_data
        # ========== END DRIVE INTEGRATION ==========
        
        # If not found in Drive or it's a video, use normal API flow
        url = await asyncio.wait_for(get_youtube_url(video_id), timeout=12)
        if not url:
            return {"error": "Invalid YouTube ID"}
        
        # Extract metadata with 12 second timeout
        metadata = await asyncio.wait_for(extract_metadata(url, video), timeout=12)
        
        if not metadata or not metadata.get("stream_url"):
            # Try fallback download with cookies
            logs.info(f"Trying fallback download with cookies for: {video_id}")
            downloaded_file_path = await download_using_cookies(url, video_id, video)
            
            if downloaded_file_path:
                # Successfully downloaded via cookies
                ip = await get_public_ip()  
                stream_id = await new_uid()  
                stream_url = f"http://{ip}:8000/stream/{stream_id}"  
                
                database[stream_id] = {
                    "file_path": downloaded_file_path, 
                    "file_name": f"{video_id}.{'mp4' if video else 'mp3'}",
                    "created_time": time.time(),
                    "is_local_file": True
                }
                
                response_data = {  
                    "id": video_id,  
                    "title": metadata.get("title", "Unknown") if metadata else "Unknown",  
                    "duration": metadata.get("duration", 0) if metadata else 0,  
                    "link": url,  
                    "channel": metadata.get("channel", "Unknown") if metadata else "Unknown",  
                    "views": metadata.get("views", 0) if metadata else 0,  
                    "thumbnail": metadata.get("thumbnail", "") if metadata else "",  
                    "stream_url": stream_url,  
                    "stream_type": "Video" if video else "Audio",  
                    "source": "cookies_download"
                }
                
                cache_db[cache_key] = {
                    "response": response_data,
                    "expiry_time": time.time() + 3600
                }
                
                elapsed_time = time.time() - start_time
                logs.info(f"Cookie download response generated in {elapsed_time:.2f}s")
                return response_data
            else:
                return {"error": "Could not fetch stream URL"}
        
        # Normal API flow with stream URL
        extension = "mp3" if not video else "mp4"  
        file_url = metadata.get("stream_url")  
        file_name = f"{metadata.get('id')}.{extension}"  
        ip = await get_public_ip()  
        stream_id = await new_uid()  
        stream_url = f"http://{ip}:8000/stream/{stream_id}"  
        
        # Store in database for streaming
        database[stream_id] = {
            "file_url": file_url, 
            "file_name": file_name,
            "created_time": time.time(),
            "is_local_file": False
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
            "stream_type": metadata.get("stream_type"),  
            "source": "api_stream"
        }
        
        # Cache the response with expiry time
        cache_db[cache_key] = {
            "response": response_data,
            "expiry_time": metadata.get("expiry_time", time.time() + 3600)
        }
        
        elapsed_time = time.time() - start_time
        logs.info(f"API response generated in {elapsed_time:.2f}s")
        
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

    # Handle local file streaming (from Drive or cookie download)
    if file_data.get("is_local_file"):
        file_path = file_data.get("file_path")
        file_name = file_data.get("file_name")
        
        if not file_path or not os.path.exists(file_path):
            return {"error": "File not found!"}
        
        def iterfile():
            with open(file_path, "rb") as f:
                while chunk := f.read(8192):
                    yield chunk
        
        headers = {"Content-Disposition": f"attachment; filename=\"{file_name}\""}
        return StreamingResponse(
            iterfile(),
            media_type="audio/mpeg" if file_name.endswith('.mp3') else "video/mp4",
            headers=headers
        )
    
    # Handle normal URL streaming
    elif file_data.get("file_url"):
        streamer = Streamer()  
        try:  
            headers = {"Content-Disposition": f"attachment; filename=\"{file_data.get('file_name')}\""}  
            return StreamingResponse(  
                streamer.stream_file(file_data.get("file_url")),  
                media_type="application/octet-stream",  
                headers=headers  
            )  
        except Exception as e:  
            logging.error(f"Stream Error: {e}")  
            return {"error": "Something went wrong!"}
    
    else:
        return {"error": "Invalid stream data!"}

# Background task to clean up old stream entries
async def cleanup_old_streams():
    while True:
        await asyncio.sleep(3600)  # Run every hour
        current_time = time.time()
        expired_streams = []
        for stream_id, data in database.items():
            if data.get("created_time", 0) + 7200 < current_time:  # 2 hours old
                expired_streams.append(stream_id)
        
        for stream_id in expired_streams:
            del database[stream_id]
            logs.info(f"Cleaned up old stream: {stream_id}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_old_streams())
    # Initialize Drive service on startup
    if DRIVE_AVAILABLE:
        service = get_drive_service()
        if service:
            logs.info("Drive integration initialized successfully")
        else:
            logs.warning("Drive integration failed to initialize")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
