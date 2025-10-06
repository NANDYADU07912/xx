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

API_KEY = "ShrutiMusic"

CLIENT_SECRET_PATH = "client_secret.json"
TOKEN_PATH = "token.json"
METADATA_FILE = "api_metadata.json"
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
        async with httpx.AsyncClient(timeout=None) as client:
            response = await client.get('https://api.ipify.org')
            public_ip = response.text
            ip_address["ip_address"] = public_ip
            return public_ip
    except:
        return "localhost"

async def get_youtube_url(video_id: str) -> str:
    if bool(re.match(r'^(https?://)?(www.)?(youtube.com|youtu.be)/(?:watch?v=|embed/|v/|shorts|live/)?([A-Za-z0-9_-]{11})(?:[?&].*)?$', video_id)):
        match = re.search(r'(?:v=|/(?:embed|v|shorts|live)/|youtu.be/)([A-Za-z0-9_-]{11})', video_id)
        if match:
            return f"https://www.youtube.com/watch?v={match.group(1)}"

    try:  
        search = VideosSearch(video_id, limit=1)  
        result = await search.next()  
        return result["result"][0]["link"]  
    except Exception:  
        return ""

def get_cookie_files():
    cookies_dir = "cookies"
    if not os.path.exists(cookies_dir):
        os.makedirs(cookies_dir)
        return []
    
    cookie_files = glob.glob(os.path.join(cookies_dir, "*.txt"))
    return cookie_files

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
            except Exception as e:
                logs.error(f"Token refresh failed: {e}")
                creds = None
        else:
            if not os.path.exists(CLIENT_SECRET_PATH):
                return None
            try:
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
                auth_url, _ = flow.authorization_url(prompt='consent')
                logs.info(auth_url)
                code = input("Enter authorization code: ").strip()
                flow.fetch_token(code=code)
                creds = flow.credentials
            except Exception as e:
                logs.error(f"OAuth flow failed: {e}")
                return None
        
        try:
            with open(TOKEN_PATH, "w") as f:
                f.write(creds.to_json())
        except Exception as e:
            logs.error(f"Token save failed: {e}")
    
    try:
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception as e:
        logs.error(f"Drive service failed: {e}")
        return None

def load_metadata():
    """Load metadata from api_metadata.json"""
    if not os.path.exists(METADATA_FILE):
        return {}
    try:
        with open(METADATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logs.error(f"Metadata load error: {e}")
        return {}

def save_metadata(metadata):
    """Save metadata to api_metadata.json"""
    try:
        with open(METADATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        logs.error(f"Metadata save error: {e}")
        return False

def download_from_drive(drive_file_id, dest_path):
    """Download file from Google Drive"""
    service = get_drive_service()
    if not service:
        return False
        
    try:
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

def upload_to_drive(file_path, video_id, file_format):
    """Upload file to Google Drive"""
    service = get_drive_service()
    if not service or not os.path.exists(file_path):
        return None
    
    file_size = os.path.getsize(file_path)
    file_size_mb = file_size / (1024 * 1024)
    
    if file_size_mb > 120:
        logs.warning(f"File too large for Drive: {file_size_mb:.2f}MB")
        return None
        
    try:
        file_metadata = {"name": f"{video_id}.{file_format}"}
        if DRIVE_FOLDER_ID:
            file_metadata["parents"] = [DRIVE_FOLDER_ID]
        
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        drive_file_id = file.get("id")
        logs.info(f"Uploaded to Drive: {video_id}.{file_format} -> {drive_file_id}")
        return drive_file_id
    except Exception as e:
        logs.error(f"Drive upload failed: {e}")
        return None

async def extract_metadata(url: str, video: bool = False):
    """Extract metadata from YouTube URL"""
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
            "extract_flat": False,  
            "skip_download": True,  
            "ignoreerrors": True,
        }  

        for cookie_file in cookie_files:
            try:
                current_ydl_opts = ydl_opts.copy()
                current_ydl_opts["cookiefile"] = cookie_file
                
                with yt_dlp.YoutubeDL(current_ydl_opts) as ydl:  
                    metadata = ydl.extract_info(url, download=False)
                    if metadata and metadata.get('url'):
                        logs.info(f"Successfully used cookie: {cookie_file}")
                        return metadata
            except Exception as e:
                logs.warning(f"Cookie {cookie_file} failed: {e}")
                continue
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:  
                return ydl.extract_info(url, download=False)
        except Exception as e:
            logs.error(f"Metadata error: {e}")
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
            "ext": metadata.get("ext", "mp3"),
            "stream_type": "Video" if video else "Audio",  
        }  

    return {}

async def download_audio_with_ytdlp(url, video_id, cookie_files):
    """Download audio using yt-dlp with cookies"""
    download_folder = "downloads"
    os.makedirs(download_folder, exist_ok=True)
    
    def sync_download():
        for cookie_file in cookie_files:
            try:
                ydl_opts = {
                    "format": "bestaudio/best",
                    "outtmpl": f"{download_folder}/{video_id}.%(ext)s",
                    "geo_bypass": True,
                    "nocheckcertificate": True,
                    "quiet": True,
                    "no_warnings": True,
                    "cookiefile": cookie_file,
                    "postprocessors": [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '192',
                    }] if os.path.exists('/usr/bin/ffmpeg') else [],
                }
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    ext = info.get('ext', 'webm')
                    
                    # Check for converted mp3 first
                    mp3_path = f"{download_folder}/{video_id}.mp3"
                    if os.path.exists(mp3_path):
                        return mp3_path, 'mp3', info.get('title', 'Unknown')
                    
                    # Check for original format
                    original_path = f"{download_folder}/{video_id}.{ext}"
                    if os.path.exists(original_path):
                        return original_path, ext, info.get('title', 'Unknown')
                    
                    # Check any file with video_id
                    for file in glob.glob(f"{download_folder}/{video_id}.*"):
                        file_ext = file.split('.')[-1]
                        return file, file_ext, info.get('title', 'Unknown')
                        
                    return None, None, None
                    
            except Exception as e:
                logs.warning(f"Download with cookie {cookie_file} failed: {e}")
                continue
        
        return None, None, None
    
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, sync_download)

class Streamer:
    def __init__(self):
        self.chunk_size = 1 * 1024 * 1024

    async def stream_local_file(self, file_path):
        """Stream local file"""
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(self.chunk_size):
                    yield chunk
        except Exception as e:
            logs.error(f"Stream error: {e}")

    async def get_total_chunks(self, file_url):  
        async with httpx.AsyncClient() as client:  
            response = await client.head(file_url)  
            file_size = response.headers.get("Content-Length")  
            return (int(file_size) + self.chunk_size - 1) // self.chunk_size if file_size else None  

    async def fetch_chunk(self, file_url, chunk_id):  
        start_byte = chunk_id * self.chunk_size  
        async with httpx.AsyncClient(follow_redirects=True, timeout=10) as client:  
            headers = {"Range": f"bytes={start_byte}-{start_byte + self.chunk_size - 1}", "User-Agent": "Mozilla/5.0"}  
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

def extract_video_id(text: str) -> str:
    match = re.search(r'(?:v=|/(?:embed|v|shorts|live)/|youtu.be/)([A-Za-z0-9_-]{11})', text)
    if match:
        return match.group(1)
    return text

@app.get("/youtube")
async def get_youtube_info(id: str, video: bool = False, user: str = Security(get_user)):
    try:
        video_id = extract_video_id(id)
        logs.info(f"Processing: {video_id} (video={video})")
        
        # For VIDEO requests - always use direct stream, no Drive
        if video:
            url = await get_youtube_url(id)
            metadata = await extract_metadata(url, video=True)
            
            if not metadata or not metadata.get("stream_url"):
                return {"error": "Could not fetch video info"}
            
            file_url = metadata.get("stream_url")
            extension = metadata.get("ext", "mp4")
            file_name = f"{video_id}.{extension}"
            
            ip = await get_public_ip()
            stream_id = await new_uid()
            stream_url = f"http://{ip}:8000/stream/{stream_id}"
            
            database[stream_id] = {
                "file_url": file_url, 
                "file_name": file_name, 
                "is_local": False
            }
            logs.info(f"Video stream setup: {stream_id}")
            
            return {
                "id": video_id,
                "title": metadata.get("title", "Unknown"),
                "duration": metadata.get("duration", 0),
                "link": metadata.get("link"),
                "channel": metadata.get("channel", "Unknown"),
                "views": metadata.get("views", 0),
                "thumbnail": metadata.get("thumbnail", ""),
                "stream_url": stream_url,
                "stream_type": "Video",
                "source": "direct_stream"
            }
        
        # For AUDIO requests - check Drive first
        api_metadata = load_metadata()
        
        # Check if video_id exists in metadata
        if video_id in api_metadata and DRIVE_AVAILABLE:
            metadata_entry = api_metadata[video_id]
            drive_file_id = metadata_entry.get("drive_file_id")
            file_format = metadata_entry.get("format", "mp3")
            
            download_folder = "downloads"
            os.makedirs(download_folder, exist_ok=True)
            local_path = f"{download_folder}/{video_id}.{file_format}"
            
            # Download from Drive if not in local
            if not os.path.exists(local_path):
                logs.info(f"Downloading from Drive: {video_id}")
                if not download_from_drive(drive_file_id, local_path):
                    logs.error(f"Drive download failed for {video_id}")
                    # Remove from metadata if download fails
                    del api_metadata[video_id]
                    save_metadata(api_metadata)
                else:
                    logs.info(f"Successfully downloaded from Drive: {video_id}")
            
            # If file exists locally now, stream it
            if os.path.exists(local_path):
                ip = await get_public_ip()
                stream_id = await new_uid()
                stream_url = f"http://{ip}:8000/stream/{stream_id}"
                
                database[stream_id] = {
                    "file_path": local_path,
                    "file_name": f"{video_id}.{file_format}",
                    "is_local": True
                }
                logs.info(f"Streaming from Drive cache: {stream_id}")
                
                url = f"https://www.youtube.com/watch?v={video_id}"
                yt_metadata = await extract_metadata(url, video=False)
                
                return {
                    "id": video_id,
                    "title": metadata_entry.get("title", yt_metadata.get("title", "Unknown")),
                    "duration": yt_metadata.get("duration", 0),
                    "link": url,
                    "channel": yt_metadata.get("channel", "Unknown"),
                    "views": yt_metadata.get("views", 0),
                    "thumbnail": yt_metadata.get("thumbnail", ""),
                    "stream_url": stream_url,
                    "stream_type": "Audio",
                    "source": "drive"
                }
        
        # If not in Drive metadata or Drive not available, download with yt-dlp
        logs.info(f"Not found in Drive, downloading: {video_id}")
        url = await get_youtube_url(id)
        cookie_files = get_cookie_files()
        
        # Download the audio file
        downloaded_file, file_format, title = await download_audio_with_ytdlp(url, video_id, cookie_files)
        
        if not downloaded_file or not os.path.exists(downloaded_file):
            logs.error(f"Download failed for {video_id}")
            return {"error": "Download failed"}
        
        logs.info(f"Downloaded: {downloaded_file}")
        
        # Upload to Drive in background
        if DRIVE_AVAILABLE:
            file_size = os.path.getsize(downloaded_file)
            drive_file_id = upload_to_drive(downloaded_file, video_id, file_format)
            
            if drive_file_id:
                # Update metadata
                api_metadata[video_id] = {
                    "drive_file_id": drive_file_id,
                    "uploaded_at": datetime.now().isoformat(),
                    "format": file_format,
                    "file_size": file_size,
                    "title": title
                }
                save_metadata(api_metadata)
                logs.info(f"Added to metadata: {video_id}")
        
        # Stream the downloaded file
        ip = await get_public_ip()
        stream_id = await new_uid()
        stream_url = f"http://{ip}:8000/stream/{stream_id}"
        
        database[stream_id] = {
            "file_path": downloaded_file,
            "file_name": os.path.basename(downloaded_file),
            "is_local": True
        }
        logs.info(f"Streaming downloaded file: {stream_id}")
        
        yt_metadata = await extract_metadata(url, video=False)
        
        return {
            "id": video_id,
            "title": title or yt_metadata.get("title", "Unknown"),
            "duration": yt_metadata.get("duration", 0),
            "link": url,
            "channel": yt_metadata.get("channel", "Unknown"),
            "views": yt_metadata.get("views", 0),
            "thumbnail": yt_metadata.get("thumbnail", ""),
            "stream_url": stream_url,
            "stream_type": "Audio",
            "source": "downloaded"
        }
            
    except Exception as e:  
        logs.error(f"Error: {e}", exc_info=True)  
        return {"error": str(e)}

@app.get("/stream/{stream_id}")
async def stream_from_stream_url(stream_id: str):
    logs.info(f"Stream request: {stream_id}")
    
    file_data = database.get(stream_id)
    if not file_data:
        logs.error(f"Stream ID not found: {stream_id}")
        return {"error": "Invalid stream request!"}

    streamer = Streamer()
    
    # Local file streaming
    if file_data.get("is_local"):
        file_path = file_data.get("file_path")
        file_name = file_data.get("file_name")
        
        if not file_path or not os.path.exists(file_path):
            logs.error(f"File not found: {file_path}")
            return {"error": "File not found!"}
        
        media_type = "audio/mpeg" if file_name.endswith('.mp3') else "audio/webm" if file_name.endswith('.webm') else "video/mp4"
        headers = {"Content-Disposition": f"attachment; filename=\"{file_name}\""}
        
        return StreamingResponse(
            streamer.stream_local_file(file_path), 
            media_type=media_type, 
            headers=headers
        )
    
    # Direct URL streaming
    file_url = file_data.get("file_url")
    file_name = file_data.get("file_name")
    
    if not file_url or not file_name:
        return {"error": "Invalid stream request!"}

    try:  
        media_type = "video/mp4" if file_name.endswith('.mp4') else "application/octet-stream"
        headers = {"Content-Disposition": f"attachment; filename=\"{file_name}\""}  
        return StreamingResponse(
            streamer.stream_file(file_url), 
            media_type=media_type, 
            headers=headers
        )  
    except Exception as e:  
        logs.error(f"Stream Error: {e}")  
        return {"error": "Something went wrong!"}

@app.on_event("startup")
async def startup_event():
    if DRIVE_AVAILABLE:
        service = get_drive_service()
        if service:
            logs.info("Drive ready")
            metadata = load_metadata()
            logs.info(f"Loaded {len(metadata)} entries from metadata")
    else:
        logs.warning("Drive not available")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
