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
DRIVE_CACHE_PATH = "drive_cache.json"
DRIVE_METADATA_FILE = "api_metadata.json"
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

def search_drive_by_video_id(video_id):
    service = get_drive_service()
    if not service:
        return None
    
    try:
        query = f"name contains '{video_id}' and trashed=false"
        if DRIVE_FOLDER_ID:
            query += f" and '{DRIVE_FOLDER_ID}' in parents"
        
        results = service.files().list(q=query, fields="files(id, name, size, mimeType)", pageSize=10).execute()
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
                
        return True
    except Exception as e:
        logs.error(f"Drive download failed: {e}")
        return False

def upload_to_drive(file_path, video_id, title, file_format, file_size):
    service = get_drive_service()
    if not service or not os.path.exists(file_path):
        return None
    
    file_size_mb = file_size / (1024 * 1024)
    if file_size_mb > 120:
        return None
        
    try:
        file_metadata = {"name": f"{video_id}.{file_format}"}
        if DRIVE_FOLDER_ID:
            file_metadata["parents"] = [DRIVE_FOLDER_ID]
        
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        drive_file_id = file.get("id")
        
        # Update metadata
        if drive_file_id:
            update_drive_metadata(video_id, drive_file_id, title, file_format, file_size)
        
        return drive_file_id
    except Exception as e:
        logs.error(f"Drive upload failed: {e}")
        return None

def load_drive_metadata():
    if not DRIVE_AVAILABLE:
        return {}
    
    # First try to load from local file
    if os.path.exists(DRIVE_METADATA_FILE):
        try:
            with open(DRIVE_METADATA_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    
    # If local file doesn't exist, try to download from Drive
    service = get_drive_service()
    if not service:
        return {}
    
    try:
        query = f"name = '{DRIVE_METADATA_FILE}' and trashed=false"
        if DRIVE_FOLDER_ID:
            query += f" and '{DRIVE_FOLDER_ID}' in parents"
        
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get('files', [])
        
        if files:
            metadata_file_id = files[0]['id']
            request = service.files().get_media(fileId=metadata_file_id)
            file_content = io.BytesIO()
            downloader = MediaIoBaseDownload(file_content, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            
            metadata_content = file_content.getvalue().decode('utf-8')
            metadata = json.loads(metadata_content)
            
            # Save locally for future use
            with open(DRIVE_METADATA_FILE, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            return metadata
    except Exception as e:
        logs.error(f"Drive metadata load failed: {e}")
    
    return {}

def save_drive_metadata(metadata):
    if not DRIVE_AVAILABLE:
        return False
    
    # Save locally
    try:
        with open(DRIVE_METADATA_FILE, 'w') as f:
            json.dump(metadata, f, indent=2)
    except Exception as e:
        logs.error(f"Local metadata save failed: {e}")
        return False
    
    # Upload to Drive
    service = get_drive_service()
    if not service:
        return False
    
    try:
        # Check if metadata file already exists on Drive
        query = f"name = '{DRIVE_METADATA_FILE}' and trashed=false"
        if DRIVE_FOLDER_ID:
            query += f" and '{DRIVE_FOLDER_ID}' in parents"
        
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get('files', [])
        
        file_metadata = {
            'name': DRIVE_METADATA_FILE,
            'mimeType': 'application/json'
        }
        if DRIVE_FOLDER_ID:
            file_metadata['parents'] = [DRIVE_FOLDER_ID]
        
        media = MediaFileUpload(DRIVE_METADATA_FILE, resumable=True, mimetype='application/json')
        
        if files:
            # Update existing file
            file = service.files().update(
                fileId=files[0]['id'],
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
        else:
            # Create new file
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
        
        return True
    except Exception as e:
        logs.error(f"Drive metadata upload failed: {e}")
        return False

def update_drive_metadata(video_id, drive_file_id, title, file_format, file_size):
    metadata = load_drive_metadata()
    
    metadata[video_id] = {
        "drive_file_id": drive_file_id,
        "uploaded_at": datetime.now().isoformat(),
        "format": file_format,
        "file_size": file_size,
        "title": title
    }
    
    save_drive_metadata(metadata)
    return True

def get_drive_file_info(video_id):
    metadata = load_drive_metadata()
    return metadata.get(video_id)

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
                        logging.info(f"Successfully used cookie: {cookie_file}")
                        return metadata
            except Exception as e:
                logging.warning(f"Cookie {cookie_file} failed: {e}")
                continue
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:  
                return ydl.extract_info(url, download=False)
        except Exception as e:
            logging.error(f"Metadata error: {e}")
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
        }  

    return {}

class Streamer:
    def __init__(self):
        self.chunk_size = 1 * 1024 * 1024

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

    async def stream_local_file(self, file_path):
        with open(file_path, "rb") as f:
            while chunk := f.read(self.chunk_size):
                yield chunk

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
        
        # VIDEO REQUEST: Always use direct streaming (no Drive)
        if video:
            url = await get_youtube_url(id)
            metadata = await extract_metadata(url, video)
            
            if not metadata:
                logs.error(f"Metadata extraction failed for {video_id}")
                return {"error": "Could not fetch video info"}

            file_url = metadata.get("stream_url")
            
            if file_url:
                file_extension = "mp4"
                file_name = f"{metadata.get('id')}.{file_extension}"
                
                ip = await get_public_ip()  
                stream_id = await new_uid()  
                stream_url = f"http://{ip}:8000/stream/{stream_id}"  
                database[stream_id] = {"file_url": file_url, "file_name": file_name, "is_local": False}  
                logs.info(f"Database set for {stream_id}: video stream URL")
                logs.info(f"Total database entries: {len(database)}")

                return {  
                    "id": metadata.get("id"),  
                    "title": metadata.get("title"),  
                    "duration": metadata.get("duration"),  
                    "link": metadata.get("link"),  
                    "channel": metadata.get("channel"),  
                    "views": metadata.get("views"),  
                    "thumbnail": metadata.get("thumbnail"),  
                    "stream_url": stream_url,  
                    "stream_type": metadata.get("stream_type"),
                    "source": "stream"
                }
            else:
                return {"error": "No stream URL available for video"}
        
        # AUDIO REQUEST: Check Drive first, then stream/download
        if not video and DRIVE_AVAILABLE:
            # Check if file exists in Drive metadata
            drive_file_info = get_drive_file_info(video_id)
            if drive_file_info:
                logs.info(f"Found in Drive metadata: {video_id}")
                
                download_folder = "downloads"
                os.makedirs(download_folder, exist_ok=True)
                local_path = f"{download_folder}/{video_id}.{drive_file_info['format']}"
                
                # Download from Drive if not exists locally
                if not os.path.exists(local_path):
                    if download_from_drive(drive_file_info['drive_file_id'], local_path):
                        logs.info(f"Downloaded from Drive: {video_id}")
                    else:
                        logs.error(f"Failed to download from Drive: {video_id}")
                        drive_file_info = None
                
                if drive_file_info and os.path.exists(local_path):
                    ip = await get_public_ip()
                    stream_id = await new_uid()
                    stream_url = f"http://{ip}:8000/stream/{stream_id}"
                    
                    database[stream_id] = {"file_path": local_path, "file_name": f"{video_id}.{drive_file_info['format']}", "is_local": True}
                    logs.info(f"Database set for {stream_id}: drive file")
                    logs.info(f"Total database entries: {len(database)}")
                    
                    url = f"https://www.youtube.com/watch?v={video_id}"
                    metadata = await extract_metadata(url, video)
                    
                    return {
                        "id": video_id,
                        "title": drive_file_info.get("title", metadata.get("title", "Unknown")),
                        "duration": metadata.get("duration", 0),
                        "link": url,
                        "channel": metadata.get("channel", "Unknown"),
                        "views": metadata.get("views", 0),
                        "thumbnail": metadata.get("thumbnail", ""),
                        "stream_url": stream_url,
                        "stream_type": "Audio",
                        "source": "drive"
                    }
            
            # Check local downloads
            download_folder = "downloads"
            os.makedirs(download_folder, exist_ok=True)
            
            for ext in ["mp3", "m4a", "webm"]:
                local_path = f"{download_folder}/{video_id}.{ext}"
                if os.path.exists(local_path):
                    ip = await get_public_ip()
                    stream_id = await new_uid()
                    stream_url = f"http://{ip}:8000/stream/{stream_id}"
                    
                    database[stream_id] = {"file_path": local_path, "file_name": os.path.basename(local_path), "is_local": True}
                    logs.info(f"Database set for {stream_id}: local file")
                    logs.info(f"Total database entries: {len(database)}")
                    
                    url = f"https://www.youtube.com/watch?v={video_id}"
                    metadata = await extract_metadata(url, video)
                    
                    # Upload to Drive in background if not already there
                    if DRIVE_AVAILABLE and not get_drive_file_info(video_id):
                        file_size = os.path.getsize(local_path)
                        asyncio.create_task(upload_to_drive_async(local_path, video_id, metadata.get("title", "Unknown"), ext, file_size))
                    
                    return {
                        "id": video_id,
                        "title": metadata.get("title", "Unknown"),
                        "duration": metadata.get("duration", 0),
                        "link": url,
                        "channel": metadata.get("channel", "Unknown"),
                        "views": metadata.get("views", 0),
                        "thumbnail": metadata.get("thumbnail", ""),
                        "stream_url": stream_url,
                        "stream_type": "Audio",
                        "source": "local"
                    }
        
        # Direct streaming for audio (when not in Drive and not locally downloaded)
        url = await get_youtube_url(id)
        metadata = await extract_metadata(url, video)
        
        if not metadata:
            logs.error(f"Metadata extraction failed for {video_id}")
            return {"error": "Could not fetch video info"}

        file_url = metadata.get("stream_url")
        
        if file_url:
            file_extension = "mp3"
            file_name = f"{metadata.get('id')}.{file_extension}"
            
            ip = await get_public_ip()  
            stream_id = await new_uid()  
            stream_url = f"http://{ip}:8000/stream/{stream_id}"  
            database[stream_id] = {"file_url": file_url, "file_name": file_name, "is_local": False}  
            logs.info(f"Database set for {stream_id}: audio stream URL")
            logs.info(f"Total database entries: {len(database)}")

            return {  
                "id": metadata.get("id"),  
                "title": metadata.get("title"),  
                "duration": metadata.get("duration"),  
                "link": metadata.get("link"),  
                "channel": metadata.get("channel"),  
                "views": metadata.get("views"),  
                "thumbnail": metadata.get("thumbnail"),  
                "stream_url": stream_url,  
                "stream_type": metadata.get("stream_type"),
                "source": "stream"
            }
        else:
            # Download audio and upload to Drive
            download_folder = "downloads"
            os.makedirs(download_folder, exist_ok=True)
            cookie_files = get_cookie_files()
            downloaded_file = None
            downloaded_format = None
            
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
                    }
                    
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=False)
                        expected_ext = info.get('ext', 'mp3')
                        expected_path = f"{download_folder}/{video_id}.{expected_ext}"
                        
                        if os.path.exists(expected_path):
                            downloaded_file = expected_path
                            downloaded_format = expected_ext
                            break
                        
                        ydl.download([url])
                        
                        if os.path.exists(expected_path):
                            downloaded_file = expected_path
                            downloaded_format = expected_ext
                            break
                            
                except Exception as e:
                    logs.warning(f"Cookie {cookie_file} failed: {e}")
                    continue
            
            if downloaded_file:
                ip = await get_public_ip()
                stream_id = await new_uid()
                stream_url = f"http://{ip}:8000/stream/{stream_id}"
                
                database[stream_id] = {"file_path": downloaded_file, "file_name": os.path.basename(downloaded_file), "is_local": True}
                logs.info(f"Database set for {stream_id}: downloaded file")
                logs.info(f"Total database entries: {len(database)}")
                
                # Upload to Drive in background
                if DRIVE_AVAILABLE:
                    file_size = os.path.getsize(downloaded_file)
                    asyncio.create_task(upload_to_drive_async(downloaded_file, video_id, metadata.get("title", "Unknown"), downloaded_format, file_size))
                
                return {
                    "id": video_id,
                    "title": metadata.get("title", "Unknown"),
                    "duration": metadata.get("duration", 0),
                    "link": url,
                    "channel": metadata.get("channel", "Unknown"),
                    "views": metadata.get("views", 0),
                    "thumbnail": metadata.get("thumbnail", ""),
                    "stream_url": stream_url,
                    "stream_type": "Audio",
                    "source": "download"
                }
            
            return {"error": "Download failed"}
            
    except Exception as e:  
        logs.error(f"Error: {e}")  
        return {"error": str(e)}

async def upload_to_drive_async(file_path, video_id, title, file_format, file_size):
    """Upload file to Drive in background"""
    try:
        drive_file_id = upload_to_drive(file_path, video_id, title, file_format, file_size)
        if drive_file_id:
            logs.info(f"Successfully uploaded to Drive: {video_id}")
        else:
            logs.warning(f"Failed to upload to Drive: {video_id}")
    except Exception as e:
        logs.error(f"Background upload failed: {e}")

@app.get("/stream/{stream_id}")
async def stream_from_stream_url(stream_id: str):
    logs.info(f"Stream request: {stream_id}")
    logs.info(f"Database has {len(database)} entries")
    
    file_data = database.get(stream_id)
    if not file_data:
        logs.error(f"Stream ID not found: {stream_id}")
        logs.error(f"Available IDs: {list(database.keys())}")
        return {"error": "Invalid stream request!"}

    if file_data.get("is_local"):
        file_path = file_data.get("file_path")
        file_name = file_data.get("file_name")
        
        if not file_path or not os.path.exists(file_path):
            return {"error": "File not found!"}
        
        streamer = Streamer()
        media_type = "audio/mpeg" if file_name.endswith('.mp3') else "audio/mp4" if file_name.endswith('.m4a') else "audio/webm"
        headers = {"Content-Disposition": f"attachment; filename=\"{file_name}\""}
        
        return StreamingResponse(streamer.stream_local_file(file_path), media_type=media_type, headers=headers)
    
    file_url = file_data.get("file_url")
    file_name = file_data.get("file_name")
    
    if not file_url or not file_name:
        return {"error": "Invalid stream request!"}

    streamer = Streamer()  
    try:  
        headers = {"Content-Disposition": f"attachment; filename=\"{file_name}\""}  
        return StreamingResponse(streamer.stream_file(file_url), media_type="application/octet-stream", headers=headers)  
    except Exception as e:  
        logging.error(f"Stream Error: {e}")  
        return {"error": "Something went wrong!"}

@app.on_event("startup")
async def startup_event():
    if DRIVE_AVAILABLE:
        service = get_drive_service()
        if service:
            logs.info("Drive ready")
            # Load metadata on startup
            metadata = load_drive_metadata()
            logs.info(f"Loaded {len(metadata)} entries from Drive metadata")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
