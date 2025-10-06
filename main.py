import aiohttp, asyncio, httpx, logging, re, uuid, uvicorn, yt_dlp, os, glob, time, json
from fastapi import FastAPI, HTTPException, Security
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyQuery
from logging.handlers import RotatingFileHandler
from youtubesearchpython.__future__ import VideosSearch

# --- Google Drive Imports ---
# NOTE: pydrive2 handles file cache issue better than older google-api-python-client versions.
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO
# ----------------------------

# --- Configuration ---
CLIENT_SECRET_PATH = "client_secret.json"
TOKEN_PATH = "token.json"
DRIVE_CACHE_PATH = "drive_cache.json" # Not directly used with pydrive2 but kept for context
METADATA_DRIVE_FILENAME = "api_metadata.json"
DRIVE_FOLDER_ID = None # Set this if you want to upload to a specific folder
SCOPES = ['https://www.googleapis.com/auth/drive.file']
# ---------------------

# --- Logging Setup ---
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
logging.getLogger("googleapiclient").setLevel(logging.WARNING) # Suppress Drive API logs

logs = logging.getLogger(__name__)
# ---------------------

app = FastAPI()
database = {}
ip_address = {}
cache_db = {}
metadata_db = {} # In-memory metadata from api_metadata.json

# Single API key
API_KEY = "ShrutiMusic"

api_key_query = APIKeyQuery(name="api_key", auto_error=True)

# --- Global Drive Objects ---
gauth = None
drive = None
drive_service = None
# ----------------------------

# --- Drive Functions ---

def load_drive_credentials():
    """Authenticates with Google Drive and returns the GoogleDrive object."""
    global drive, gauth, drive_service
    try:
        gauth = GoogleAuth()
        
        # Try to load saved credentials
        if os.path.exists(TOKEN_PATH):
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        else:
            creds = None

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
                # Note: Run this interactively once to create token.json
                # For non-interactive deployment, you need to manually handle the flow
                creds = flow.run_local_server(port=0) 
            
            # Save the credentials for the next run
            with open(TOKEN_PATH, 'w') as token:
                token.write(creds.to_json())

        # Set credentials for pydrive2
        gauth.credentials = creds
        drive = GoogleDrive(gauth)
        
        # Build the service for more direct API calls (like downloading chunks)
        drive_service = build('drive', 'v3', credentials=creds)

        logs.info("Google Drive authentication successful.")
        return True
    except Exception as e:
        logs.error(f"Google Drive authentication failed: {e}")
        return False

def get_metadata_file_id() -> str | None:
    """Searches for the metadata file in Drive."""
    try:
        file_list = drive.ListFile({'q': f"title='{METADATA_DRIVE_FILENAME}' and trashed=false"}).GetList()
        if file_list:
            return file_list[0]['id']
    except Exception as e:
        logs.error(f"Error searching for metadata file: {e}")
    return None

def download_metadata_from_drive(file_id: str):
    """Downloads and loads the metadata from Drive."""
    global metadata_db
    try:
        logs.info(f"Downloading metadata file: {file_id}")
        file = drive.CreateFile({'id': file_id})
        content = file.GetContentString()
        metadata_db = json.loads(content)
        logs.info(f"Successfully loaded {len(metadata_db)} entries from Drive metadata.")
    except Exception as e:
        logs.error(f"Error downloading or loading metadata: {e}")
        metadata_db = {}

def upload_file_to_drive(local_path: str, title: str) -> tuple[str | None, int]:
    """Uploads a file to Drive and returns its ID and size."""
    try:
        logs.info(f"Starting upload of {title} to Google Drive...")
        
        file_size = os.path.getsize(local_path)
        
        file_metadata = {'title': title}
        if DRIVE_FOLDER_ID:
            file_metadata['parents'] = [{'id': DRIVE_FOLDER_ID}]
            
        file = drive.CreateFile(file_metadata)
        file.SetContentFile(local_path)
        file.Upload()
        
        logs.info(f"Successfully uploaded {title} (ID: {file['id']})")
        return file['id'], file_size
    except Exception as e:
        logs.error(f"Error uploading file to Drive: {e}")
        return None, 0

def update_metadata_on_drive(metadata_file_id: str | None, new_metadata: dict):
    """Updates the in-memory metadata and uploads it back to Drive."""
    global metadata_db
    metadata_db.update(new_metadata)

    try:
        json_content = json.dumps(metadata_db, indent=4)
        
        if metadata_file_id:
            # Update existing file
            file = drive.CreateFile({'id': metadata_file_id})
            file.SetContentString(json_content)
            file.Upload()
            logs.info("Successfully updated existing metadata file on Drive.")
        else:
            # Create new file
            file = drive.CreateFile({'title': METADATA_DRIVE_FILENAME})
            file.SetContentString(json_content)
            file.Upload()
            logs.info("Successfully created new metadata file on Drive.")
        
        return file['id']
    except Exception as e:
        logs.error(f"Error updating metadata on Drive: {e}")
        return None

# --- Core API Functions (Unchanged) ---
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

async def extract_metadata_and_download(url: str, video: bool = False):
    """
    Extracts metadata, and for audio, conditionally downloads the file.
    Returns: metadata dict, local_audio_file_path (or None)
    """
    if not url:
        return {}, None

    format_type = "best" if video else "bestaudio/best"  
    cookie_files = get_cookie_files()
    temp_filename = None
    
    # Only download for audio requests
    if not video:
        temp_filename = f"{uuid.uuid4()}.temp"

    def sync_extract_metadata_and_download():  
        ydl_opts = {  
            "format": format_type,  
            "no_warnings": True,  
            "simulate": True,  
            "quiet": True,  
            "noplaylist": True,  
            "extract_flat": True,  
            "force_generic_extractor": True,  
            "ignoreerrors": True,
            "skip_download": video, # Skip download for video
        }
        
        if not video and temp_filename:
            ydl_opts["outtmpl"] = temp_filename
            ydl_opts["skip_download"] = False # Do download for audio
            # yt-dlp might use bestaudio format which is often webm or m4a. 
            # We'll rely on the actual downloaded extension later if needed.
        
        # Try each cookie file until one works
        for cookie_file in cookie_files:
            try:
                current_ydl_opts = ydl_opts.copy()
                current_ydl_opts["cookiefile"] = cookie_file
                
                with yt_dlp.YoutubeDL(current_ydl_opts) as ydl:  
                    metadata = ydl.extract_info(url, download=(not video))
                    if metadata and metadata.get('url') and (video or os.path.exists(temp_filename)):
                        logging.info(f"Successfully used cookie file: {cookie_file}")
                        return metadata
            except Exception as e:
                # logs.warning(f"Cookie file {cookie_file} failed: {e}") # Too chatty
                continue
        
        # If no cookie files worked, try without cookies
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:  
                return ydl.extract_info(url, download=(not video))
        except Exception as e:
            logging.error(f"Metadata extraction/download error without cookies: {e}")
            return {}

    loop = asyncio.get_running_loop()  
    metadata = await loop.run_in_executor(None, sync_extract_metadata_and_download)  

    if metadata:  
        # Find the actual downloaded file path for audio
        if not video and temp_filename:
            downloaded_files = glob.glob(f"{os.path.splitext(temp_filename)[0]}.*")
            actual_file_path = downloaded_files[0] if downloaded_files else None
        else:
            actual_file_path = None
        
        # Determine stream_url for non-audio (yt-dlp stream) or None for audio (downloaded)
        stream_url = metadata.get("url") if video else None
        
        return {  
            "id": metadata.get("id"),  
            "title": metadata.get("title"),  
            "duration": metadata.get("duration"),  
            "link": metadata.get("webpage_url"),  
            "channel": metadata.get("channel", "Unknown"),  
            "views": metadata.get("view_count"),  
            "thumbnail": metadata.get("thumbnail"),  
            "stream_url": stream_url,  
            "stream_type": "Video" if video else "Audio",  
            "expiry_time": time.time() + 3600,  # Cache for 1 hour
        }, actual_file_path

    return {}, None


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

# --- Streamer Class (Modified for Drive) ---
class Streamer:
    def __init__(self):
        self.chunk_size = 1 * 1024 * 1024

    async def get_total_chunks(self, file_url=None, drive_file_id=None, file_size=None):
        if drive_file_id and file_size:
            return (int(file_size) + self.chunk_size - 1) // self.chunk_size
        
        if file_url:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.head(file_url)  
                file_size = response.headers.get("Content-Length")  
                return (int(file_size) + self.chunk_size - 1) // self.chunk_size if file_size else None  
        return None

    async def fetch_chunk(self, file_url=None, drive_file_id=None, chunk_id=0):  
        start_byte = chunk_id * self.chunk_size  
        end_byte = start_byte + self.chunk_size - 1
        
        if drive_file_id and drive_service:
            # Stream from Drive
            loop = asyncio.get_running_loop()
            def sync_drive_download():
                request = drive_service.files().get_media(fileId=drive_file_id)
                request.headers['Range'] = f'bytes={start_byte}-{end_byte}'
                
                fh = BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                return fh.getvalue()

            try:
                return await loop.run_in_executor(None, sync_drive_download)
            except Exception as e:
                logs.error(f"Error fetching Drive chunk {chunk_id}: {e}")
                return None
        
        if file_url:
            # Stream from YT-DLP URL
            async with httpx.AsyncClient(follow_redirects=True, timeout=10) as client:
                headers = {  
                    "Range": f"bytes={start_byte}-{end_byte}",  
                    "User-Agent": "Mozilla/5.0"  
                }  
                response = await client.get(file_url, headers=headers)  
                return response.content if response.status_code in {206, 200} else None
        
        return None

    async def stream_file(self, file_url=None, drive_file_id=None, file_size=None):  
        # Use one of the streaming methods
        if drive_file_id:
            total_chunks = await self.get_total_chunks(drive_file_id=drive_file_id, file_size=file_size)
            stream_func = lambda cid: self.fetch_chunk(drive_file_id=drive_file_id, chunk_id=cid)
        elif file_url:
            total_chunks = await self.get_total_chunks(file_url=file_url)
            stream_func = lambda cid: self.fetch_chunk(file_url=file_url, chunk_id=cid)
        else:
            return

        received_chunks = set()  
        chunk_id = 0  
        
        # Simple streaming loop (not fully optimized for parallel but safer)
        while total_chunks is None or chunk_id < total_chunks:  
            current_chunk = await stream_func(chunk_id)
            if current_chunk:
                received_chunks.add(chunk_id)
                yield current_chunk
            else:
                # If a chunk fails, try to skip it or break if total_chunks is known
                if total_chunks is not None:
                    logs.warning(f"Failed to fetch chunk {chunk_id}. Skipping.")
                elif chunk_id > 10: # Stop trying to stream if too many initial chunks fail
                    break
            
            chunk_id += 1
            # Add a small delay for large files to prevent overwhelming the source
            if not drive_file_id: # Only for yt-dlp URL streaming
                await asyncio.sleep(0.001)

# --- API Endpoints (Modified) ---
@app.get("/youtube")
async def get_youtube_info(id: str, video: bool = False, user: str = Security(get_user)):
    start_time = time.time()
    
    try:
        await cleanup_expired_cache()
        video_id = extract_video_id(id)
        
        # 1. Check Drive Metadata (Audio Only)
        source = "yt-dlp"
        drive_metadata = metadata_db.get(video_id)
        
        if not video and drive_metadata:
            logs.info(f"Found audio in Drive metadata for ID: {video_id}. Streaming from Drive.")
            
            # --- Stream from Drive ---
            ip = await get_public_ip()  
            stream_id = await new_uid()  
            stream_url = f"http://{ip}:8000/stream/{stream_id}"
            extension = drive_metadata.get("format", "mp3")

            database[stream_id] = {
                "drive_file_id": drive_metadata["drive_file_id"], 
                "file_name": f"{video_id}.{extension}",
                "file_size": drive_metadata["file_size"],
                "created_time": time.time()
            }
            
            response_data = {  
                "id": video_id,  
                "title": drive_metadata.get("title", "Unknown"),  
                "duration": None, # Duration is not stored in your example metadata
                "link": f"https://www.youtube.com/watch?v={video_id}",  
                "channel": "Unknown",  
                "views": None,  
                "thumbnail": None,  
                "stream_url": stream_url,  
                "stream_type": "Audio",
                "source": "Drive" # Source added
            }
            
            elapsed_time = time.time() - start_time
            logs.info(f"Drive response generated in {elapsed_time:.2f}s")
            return response_data
        
        # 2. Check Cache (yt-dlp stream URL)
        cache_key = f"{video_id}_{'video' if video else 'audio'}"
        if cache_key in cache_db and cache_db[cache_key].get("expiry_time", 0) > time.time():
            elapsed_time = time.time() - start_time
            logs.info(f"Returning cached response for ID: {video_id} in {elapsed_time:.2f}s")
            return cache_db[cache_key]["response"]
        
        # 3. Use YT-DLP (Fetch URL/Download)
        url = await asyncio.wait_for(get_youtube_url(video_id), timeout=12)
        if not url:
            return {"error": "Invalid YouTube ID"}
        
        metadata, local_file_path = await asyncio.wait_for(
            extract_metadata_and_download(url, video), timeout=30 # Increased timeout for download
        )
        
        if not metadata or (video and not metadata.get("stream_url")):
            return {"error": "Could not fetch stream URL or download audio"}
        
        # --- Handle Audio Upload and Stream URL ---
        file_url = metadata.get("stream_url")
        if not video and local_file_path:
            # Upload to Drive
            filename = os.path.basename(local_file_path)
            drive_id, file_size = await asyncio.get_running_loop().run_in_executor(
                None, upload_file_to_drive, local_file_path, filename
            )
            
            if drive_id:
                # Update in-memory and Drive metadata
                file_extension = os.path.splitext(filename)[1].lstrip('.')
                new_entry = {
                    video_id: {
                        "drive_file_id": drive_id,
                        "uploaded_at": time.time(),
                        "format": file_extension,
                        "file_size": file_size,
                        "title": metadata.get("title", "Unknown")
                    }
                }
                logs.info(f"Updating metadata for ID: {video_id}")
                metadata_file_id = get_metadata_file_id()
                await asyncio.get_running_loop().run_in_executor(
                    None, update_metadata_on_drive, metadata_file_id, new_entry
                )
                
                # Cleanup local file
                os.remove(local_file_path)
                logs.info(f"Cleaned up local file: {local_file_path}")
                
                # Use Drive details for streaming in the next request (or for current if we adapt)
                # For simplicity, we'll return a YT-DLP generated stream URL *this time* if available,
                # but the Drive entry ensures it's used next time.
                # However, your requirement says: "direct drive se stream ho jayega" -> use drive URL now.
                
                ip = await get_public_ip()  
                stream_id = await new_uid()  
                stream_url = f"http://{ip}:8000/stream/{stream_id}"
                
                # Store Drive info for streaming
                database[stream_id] = {
                    "drive_file_id": drive_id, 
                    "file_name": f"{video_id}.{file_extension}",
                    "file_size": file_size,
                    "created_time": time.time()
                }
                source = "yt-dlp (Uploaded to Drive)" # Indicate it was a fresh fetch/upload
                
            else:
                # Fallback: Use the YT-DLP stream URL if download was successful but upload failed.
                # Since we don't have the temporary stream URL, we'll mark this as a failure for streaming this time.
                # You might need to re-extract for a direct stream URL if you want a fallback.
                return {"error": "Failed to upload to Drive and cannot get temporary stream URL"}

        # --- Handle Video (YT-DLP Stream) ---
        elif video:
            extension = "mp4" 
            file_name = f"{metadata.get('id')}.{extension}"
            ip = await get_public_ip()  
            stream_id = await new_uid()  
            stream_url = f"http://{ip}:8000/stream/{stream_id}"
            
            # Store YT-DLP stream URL for streaming
            database[stream_id] = {
                "file_url": file_url, 
                "file_name": file_name,
                "created_time": time.time()
            }
            source = "yt-dlp"

        # Prepare response
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
            "source": source # Source added
        }
        
        # Cache the response for direct yt-dlp streams (mainly video)
        if video:
             cache_db[cache_key] = {
                "response": response_data,
                "expiry_time": metadata.get("expiry_time", time.time() + 3600)
            }
        
        elapsed_time = time.time() - start_time
        logs.info(f"Response generated in {elapsed_time:.2f}s (Source: {source})")
        
        return response_data
            
    except asyncio.TimeoutError:
        logs.error("Request timeout - taking more than 30 seconds")
        return {"error": "Request timeout - taking too long"}
    except Exception as e:  
        logs.error(f"Error fetching YouTube info: {e}")  
        return {"error": "Something went wrong"}

@app.get("/stream/{stream_id}")
async def stream_from_stream_url(stream_id: str):
    file_data = database.get(stream_id)
    if not file_data:
        return {"error": "Invalid stream request!"}

    streamer = Streamer()
    
    file_url = file_data.get("file_url")
    drive_file_id = file_data.get("drive_file_id")
    file_name = file_data.get("file_name", "stream_file")
    file_size = file_data.get("file_size") # Used for Drive streaming

    try:  
        headers = {"Content-Disposition": f"attachment; filename=\"{file_name}\""}  
        
        if drive_file_id:
            # Stream from Google Drive
            logs.info(f"Streaming from Drive ID: {drive_file_id}")
            return StreamingResponse(  
                streamer.stream_file(drive_file_id=drive_file_id, file_size=file_size),  
                media_type="application/octet-stream",  
                headers=headers  
            )
        elif file_url:
            # Stream from YT-DLP URL
            logs.info(f"Streaming from YT-DLP URL")
            return StreamingResponse(  
                streamer.stream_file(file_url=file_url),  
                media_type="application/octet-stream",  
                headers=headers  
            )
        else:
            return {"error": "No stream source found for this ID."}

    except Exception as e:  
        logging.error(f"Stream Error: {e}")  
        return {"error": "Something went wrong!"}

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
    # Load Drive credentials and metadata synchronously on startup
    if load_drive_credentials():
        metadata_file_id = await asyncio.get_running_loop().run_in_executor(None, get_metadata_file_id)
        if metadata_file_id:
            await asyncio.get_running_loop().run_in_executor(None, download_metadata_from_drive, metadata_file_id)

    asyncio.create_task(cleanup_old_streams())

if __name__ == "__main__":
    # Ensure a 'cookies' folder exists for yt-dlp
    if not os.path.exists("cookies"):
        os.makedirs("cookies")
        
    # NOTE: The first run will require interactive authentication to create 'token.json'
    # if it doesn't exist and 'client_secret.json' is present.
    # Uvicorn run
    uvicorn.run(app, host="0.0.0.0", port=8000)
