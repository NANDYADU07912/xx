import aiohttp, asyncio, httpx, logging, re, uuid, uvicorn, yt_dlp, os, glob, time
import json
from fastapi import FastAPI, HTTPException, Security
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyQuery
from logging.handlers import RotatingFileHandler
from youtubesearchpython.future import VideosSearch

# Google Drive API imports
from google.oauth2 import credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io

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
database = {}  # For temporary stream URLs
ip_address = {}
cache_db = {}  # For YouTube metadata cache

# Single API key
API_KEY = "ShrutiMusic"
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
        logs.info(f"Created cookies directory: {cookies_dir}")
    cookie_files = glob.glob(os.path.join(cookies_dir, "*.txt"))
    logs.info(f"Found cookie files: {cookie_files}")
    return cookie_files

async def extract_metadata_from_yt(url: str, video: bool = False):
    """Extract metadata and stream URL using yt-dlp."""
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

        # Try each cookie file until one works
        for cookie_file in cookie_files:
            try:
                current_ydl_opts = ydl_opts.copy()
                current_ydl_opts["cookiefile"] = cookie_file
                with yt_dlp.YoutubeDL(current_ydl_opts) as ydl:
                    metadata = ydl.extract_info(url, download=False)
                    if metadata and metadata.get('url'):
                        logs.info(f"Successfully used cookie file: {cookie_file} for {url}")
                        return metadata
            except Exception as e:
                logs.warning(f"Cookie file {cookie_file} failed for {url}: {e}")
                continue
        
        # If no cookie files worked, try without cookies
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                logs.info(f"Trying yt-dlp without cookies for {url}")
                return ydl.extract_info(url, download=False)
        except Exception as e:
            logs.error(f"Metadata extraction error without cookies for {url}: {e}")
            return {}

    loop = asyncio.get_running_loop()
    metadata = await loop.run_in_executor(None, sync_extract)

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
            "format": metadata.get("ext"),
            "file_size": metadata.get("filesize"),
        }
    return {}

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
        self.chunk_size = 1 * 1024 * 1024  # 1MB chunks

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
            # Fetch current chunk and potentially the next one in parallel
            tasks = [asyncio.create_task(self.fetch_chunk(file_url, chunk_id))]
            if total_chunks is None or chunk_id + 1 < total_chunks:
                tasks.append(asyncio.create_task(self.fetch_chunk(file_url, chunk_id + 1)))
            
            results = await asyncio.gather(*tasks)

            for i, chunk in enumerate(results):
                current_chunk_id = chunk_id + i
                if chunk:
                    received_chunks.add(current_chunk_id)
                    yield chunk
            
            chunk_id += len(results) # Increment by the number of chunks processed in this iteration
            
            if total_chunks and chunk_id >= total_chunks:
                break # Exit if all chunks are processed

        # Fallback for any potentially missed chunks, though the parallel fetching should mostly cover it
        if total_chunks:
            for i in range(total_chunks):
                if i not in received_chunks:
                    missing_chunk = await self.fetch_chunk(file_url, i)
                    if missing_chunk:
                        yield missing_chunk


# Google Drive Configuration
CLIENT_SECRET_PATH = "client_secret.json"
TOKEN_PATH = "token.json"
DRIVE_CACHE_PATH = "drive_cache.json"  # Not directly used for metadata, but kept for consistency
METADATA_DRIVE_FILENAME = "api_metadata.json"
DRIVE_FOLDER_ID = None  # This should be set to your desired Google Drive folder ID
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Global variable to store metadata
drive_metadata = {}
drive_service = None

async def authenticate_drive():
    """Authenticates with Google Drive API."""
    global drive_service
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = credentials.Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES))
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
            creds = await asyncio.to_thread(flow.run_local_server, port=0)
        
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    
    drive_service = build('drive', 'v3', credentials=creds)
    logs.info("Google Drive API authenticated successfully.")

async def get_drive_metadata_file_id():
    """Searches for the metadata file on Google Drive and returns its ID."""
    if not drive_service:
        await authenticate_drive()
    
    response = await asyncio.to_thread(
        drive_service.files().list,
        q=f"name='{METADATA_DRIVE_FILENAME}' and trashed=false",
        spaces='drive',
        fields='files(id, name)'
    ).execute
    files = response.get('files', [])
    if files:
        logs.info(f"Found existing metadata file on Drive: {files[0]['name']} (ID: {files[0]['id']})")
        return files[0]['id']
    logs.info(f"Metadata file '{METADATA_DRIVE_FILENAME}' not found on Drive.")
    return None

async def download_drive_metadata():
    """Downloads the metadata JSON from Google Drive."""
    global drive_metadata
    metadata_file_id = await get_drive_metadata_file_id()
    
    if metadata_file_id:
        request = drive_service.files().get_media(fileId=metadata_file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = await asyncio.to_thread(downloader.next_chunk)
            logs.info(f"Download metadata progress: {int(status.progress() * 100)}%")
        
        fh.seek(0)
        try:
            drive_metadata = json.loads(fh.read().decode('utf-8'))
            logs.info(f"Downloaded metadata from Drive. Total entries: {len(drive_metadata)}")
        except json.JSONDecodeError as e:
            logs.error(f"Error decoding metadata JSON from Drive: {e}")
            drive_metadata = {} # Reset to empty if corrupted
    else:
        logs.info("No metadata file found on Drive, initializing empty metadata.")
        drive_metadata = {}

async def upload_drive_metadata():
    """Uploads/updates the metadata JSON to Google Drive."""
    global drive_metadata
    metadata_file_id = await get_drive_metadata_file_id()
    
    metadata_bytes = json.dumps(drive_metadata, indent=4).encode('utf-8')
    media_body = MediaFileUpload(
        io.BytesIO(metadata_bytes),
        mimetype='application/json',
        resumable=True
    )

    if metadata_file_id:
        # Update existing file
        await asyncio.to_thread(
            drive_service.files().update,
            fileId=metadata_file_id,
            media_body=media_body,
            fields='id'
        ).execute
        logs.info("Updated existing metadata file on Drive.")
    else:
        # Create new file
        file_metadata = {'name': METADATA_DRIVE_FILENAME, 'mimeType': 'application/json'}
        if DRIVE_FOLDER_ID:
            file_metadata['parents'] = [DRIVE_FOLDER_ID]
        
        file = await asyncio.to_thread(
            drive_service.files().create,
            body=file_metadata,
            media_body=media_body,
            fields='id'
        ).execute
        logs.info(f"Created new metadata file on Drive with ID: {file.get('id')}")

async def upload_audio_to_drive(file_path: str, title: str, video_id: str):
    """Uploads an audio file to Google Drive and updates metadata."""
    if not drive_service:
        await authenticate_drive()

    # Ensure file exists
    if not os.path.exists(file_path):
        logs.error(f"File not found for upload: {file_path}")
        return None

    # Determine MIME type based on file extension
    ext = os.path.splitext(file_path)[1].lstrip('.')
    mime_type = f"audio/{ext}" if ext else "application/octet-stream"
    
    file_metadata = {
        'name': f"{title} ({video_id}).{ext}",
        'mimeType': mime_type,
    }
    
    if DRIVE_FOLDER_ID:
        file_metadata['parents'] = [DRIVE_FOLDER_ID]
    
    media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
    
    try:
        file = await asyncio.to_thread(
            drive_service.files().create,
            body=file_metadata,
            media_body=media,
            fields='id, webContentLink, size'
        ).execute
        
        file_id = file.get('id')
        file_size = int(file.get('size', 0)) # Convert to int
        
        logs.info(f"Uploaded '{title}' ({video_id}) to Drive. File ID: {file_id}")

        # Update local metadata
        global drive_metadata
        drive_metadata[video_id] = {
            "drive_file_id": file_id,
            "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%S.000000"),
            "format": ext,
            "file_size": file_size,
            "title": title
        }
        await upload_drive_metadata() # Upload updated metadata to Drive
        return file_id
    except Exception as e:
        logs.error(f"Error uploading file to Drive: {e}")
        return None

async def get_drive_streaming_url(drive_file_id: str) -> str:
    """Generates a direct streaming/download URL for a Google Drive file."""
    # This URL directly uses the file ID to bypass Google Drive UI for streaming
    return f"https://docs.google.com/uc?export=download&id={drive_file_id}"

@app.get("/youtube")
async def get_youtube_info(id: str, video: bool = False, user: str = Security(get_user)):
    start_time = time.time()
    
    try:
        await cleanup_expired_cache()
        video_id = extract_video_id(id)
        
        cache_key = f"{video_id}_{'video' if video else 'audio'}"
        if cache_key in cache_db:
            cached_data = cache_db[cache_key]
            if cached_data.get("expiry_time", 0) > time.time():
                elapsed_time = time.time() - start_time
                logs.info(f"Returning cached response for ID: {video_id} in {elapsed_time:.2f}s")
                return cached_data["response"]
        
        # --- Audio Logic (Prioritize Drive) ---
        if not video: # If it's an audio request
            if video_id in drive_metadata:
                logs.info(f"Audio for {video_id} found in Drive metadata. Streaming from Drive.")
                drive_entry = drive_metadata[video_id]
                drive_file_id = drive_entry["drive_file_id"]
                stream_url = await get_drive_streaming_url(drive_file_id)

                if stream_url:
                    ip = await get_public_ip()
                    stream_id = await new_uid()
                    
                    # Store in database for streaming
                    database[stream_id] = {
                        "file_url": stream_url,
                        "file_name": f"{drive_entry['title']} ({video_id}).{drive_entry['format']}",
                        "created_time": time.time(),
                        "source": "drive"
                    }

                    response_data = {
                        "id": video_id,
                        "title": drive_entry["title"],
                        "duration": None, # Drive metadata might not have duration easily
                        "link": f"https://www.youtube.com/watch?v={video_id}",
                        "channel": "Unknown", # Drive metadata might not have channel
                        "views": None, # Drive metadata might not have views
                        "thumbnail": None, # Drive metadata might not have thumbnail
                        "stream_url": f"http://{ip}:8000/stream/{stream_id}",
                        "stream_type": "Audio",
                        "source": "drive"
                    }
                    cache_db[cache_key] = {"response": response_data, "expiry_time": time.time() + 3600}
                    elapsed_time = time.time() - start_time
                    logs.info(f"Response generated from Drive in {elapsed_time:.2f}s")
                    return response_data
                else:
                    logs.warning(f"Failed to get Drive streaming URL for {video_id}, falling back to yt-dlp.")
            else:
                logs.info(f"Audio for {video_id} not found in Drive metadata. Using yt-dlp.")
        
        # --- yt-dlp Logic (Fallback for Audio, Primary for Video) ---
        url = await asyncio.wait_for(get_youtube_url(video_id), timeout=12)
        if not url:
            raise HTTPException(status_code=404, detail="Invalid YouTube ID or URL could not be resolved.")
        
        metadata = await asyncio.wait_for(extract_metadata_from_yt(url, video), timeout=12)
        
        if not metadata or not metadata.get("stream_url"):
            raise HTTPException(status_code=500, detail="Could not fetch stream URL from YouTube.")
        
        file_url = metadata.get("stream_url")
        extension = metadata.get("format", "mp4" if video else "webm")
        file_name = f"{metadata.get('title', 'Unknown')} ({metadata.get('id')}).{extension}"
        
        ip = await get_public_ip()
        stream_id = await new_uid()
        
        # Store in database for streaming
        database[stream_id] = {
            "file_url": file_url,
            "file_name": file_name,
            "created_time": time.time(),
            "source": "yt-dlp"
        }

        response_data = {
            "id": metadata.get("id"),
            "title": metadata.get("title"),
            "duration": metadata.get("duration"),
            "link": metadata.get("link"),
            "channel": metadata.get("channel"),
            "views": metadata.get("views"),
            "thumbnail": metadata.get("thumbnail"),
            "stream_url": f"http://{ip}:8000/stream/{stream_id}",
            "stream_type": metadata.get("stream_type"),
            "source": "yt-dlp"
        }

        # If it's an audio file and not from Drive, download and upload to Drive in background
        if not video and video_id not in drive_metadata:
            logs.info(f"Initiating background upload for audio {video_id} to Google Drive.")
            asyncio.create_task(
                download_and_upload_to_drive_bg(
                    url, 
                    metadata.get("title", "Unknown"), 
                    video_id, 
                    extension,
                    metadata.get("file_size") # Pass original file size if available from yt-dlp
                )
            )

        # Cache the response
        cache_db[cache_key] = {
            "response": response_data,
            "expiry_time": metadata.get("expiry_time", time.time() + 3600)
        }
        
        elapsed_time = time.time() - start_time
        logs.info(f"Response generated from yt-dlp in {elapsed_time:.2f}s")
        return response_data
            
    except asyncio.TimeoutError:
        logs.error("Request timeout - taking more than 12 seconds")
        raise HTTPException(status_code=504, detail="Request timeout - taking too long.")
    except Exception as e:
        logs.error(f"Error fetching YouTube info for ID {id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Something went wrong: {e}")

async def download_and_upload_to_drive_bg(
    youtube_url: str, title: str, video_id: str, file_ext: str, estimated_file_size: int = None
):
    """
    Downloads audio using yt-dlp and uploads it to Google Drive in the background.
    """
    temp_dir = "temp_audio_uploads"
    os.makedirs(temp_dir, exist_ok=True)
    
    temp_file_path = os.path.join(temp_dir, f"{video_id}.{file_ext}")

    def sync_download_audio():
        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": temp_file_path,
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "postprocessors": [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': file_ext, # Ensure the correct codec is used
                'preferredquality': '192',
            }] if file_ext in ['mp3', 'm4a', 'opus', 'flac', 'aac', 'wav'] else [], # Only apply if it's an audio format
        }
        cookie_files = get_cookie_files()
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Try with cookies first
            for cookie_file in cookie_files:
                try:
                    current_ydl_opts = ydl_opts.copy()
                    current_ydl_opts["cookiefile"] = cookie_file
                    with yt_dlp.YoutubeDL(current_ydl_opts) as ydl_cookie:
                        logs.info(f"Downloading audio for {video_id} using cookie: {cookie_file}")
                        ydl_cookie.download([youtube_url])
                        return True
                except Exception as e:
                    logs.warning(f"Cookie file {cookie_file} failed for download {video_id}: {e}")
                    continue
            
            # Fallback without cookies
            logs.info(f"Downloading audio for {video_id} without cookies.")
            ydl.download([youtube_url])
            return True
        
    try:
        success = await asyncio.to_thread(sync_download_audio)
        if success and os.path.exists(temp_file_path):
            logs.info(f"Audio downloaded for {video_id} to {temp_file_path}. Uploading to Drive...")
            await upload_audio_to_drive(temp_file_path, title, video_id)
            os.remove(temp_file_path) # Clean up temp file
            logs.info(f"Cleaned up temporary file: {temp_file_path}")
        else:
            logs.error(f"Failed to download audio for {video_id} or file not found after download.")
    except Exception as e:
        logs.error(f"Error in background download and upload for {video_id}: {e}", exc_info=True)
    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path) # Ensure cleanup even if upload fails

@app.get("/stream/{stream_id}")
async def stream_from_stream_url(stream_id: str):
    file_data = database.get(stream_id)
    if not file_data or not file_data.get("file_url") or not file_data.get("file_name"):
        raise HTTPException(status_code=404, detail="Invalid stream request!")
    
    streamer = Streamer()
    try:
        headers = {
            "Content-Disposition": f"attachment; filename=\"{file_data.get('file_name')}\"",
            "X-Content-Source": file_data.get("source", "unknown") # Add source header
        }
        return StreamingResponse(
            streamer.stream_file(file_data.get("file_url")),
            media_type="application/octet-stream",
            headers=headers
        )
    except Exception as e:
        logs.error(f"Stream Error for {stream_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Something went wrong during streaming!")

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
    # Initialize Google Drive
    await authenticate_drive()
    await download_drive_metadata()
    
    # Start background tasks
    asyncio.create_task(cleanup_old_streams())

if __name__ == "__main__":
    # Ensure client_secret.json exists
    if not os.path.exists(CLIENT_SECRET_PATH):
        logs.error(f"'{CLIENT_SECRET_PATH}' not found. Please create one for Google Drive API.")
        logs.error("Refer to Google Drive API documentation for creating OAuth 2.0 Client IDs.")
        # Exit or handle gracefully depending on desired behavior
        exit(1)
        
    # Set your Google Drive Folder ID here if you want to upload to a specific folder
    # Otherwise, files will be uploaded to the root of "My Drive"
    # To get a folder ID, open the folder in Google Drive, the ID is in the URL.
    # e.g., https://drive.google.com/drive/folders/YOUR_FOLDER_ID_HERE
    global DRIVE_FOLDER_ID
    DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", None) # Get from environment or hardcode
    if DRIVE_FOLDER_ID:
        logs.info(f"Google Drive uploads will target folder ID: {DRIVE_FOLDER_ID}")
    else:
        logs.warning("DRIVE_FOLDER_ID not set. Files will be uploaded to the root of My Drive.")

    uvicorn.run(app, host="0.0.0.0", port=8000)
