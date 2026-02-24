# Upload History and Stability Fixes

## Issue Background (v1.6.x to v1.7.x)
In previous versions (specifically starting around v1.6.2 and v1.6.3), users reported recurrent `ConnectionResetError(104)`, `RemoteDisconnected`, and `Max retries exceeded` errors during the creative upload process.

The AI assistant at the time claimed to have implemented "resumable video upload chunks" and "stealth headers" to bypass VPS timeout limitations. However, a Git audit (`git log --oneline 780a059..HEAD -- meta_api.py`) revealed that the chunking logic was **never merged into the codebase**. The commits were effectively empty for `meta_api.py`.

## The Root Cause
Because the chunking logic was missing, the application was continuing to use the synchronous Meta Python SDK method for large files:
```python
video = AdVideo(parent_id=self.account_id)
video[AdVideo.Field.filepath] = file_path
video.remote_create() # Synchonous, monolithic upload
```

When uploading large videos (e.g., Feed and Stories creatives) from a remote VPS, the HTTP POST request to `graph.facebook.com` took longer than 60 seconds. The networking path (either Docker, the VPS firewall, or the upstream ISP) enforces strict idle timeouts or MTU limits on long-standing connections. Because the connection dragged on, it was forcibly reset (`ConnectionResetError`), causing the entire queued upload to fail. 

## The Solution (Applied in v1.7.x)
To permanently resolve this without relying on VPS configurations:

### 1. API-Native Resumable Video Uploads 
We replaced the synchronous SDK call with direct requests to the Meta Graph API utilizing the Resumable Upload protocol. The upload is now split into three phases:
- **Phase 1 (Start):** Registers the `file_size` and receives an `upload_session_id`.
- **Phase 2 (Transfer):** Reads the local file in chunks and uploads exactly the byte range requested by Meta API (`start_offset` to `end_offset`). If a chunk drops, only that chunk is retried, avoiding full connection timeouts.
- **Phase 3 (Finish):** Completes the upload session and returns the final `video_id`.

### 2. Expanded Timeouts
Timeouts for both `requests.post` and the `_download_file` helper (for pulling from Google Drive) were explicitly increased and scoped specifically for file transfer operations to allow slow connections to complete their chunks.

### 3. Synchronized Creation Flow
The `modules/creatives.py` backend was updated to strictly separate Creative generation and Ad generation:
1. Orchestrate the assets (images/videos) and texts via `uploader.create_creative_with_placements()`.
2. Grab the resulting `creative_id`.
3. Pass that ID to `uploader.create_ad()` to mount the final Ad inside the Ad Set.

This fixes outdated signature calls and ensures modular, fail-safe ad creation.
