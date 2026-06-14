# Upload Feature — Implementation Report

**Date:** 2026-06-14  
**Files changed:** `app.py`, `templates/index.html`

---

## Overview

This report documents the addition of a paced SFTP upload capability to PulseFetch. The upload engine mirrors the existing download engine's core design principle: **fresh connection per chunk, mandatory cooldown between chunks, automatic resume from the last committed byte**. The same router-friendly pacing that made downloads reliable applies equally in the upload direction.

---

## Problem Statement

The existing PulseFetch application solved downloading large files from a weak-CPU SFTP server (Huawei HG8145X7 and similar home router/NAS combos) by breaking transfers into discrete chunks with cooling-off pauses between each one. The upload direction had the same underlying bottleneck — the router's tiny CPU and small SSH buffers — but no mechanism to pace it. A naive one-shot `put()` call would saturate the router just as badly as an unthrottled download.

---

## Architecture Decision: Local Path Input vs. Browser File Upload

Two approaches were considered for how the user specifies the file to upload:

| Approach | Pros | Cons |
|---|---|---|
| Browser `<input type="file">` | Familiar UX | File travels Browser → Flask (loopback) → SFTP. Needs temp storage equal to the full file size. Double I/O. |
| Local file path text input | No temp storage. Flask reads directly from disk. Zero double-write overhead. | User must paste a path manually. |

Since PulseFetch is a **localhost tool** (always served at `127.0.0.1:5000`), the path-input approach is cleaner, more honest about what is happening, and avoids consuming double the disk space for large files. This matches the spirit of the project.

---

## Backend Changes (`app.py`)

### New Route: `POST /upload`

```
POST /upload
Form fields:
  local_path  — absolute path to the file on the local machine
  remote_dir  — destination directory on the SFTP server (e.g. /USB-Drive/Media)
```

**What it does:**
1. Validates that `local_path` is a real file on disk (`os.path.isfile`).
2. Derives `remote_path = remote_dir.rstrip("/") + "/" + filename`.
3. Checks for an already in-progress upload of the same remote path and returns the existing `job_id` rather than spawning a duplicate.
4. Creates a job dict (same structure as download jobs, with `type = "upload"`) and registers it in the shared `download_jobs` registry.
5. Starts `_paced_upload(job_id)` in a daemon thread.
6. Returns `{"job_id": "..."}` immediately so the frontend can begin polling.

**Duplicate detection** is scoped to `type == "upload"` so an in-progress upload and an in-progress download of the same remote path can coexist if needed (e.g. downloading one file while uploading another to the same directory).

### New Function: `_paced_upload(job_id)`

The upload engine runs as a background thread and follows this algorithm:

#### Step 1 — Derive starting offset (resume support)

```python
sftp, transport = sftp_connect()
try:
    offset = sftp.stat(remote_path).st_size   # bytes already on the server
except IOError:
    offset = 0                                 # file does not exist yet
sftp_close(sftp, transport)
```

If a previous upload attempt was interrupted, the remote file will have `N` bytes. The engine starts from byte `N`, skipping those bytes in the local file (`local_fh.seek(offset)`). This means a resumed upload never re-sends data the server already has.

#### Step 2 — Chunk loop

For each chunk:

1. **Read** `CHUNK_SIZE` bytes from the local file (fast, local disk).
2. **Open a fresh SFTP connection** — this gives the router a clean TCP session with no lingering state from the previous chunk.
3. **Open the remote file** in the correct mode:
   - `"wb"` (write/create/truncate) when `offset == 0` — ensures a clean start even if a stale remote file exists.
   - `"ab"` (append) when `offset > 0` — appends exactly to the end of committed remote data without touching anything already written.
4. **Write** the chunk and **close** the remote file handle. Closing before `sftp_close()` ensures the server flushes and ACKs the write.
5. **Close the SFTP connection** entirely — the router releases its SSH buffers and connection state.
6. Advance `offset += len(chunk)`, update the job's progress fields.
7. **Sleep `SLEEP_BETWEEN_CHUNKS` seconds** (cancellable in 1-second ticks) to give the router's CPU time to recover before the next burst.

#### Why `"ab"` for resume chunks?

The SFTP `O_APPEND` flag (`"ab"` mode) tells the server to write each payload to the **end of the file as it exists at write time**, regardless of any client-side seek. This is the safest mode for resume: even if there is slight timing uncertainty in the stat, `O_APPEND` guarantees no byte is overwritten and no gap is left. `"wb"` on the first chunk guarantees a clean slate for fresh uploads.

#### Error recovery

On any exception during a chunk:

```python
sftp_close(sftp, transport)
# ... wait RETRY_WAIT_SECS seconds ...
sftp, transport = sftp_connect()
offset = sftp.stat(remote_path).st_size   # ground truth from server
sftp_close(sftp, transport)
local_fh.seek(offset)                     # re-align local read cursor
```

The remote stat is the **single source of truth** after an error — it reflects exactly how many bytes the server committed to storage, which may be anywhere from 0 to `offset + CHUNK_SIZE` depending on where the connection dropped. The local file handle is re-seeked to match. This is the same "re-derive from disk/remote" philosophy as the download engine's recovery logic.

#### Cancellation

The same `_cancellable_sleep()` mechanism used by downloads applies: the cancel event is checked at the top of every chunk iteration and every second during pauses. The cancel button responds within ~1 second.

### Job Dict Fields for Upload Jobs

| Field | Value |
|---|---|
| `type` | `"upload"` |
| `filename` | Basename of the local file |
| `local_path` | Full local path (source) |
| `remote_path` | Full remote path (destination, including filename) |
| `total_bytes` | Size of the local file (set synchronously at job creation) |
| `downloaded_bytes` | Bytes transferred so far (semantically "uploaded bytes" — reuses the existing field name for frontend compatibility) |
| `chunks_done` | `offset // CHUNK_SIZE` |
| `status` | `starting → uploading → pausing → uploading → … → complete` |

The `/status/<job_id>` and `/jobs` endpoints require **no changes** — they already serialize the full job dict and compute `percent` from `downloaded_bytes / total_bytes`, which works correctly for uploads.

The `/cancel/<job_id>` endpoint requires **no changes** — it sets the cancel event regardless of job type.

---

## Frontend Changes (`templates/index.html`)

### Upload Button

A small **"Upload here"** button is placed in a toolbar row above the file table, right-aligned:

```html
<div class="d-flex justify-content-end mb-2">
  <button class="btn btn-sm btn-outline-primary"
          data-bs-toggle="modal" data-bs-target="#uploadModal">
    <i class="bi bi-cloud-upload me-1"></i>Upload here
  </button>
</div>
```

The button opens the upload modal. It is always visible in the current directory, making it easy to upload a file to wherever you are browsing.

### Upload Modal

A Bootstrap 5 modal with two fields:

- **"Uploading to"** — read-only input pre-filled with `{{ current_path }}` (Jinja2). Shows the user exactly where the file will land on the SFTP server.
- **"Local file path"** — text input where the user pastes the full path to the file on their computer (e.g. `C:\Users\You\Videos\movie.mkv`).

Validation: the "Start Upload" button calls `startUpload()`, which checks that the path field is non-empty before submitting. Server-side validation checks that the file actually exists. Error messages surface in the modal via Bootstrap's `is-invalid` / `invalid-feedback` pattern. The modal resets its input when closed (`hidden.bs.modal` event).

### `startUpload()` JavaScript Function

1. Reads the local path from the input; adds `is-invalid` styling if empty.
2. Disables the button and shows a spinner while the request is in flight.
3. `POST /upload` with `local_path` and `remote_dir = '{{ current_path }}'`.
4. On success: closes the modal, calls `ensurePolling(job_id, null)`, expands the transfers panel.
5. On error: re-enables the button and shows the server error message in the `invalid-feedback` span.

### Transfers Panel (formerly "Downloads Panel")

- Panel header renamed from **"Downloads"** to **"Transfers"** with a bidirectional arrows icon (`bi-arrow-left-right`) to reflect that it now tracks both uploads and downloads.
- The active-job counter badge continues to show the count of all in-flight jobs.

### Job Card Updates

`renderJobCard()` was updated to be direction-aware:

- **Job icon**: upload jobs show a `bi-cloud-upload` (blue) icon in the card title; downloads show no icon (files) or a folder icon (folders).
- **Transfer label**: the byte-progress line reads "Uploaded: X / Y" for upload jobs and "Downloaded: X / Y" for download jobs.
- **Completion message**: shows "Uploaded to: `<remote_path>`" for uploads and "Saved to: `<local_path>`" for downloads.
- **Cancellation message**: shows "Cancelled — partial upload at: `<remote_path>`" for uploads.

### New CSS Badge

```css
.badge-uploading { background-color: #0d6efd; }
```

Matched to the blue downloading badge for visual consistency — both directions use the same colour while active, distinguishable by the job icon and label text.

### `STATUS_META` Update

```javascript
uploading: { label: 'Uploading', badge: 'badge-uploading' },
```

Added alongside the existing `downloading` entry. The `pausing`, `retrying`, `complete`, `error`, and `cancelled` statuses are shared between uploads and downloads with no changes needed.

---

## Pacing Parameters

Upload jobs use the same tuning constants as downloads:

| Constant | Default | Effect on uploads |
|---|---|---|
| `CHUNK_SIZE` | 100 MB | Each burst sends this many bytes then closes the connection |
| `SLEEP_BETWEEN_CHUNKS` | 7 s | Router cooling-off window between upload bursts |
| `RETRY_WAIT_SECS` | 15 s | Wait before retrying after a dropped connection |

These are defined once at the top of `app.py` and apply to both directions. Reduce `CHUNK_SIZE` or increase `SLEEP_BETWEEN_CHUNKS` if the router still struggles during uploads.

---

## What Was Not Changed

- The `/status/<job_id>`, `/jobs`, and `/cancel/<job_id>` routes required no changes.
- `bulk_download.py` was not modified — it remains a download-only CLI tool.
- No new dependencies were added.
- The `sftp_connect()` and `sftp_close()` helpers are reused as-is.

---

## Known Limitations

1. **No folder upload** — the current implementation supports single-file uploads only. A folder upload (mirroring `_folder_download`) could be added in a future iteration.
2. **Local path input only** — users must paste a file path rather than use a native file picker. This is an intentional design choice for a localhost tool, avoiding double-copy overhead for large files.
3. **No conflict detection** — if a complete file already exists at the remote path, the upload will overwrite it (first chunk uses `"wb"` mode). There is no "file exists, skip?" prompt.
4. **Resume assumes remote file is intact** — resume is based solely on the remote file's byte count. If the remote file was partially corrupted (unlikely with SFTP but possible), a manual delete of the remote file before re-uploading would be needed.
