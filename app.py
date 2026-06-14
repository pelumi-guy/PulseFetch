"""
PulseFetch — Paced SFTP File Explorer
======================================
Downloads large files from a weak-CPU SFTP server (e.g. a home router) by
splitting transfers into discrete chunks with mandatory cooling-off pauses
between each chunk.  This prevents the router's CPU from overheating and its
tiny memory buffer from filling up.

How the byte-seek / chunk logic works
--------------------------------------
paramiko's SFTPFile.seek(n) sets an internal cursor.  When the following
SFTPFile.read(size) call is issued, paramiko sends a single SSH_FXP_READ
packet with offset=n to the server.  No bytes before `n` are transmitted —
it is a true O(1) seek at the protocol level.

For each iteration of the download loop:
  1. Open a fresh Transport + SFTPClient  (gives the router a clean connection)
  2. sftp.open(remote_path, "rb")
  3. remote_file.seek(offset)              ← tells server: start here
  4. data = remote_file.read(CHUNK_SIZE)   ← server sends exactly CHUNK_SIZE B
  5. Close the connection immediately      ← router releases all state / buffers
  6. Append data to local file, fsync to disk
  7. offset += len(data)
  8. Sleep SLEEP_BETWEEN_CHUNKS seconds    ← CPU/buffer cool-down window
  9. Repeat from step 1 until offset >= total_size

On any exception:
  - Wait RETRY_WAIT_SECS seconds
  - Re-derive offset from os.path.getsize(local_path) — the actual bytes
    safely written to disk — then retry.  This guarantees no double-writes
    and no gaps even if the process was interrupted between a network read
    and a disk write.

Tuning
------
Reduce CHUNK_SIZE  → gentler on the router, more pauses, slower overall
Increase SLEEP_BETWEEN_CHUNKS → more recovery time for the router
Reduce them if your router is beefy enough to handle it
"""

import os
import stat
import time
import uuid
import threading
from pathlib import Path
from datetime import datetime

import paramiko
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, redirect

load_dotenv()

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION  ← edit before running
# ═══════════════════════════════════════════════════════════════════════════════

SFTP_HOST   = os.environ["SFTP_HOST"]
SFTP_PORT   = int(os.environ["SFTP_PORT"])
SFTP_USER   = os.environ["SFTP_USER"]
SFTP_PASS   = os.environ["SFTP_PASS"]

# Tuning knobs
CHUNK_SIZE            = 100 * 1024 * 1024   # 100 MB per chunk  (try 25 MB if it still crashes)
SLEEP_BETWEEN_CHUNKS  = 7                   # seconds to pause between chunks
RETRY_WAIT_SECS       = 15                  # seconds to wait before retrying after an error

# Local destination folder  (created automatically)
LOCAL_DOWNLOAD_DIR = str(Path.home() / "Downloads" / "PulseFetch")

# ═══════════════════════════════════════════════════════════════════════════════

app = Flask(__name__)

# job registry — keyed by UUID string
# Each job is a plain dict; mutations must hold jobs_lock
download_jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()

# cancel signals — set the event to request cancellation of that job
cancel_events: dict[str, threading.Event] = {}

os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)


# ─── SFTP helpers ─────────────────────────────────────────────────────────────

def sftp_connect() -> tuple[paramiko.SFTPClient, paramiko.Transport]:
    """Open a fresh SFTP connection.  Returns (sftp_client, transport)."""
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def sftp_close(sftp, transport):
    """Silently close an SFTPClient and its Transport."""
    for obj in (sftp, transport):
        if obj is not None:
            try:
                obj.close()
            except Exception:
                pass


def _cancellable_sleep(job_id: str, seconds: int) -> bool:
    """
    Sleep for `seconds` in 1-second ticks, checking for cancellation each tick.
    Returns True if the job was cancelled during the sleep, False otherwise.
    This lets the cancel button respond within ~1 second even during a long cooldown.
    """
    ev = cancel_events.get(job_id)
    for _ in range(seconds):
        if ev and ev.is_set():
            return True
        time.sleep(1)
    return bool(ev and ev.is_set())


def _fmt(n: int) -> str:
    """Human-readable byte size."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


# ─── SFTP directory walker ────────────────────────────────────────────────────

def _walk_remote(remote_dir: str, local_dir: str, file_list: list) -> None:
    """
    Recursively enumerate `remote_dir` and append every regular file as a
    (remote_path, local_path, size) tuple to `file_list`.

    A fresh SFTP connection is opened and closed for each directory level.
    This keeps each individual call short, prevents the router from holding
    a long-lived connection during the scan, and lets it release resources
    between listdir_attr calls.
    """
    sftp, transport = sftp_connect()
    try:
        attrs = sftp.listdir_attr(remote_dir)
    finally:
        sftp_close(sftp, transport)

    for a in sorted(attrs, key=lambda x: x.filename.lower()):
        r_child = remote_dir.rstrip("/") + "/" + a.filename
        l_child = os.path.join(local_dir, a.filename)
        if stat.S_ISDIR(a.st_mode or 0):
            _walk_remote(r_child, l_child, file_list)   # recurse
        else:
            file_list.append((r_child, l_child, a.st_size or 0))


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def root():
    return redirect("/browse?path=/")


@app.route("/browse")
def browse():
    raw_path    = request.args.get("path", "/").strip()
    remote_path = ("/" + raw_path.strip("/")) if raw_path != "/" else "/"

    entries = []
    error   = None

    try:
        sftp, transport = sftp_connect()
        try:
            for a in sorted(sftp.listdir_attr(remote_path),
                            key=lambda x: (not stat.S_ISDIR(x.st_mode or 0),
                                           x.filename.lower())):
                is_dir     = stat.S_ISDIR(a.st_mode or 0)
                entry_path = remote_path.rstrip("/") + "/" + a.filename
                entries.append({
                    "name":       a.filename,
                    "path":       entry_path,
                    "is_dir":     is_dir,
                    "size":       "—" if is_dir else _fmt(a.st_size or 0),
                    "size_bytes": 0 if is_dir else (a.st_size or 0),
                    "modified":   (datetime.fromtimestamp(a.st_mtime or 0)
                                   .strftime("%Y-%m-%d %H:%M")
                                   if a.st_mtime else "—"),
                })
        finally:
            sftp_close(sftp, transport)
    except Exception as exc:
        error = str(exc)

    parts       = [p for p in remote_path.split("/") if p]
    breadcrumbs = [{"name": "root", "path": "/"}]
    for i, part in enumerate(parts):
        breadcrumbs.append({"name": part,
                             "path": "/" + "/".join(parts[: i + 1])})
    parent = ("/" + "/".join(parts[:-1])) if parts else None

    return render_template(
        "index.html",
        entries      = entries,
        current_path = remote_path,
        breadcrumbs  = breadcrumbs,
        parent       = parent,
        error        = error,
        sftp_host    = SFTP_HOST,
        sftp_port    = SFTP_PORT,
        download_dir = LOCAL_DOWNLOAD_DIR,
        chunk_mb     = CHUNK_SIZE // (1024 * 1024),
        sleep_secs   = SLEEP_BETWEEN_CHUNKS,
    )


@app.route("/download", methods=["POST"])
def start_download():
    remote_path = request.form.get("path", "").strip()
    dl_type     = request.form.get("type", "file")   # "file" or "folder"
    filename    = os.path.basename(remote_path)

    if not remote_path or not filename:
        return jsonify({"error": "Invalid path"}), 400

    # Prevent duplicate in-progress downloads of the same path
    with jobs_lock:
        for j in download_jobs.values():
            if j["remote_path"] == remote_path and j["status"] not in ("complete", "error"):
                return jsonify({"job_id": j["id"], "existing": True})

    job_id = str(uuid.uuid4())
    job = {
        "id":               job_id,
        "type":             dl_type,      # "file" | "folder"
        "filename":         filename,
        "remote_path":      remote_path,
        "local_path":       os.path.join(LOCAL_DOWNLOAD_DIR, filename),
        "total_bytes":      0,
        "downloaded_bytes": 0,
        "chunks_done":      0,
        # folder-specific fields (None for file jobs)
        "total_files":      None,
        "completed_files":  0,
        "current_file":     None,
        # status values: starting | scanning | downloading | pausing | retrying | complete | error
        "status":           "starting",
        "error":            None,
        "started_at":       datetime.now().strftime("%H:%M:%S"),
    }

    with jobs_lock:
        download_jobs[job_id] = job

    cancel_events[job_id] = threading.Event()

    target = _folder_download if dl_type == "folder" else _paced_download
    threading.Thread(target=target, args=(job_id,), daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/upload", methods=["POST"])
def start_upload():
    local_path  = request.form.get("local_path", "").strip()
    remote_dir  = request.form.get("remote_dir", "").strip()

    if not local_path or not os.path.isfile(local_path):
        return jsonify({"error": "Local file not found — check the path and try again"}), 400
    if not remote_dir:
        return jsonify({"error": "Remote directory is required"}), 400

    filename    = os.path.basename(local_path)
    remote_path = remote_dir.rstrip("/") + "/" + filename

    # Prevent duplicate in-progress uploads of the same remote path
    with jobs_lock:
        for j in download_jobs.values():
            if (j["remote_path"] == remote_path
                    and j["type"] == "upload"
                    and j["status"] not in ("complete", "error", "cancelled")):
                return jsonify({"job_id": j["id"], "existing": True})

    job_id = str(uuid.uuid4())
    job = {
        "id":               job_id,
        "type":             "upload",
        "filename":         filename,
        "local_path":       local_path,
        "remote_path":      remote_path,
        "total_bytes":      os.path.getsize(local_path),
        "downloaded_bytes": 0,   # bytes transferred (uploaded so far)
        "chunks_done":      0,
        "total_files":      None,
        "completed_files":  0,
        "current_file":     None,
        "status":           "starting",
        "error":            None,
        "started_at":       datetime.now().strftime("%H:%M:%S"),
    }

    with jobs_lock:
        download_jobs[job_id] = job

    cancel_events[job_id] = threading.Event()
    threading.Thread(target=_paced_upload, args=(job_id,), daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/cancel/<job_id>", methods=["POST"])
def cancel_download(job_id: str):
    with jobs_lock:
        job = download_jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        if job["status"] in ("complete", "error", "cancelled"):
            return jsonify({"error": "Job already finished"}), 400

    ev = cancel_events.get(job_id)
    if ev:
        ev.set()

    return jsonify({"ok": True})


@app.route("/status/<job_id>")
def job_status(job_id: str):
    with jobs_lock:
        job = download_jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        data = dict(job)

    total          = data["total_bytes"]
    data["percent"]          = round(data["downloaded_bytes"] / total * 100, 1) if total else 0
    data["downloaded_human"] = _fmt(data["downloaded_bytes"])
    data["total_human"]      = _fmt(total) if total else "?"
    return jsonify(data)


@app.route("/jobs")
def all_jobs():
    with jobs_lock:
        jobs = [dict(j) for j in download_jobs.values()]
    for j in jobs:
        total        = j["total_bytes"]
        j["percent"] = round(j["downloaded_bytes"] / total * 100, 1) if total else 0
        j["downloaded_human"] = _fmt(j["downloaded_bytes"])
        j["total_human"]      = _fmt(total) if total else "?"
    return jsonify(jobs)


# ─── Paced download engine ────────────────────────────────────────────────────

def _paced_download(job_id: str) -> None:
    """
    Background thread that performs the paced, chunked SFTP download.
    See module docstring for the full explanation of the algorithm.
    """
    job         = download_jobs[job_id]
    remote_path = job["remote_path"]
    local_path  = job["local_path"]

    sftp = transport = remote_file = None

    # ── Step 1: stat the remote file to get total size ────────────────────
    try:
        sftp, transport = sftp_connect()
        total_size = sftp.stat(remote_path).st_size
        sftp_close(sftp, transport)
        sftp = transport = None
    except Exception as exc:
        with jobs_lock:
            job["status"] = "error"
            job["error"]  = f"Cannot stat remote file: {exc}"
        return

    with jobs_lock:
        job["total_bytes"] = total_size
        job["status"]      = "downloading"

    # ── Step 2: calculate starting byte offset (resume support) ──────────
    # If a partial file exists on disk from a previous run, start from there.
    offset = os.path.getsize(local_path) if os.path.exists(local_path) else 0
    with jobs_lock:
        job["downloaded_bytes"] = offset
        job["chunks_done"]      = offset // CHUNK_SIZE

    # ── Step 3: chunk loop ────────────────────────────────────────────────
    while offset < total_size:

        # Check for cancellation before opening a new connection
        if cancel_events.get(job_id, threading.Event()).is_set():
            with jobs_lock:
                job["status"] = "cancelled"
            cancel_events.pop(job_id, None)
            return

        try:
            # Open a *fresh* connection for every chunk so the router can
            # cleanly release its connection state between transfers.
            sftp, transport = sftp_connect()

            remote_file = sftp.open(remote_path, "rb")
            # seek() sets the SSH_FXP_READ offset — no bytes are skipped over
            # the wire, the server jumps directly to `offset`.
            remote_file.seek(offset)

            to_read = min(CHUNK_SIZE, total_size - offset)
            data    = remote_file.read(to_read)   # blocks until chunk arrives

            remote_file.close()
            remote_file = None
            sftp_close(sftp, transport)
            sftp = transport = None

            if not data:
                raise IOError("Server returned an empty payload")

            # Write chunk — "wb" for brand-new file, "ab" for every subsequent
            # chunk (append keeps the cursor at EOF, which is exactly what we want).
            with open(local_path, "wb" if offset == 0 else "ab") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())   # flush OS write-back cache to disk

            offset += len(data)

            with jobs_lock:
                job["downloaded_bytes"] = offset
                job["chunks_done"]      = offset // CHUNK_SIZE
                job["error"]            = None

            # Pause between chunks — this is the router's breathing room.
            # _cancellable_sleep checks for cancellation every second so the
            # button responds promptly even during a long cooldown.
            if offset < total_size:
                with jobs_lock:
                    job["status"] = "pausing"
                if _cancellable_sleep(job_id, SLEEP_BETWEEN_CHUNKS):
                    with jobs_lock:
                        job["status"] = "cancelled"
                    cancel_events.pop(job_id, None)
                    return
                with jobs_lock:
                    job["status"] = "downloading"

        except Exception as exc:
            # Clean up any dangling handles
            if remote_file is not None:
                try:
                    remote_file.close()
                except Exception:
                    pass
                remote_file = None
            sftp_close(sftp, transport)
            sftp = transport = None

            with jobs_lock:
                job["status"] = "retrying"
                job["error"]  = f"{exc}  — retrying in {RETRY_WAIT_SECS}s"

            if _cancellable_sleep(job_id, RETRY_WAIT_SECS):
                with jobs_lock:
                    job["status"] = "cancelled"
                cancel_events.pop(job_id, None)
                return

            # Re-derive offset from actual on-disk size — the ground truth for
            # how many bytes are safely persisted.  Never trust the in-memory
            # counter after an error.
            offset = os.path.getsize(local_path) if os.path.exists(local_path) else 0
            with jobs_lock:
                job["downloaded_bytes"] = offset
                job["status"]           = "downloading"

    with jobs_lock:
        job["status"] = "complete"
    cancel_events.pop(job_id, None)


# ─── Folder download engine ───────────────────────────────────────────────────

def _folder_download(job_id: str) -> None:
    """
    Background thread for downloading an entire remote directory tree.

    Phase 1 — Scan
    ──────────────
    _walk_remote() is called recursively to build a flat list of every file
    under the remote directory: [(remote_path, local_path, size), ...].
    Each listdir_attr call uses its own short-lived connection so the router
    is never asked to hold state across the full scan.

    Phase 2 — Sequential paced download
    ─────────────────────────────────────
    Files are downloaded one at a time using the identical chunk+pause+
    reconnect logic from _paced_download:
      • fresh connection per chunk
      • seek(offset) for true resume within a file
      • fsync after each chunk
      • SLEEP_BETWEEN_CHUNKS seconds of cooldown between chunks
      • RETRY_WAIT_SECS seconds on error, then resume from on-disk size

    Progress accounting
    ───────────────────
    job["downloaded_bytes"] always equals:
        sum(sizes of fully completed files) + offset within current file

    On error, both values are re-derived from actual on-disk file sizes so
    the counter is always truthful regardless of when the process is interrupted.
    """
    job         = download_jobs[job_id]
    remote_base = job["remote_path"]
    local_base  = job["local_path"]

    sftp = transport = remote_file = None

    # ── Phase 1: Scan remote tree ─────────────────────────────────────────
    with jobs_lock:
        job["status"] = "scanning"

    file_list: list[tuple[str, str, int]] = []
    try:
        _walk_remote(remote_base, local_base, file_list)
    except Exception as exc:
        with jobs_lock:
            job["status"] = "error"
            job["error"]  = f"Directory scan failed: {exc}"
        return

    if not file_list:
        with jobs_lock:
            job["status"]      = "complete"
            job["total_files"] = 0
        return

    total_bytes = sum(sz for _, _, sz in file_list)
    total_files = len(file_list)

    with jobs_lock:
        job["total_bytes"] = total_bytes
        job["total_files"] = total_files
        job["status"]      = "downloading"

    # ── Phase 2: Download each file in order ──────────────────────────────
    completed_bytes = 0   # running sum of bytes from fully finished files
    completed_files = 0

    for file_idx, (remote_path, local_path, file_size) in enumerate(file_list):

        # Check for cancellation between files
        if cancel_events.get(job_id, threading.Event()).is_set():
            with jobs_lock:
                job["status"]       = "cancelled"
                job["current_file"] = None
            cancel_events.pop(job_id, None)
            return

        # Mirror the remote directory structure locally
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with jobs_lock:
            job["current_file"] = os.path.basename(remote_path)

        # Resume: how many bytes of this file are already safely on disk?
        offset = os.path.getsize(local_path) if os.path.exists(local_path) else 0

        if offset >= file_size:
            # File is already fully present — skip it
            completed_bytes += file_size
            completed_files += 1
            with jobs_lock:
                job["downloaded_bytes"] = completed_bytes
                job["completed_files"]  = completed_files
            continue

        # Account for partial bytes already on disk when updating progress
        completed_bytes += offset

        # ── Chunk loop for this individual file ───────────────────────────
        while offset < file_size:

            # Check for cancellation before opening a new connection
            if cancel_events.get(job_id, threading.Event()).is_set():
                with jobs_lock:
                    job["status"]       = "cancelled"
                    job["current_file"] = None
                cancel_events.pop(job_id, None)
                return

            try:
                sftp, transport = sftp_connect()
                remote_file = sftp.open(remote_path, "rb")
                remote_file.seek(offset)          # true SSH-level seek
                to_read = min(CHUNK_SIZE, file_size - offset)
                data    = remote_file.read(to_read)
                remote_file.close()
                remote_file = None
                sftp_close(sftp, transport)
                sftp = transport = None

                if not data:
                    raise IOError("Server returned empty payload")

                with open(local_path, "wb" if offset == 0 else "ab") as fh:
                    fh.write(data)
                    fh.flush()
                    os.fsync(fh.fileno())

                completed_bytes += len(data)
                offset          += len(data)

                with jobs_lock:
                    job["downloaded_bytes"] = completed_bytes
                    job["chunks_done"]      = offset // CHUNK_SIZE
                    job["error"]            = None

                if offset < file_size:
                    with jobs_lock:
                        job["status"] = "pausing"
                    if _cancellable_sleep(job_id, SLEEP_BETWEEN_CHUNKS):
                        with jobs_lock:
                            job["status"]       = "cancelled"
                            job["current_file"] = None
                        cancel_events.pop(job_id, None)
                        return
                    with jobs_lock:
                        job["status"] = "downloading"

            except Exception as exc:
                if remote_file is not None:
                    try:
                        remote_file.close()
                    except Exception:
                        pass
                    remote_file = None
                sftp_close(sftp, transport)
                sftp = transport = None

                with jobs_lock:
                    job["status"] = "retrying"
                    job["error"]  = f"{exc}  — retrying in {RETRY_WAIT_SECS}s"

                if _cancellable_sleep(job_id, RETRY_WAIT_SECS):
                    with jobs_lock:
                        job["status"]       = "cancelled"
                        job["current_file"] = None
                    cancel_events.pop(job_id, None)
                    return

                # Recompute from actual on-disk sizes — the only ground truth
                disk_offset     = os.path.getsize(local_path) if os.path.exists(local_path) else 0
                completed_bytes = sum(sz for _, _, sz in file_list[:file_idx]) + disk_offset
                offset          = disk_offset

                with jobs_lock:
                    job["downloaded_bytes"] = completed_bytes
                    job["status"]           = "downloading"

        # ── File complete ─────────────────────────────────────────────────
        # Reset completed_bytes to the exact sum of finished files (avoids
        # any rounding drift from the += len(data) accumulation above).
        completed_files += 1
        completed_bytes  = sum(sz for _, _, sz in file_list[:completed_files])
        with jobs_lock:
            job["downloaded_bytes"] = completed_bytes
            job["completed_files"]  = completed_files

    with jobs_lock:
        job["status"]       = "complete"
        job["current_file"] = None
    cancel_events.pop(job_id, None)


# ─── Paced upload engine ──────────────────────────────────────────────────────

def _paced_upload(job_id: str) -> None:
    """
    Background thread that uploads a local file to the SFTP server in discrete
    chunks, closing the connection completely after each chunk and pausing for
    SLEEP_BETWEEN_CHUNKS seconds before the next one.

    Resume logic
    ─────────────
    On first run: stat the remote path.  If the remote file already has N bytes
    (from a previously interrupted upload), skip the first N bytes of the local
    file and open the remote file in append mode ("ab") so new chunks are written
    immediately after the existing data.  If offset == 0 (fresh upload), the
    remote file is created/truncated via "wb".

    On error: re-stat the remote file to get the actual committed byte count —
    this is the ground truth after any partial write — then re-seek the local
    file handle to that position and retry.

    The pacing (fresh connection + sleep between chunks) is identical to the
    download engine so the router gets the same breathing room in both directions.
    """
    job         = download_jobs[job_id]
    local_path  = job["local_path"]
    remote_path = job["remote_path"]
    total_size  = job["total_bytes"]

    sftp = transport = None

    # ── Step 1: derive starting byte offset (resume support) ─────────────
    try:
        sftp, transport = sftp_connect()
        try:
            offset = sftp.stat(remote_path).st_size
        except IOError:
            offset = 0   # file does not yet exist on the remote
        sftp_close(sftp, transport)
        sftp = transport = None
    except Exception as exc:
        with jobs_lock:
            job["status"] = "error"
            job["error"]  = f"Cannot connect to SFTP: {exc}"
        return

    with jobs_lock:
        job["downloaded_bytes"] = offset
        job["chunks_done"]      = offset // CHUNK_SIZE
        job["status"]           = "uploading"

    # ── Step 2: chunk loop ────────────────────────────────────────────────
    with open(local_path, "rb") as local_fh:
        local_fh.seek(offset)

        while offset < total_size:

            if cancel_events.get(job_id, threading.Event()).is_set():
                with jobs_lock:
                    job["status"] = "cancelled"
                cancel_events.pop(job_id, None)
                return

            try:
                chunk = local_fh.read(CHUNK_SIZE)
                if not chunk:
                    break

                # Fresh connection per chunk — router releases all state between bursts
                sftp, transport = sftp_connect()
                # "wb" on first chunk (creates / truncates); "ab" on resume chunks
                # so we always append exactly at the end of committed remote data.
                mode = "wb" if offset == 0 else "ab"
                remote_fh = sftp.open(remote_path, mode)
                remote_fh.write(chunk)
                remote_fh.close()
                sftp_close(sftp, transport)
                sftp = transport = None

                offset += len(chunk)
                with jobs_lock:
                    job["downloaded_bytes"] = offset
                    job["chunks_done"]      = offset // CHUNK_SIZE
                    job["error"]            = None

                if offset < total_size:
                    with jobs_lock:
                        job["status"] = "pausing"
                    if _cancellable_sleep(job_id, SLEEP_BETWEEN_CHUNKS):
                        with jobs_lock:
                            job["status"] = "cancelled"
                        cancel_events.pop(job_id, None)
                        return
                    with jobs_lock:
                        job["status"] = "uploading"

            except Exception as exc:
                sftp_close(sftp, transport)
                sftp = transport = None

                with jobs_lock:
                    job["status"] = "retrying"
                    job["error"]  = f"{exc}  — retrying in {RETRY_WAIT_SECS}s"

                if _cancellable_sleep(job_id, RETRY_WAIT_SECS):
                    with jobs_lock:
                        job["status"] = "cancelled"
                    cancel_events.pop(job_id, None)
                    return

                # Re-derive from remote stat — the only reliable ground truth
                # for how many bytes actually landed on the server.
                try:
                    sftp, transport = sftp_connect()
                    try:
                        offset = sftp.stat(remote_path).st_size
                    except IOError:
                        offset = 0
                    sftp_close(sftp, transport)
                    sftp = transport = None
                except Exception:
                    pass  # keep previous offset if we can't reconnect right now

                local_fh.seek(offset)
                with jobs_lock:
                    job["downloaded_bytes"] = offset
                    job["status"]           = "uploading"

    with jobs_lock:
        job["status"] = "complete"
    cancel_events.pop(job_id, None)


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    border = "═" * 58
    print(border)
    print("  PulseFetch  —  Paced SFTP File Explorer")
    print(border)
    print(f"  SFTP server : {SFTP_HOST}:{SFTP_PORT}  (user: {SFTP_USER})")
    print(f"  Chunk size  : {_fmt(CHUNK_SIZE)}  per transfer")
    print(f"  Cooldown    : {SLEEP_BETWEEN_CHUNKS}s pause between chunks")
    print(f"  Retry wait  : {RETRY_WAIT_SECS}s on error")
    print(f"  Saves to    : {LOCAL_DOWNLOAD_DIR}")
    print(border)
    print("  Open your browser →  http://127.0.0.1:5000")
    print(border)
    app.run(debug=False, host="127.0.0.1", port=5000)
