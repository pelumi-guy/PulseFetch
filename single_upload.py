#!/usr/bin/env python3
"""
single_upload.py  —  PulseFetch Single File Uploader
==========================================================
Uploads a single file from the local PC to the root directory of the
remote SFTP server using the paced-chunk logic.
Designed for routers whose tiny CPU/buffer cannot sustain
a continuous large transfer.

Resume behaviour
----------------
Checks the file size on the remote server before starting.
Resumes any partially uploaded file from its exact remote byte offset.

Usage
-----
    python single_upload.py <local_file_path>

Ctrl-C is caught gracefully: the current chunk is finished and the script
exits cleanly. Re-run to continue from the same point.
"""

import os
import sys
import time
import signal
import logging
import argparse
from pathlib import Path

import paramiko
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION  ← must match app.py / your router settings
# ══════════════════════════════════════════════════════════════════════════════

SFTP_HOST  = os.environ["SFTP_HOST"]
SFTP_PORT  = int(os.environ["SFTP_PORT"])
SFTP_USER  = os.environ["SFTP_USER"]
SFTP_PASS  = os.environ["SFTP_PASS"]

# The script uploads to the root directory by default as requested
REMOTE_DIR = "/"

# ── Tuning knobs ─────────────────────────────────────────────────────────────
CHUNK_SIZE            = 100 * 1024 * 1024   # 100 MB per chunk (same as app.py)
SLEEP_BETWEEN_CHUNKS  = 7                   # seconds — CPU+buffer cooldown inside a file
RETRY_WAIT_SECS       = 15                  # seconds — wait before retrying a failed chunk
MAX_CHUNK_RETRIES     = 5                   # abandon a file after this many consecutive chunk failures

# ══════════════════════════════════════════════════════════════════════════════

_interrupted = False

def _handle_interrupt(sig, frame):
    global _interrupted
    _interrupted = True
    print("\n\n[!] Interrupt received — finishing current chunk, then stopping safely.\n"
          "    Re-run to resume from this point.\n", flush=True)

signal.signal(signal.SIGINT, _handle_interrupt)

# ── Logging ──────────────────────────────────────────────────────────────────

def setup_logging():
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.addHandler(ch)

# ── Formatting helpers ───────────────────────────────────────────────────────

def fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"

def fmt_pct(done: int, total: int) -> str:
    return f"{done / total * 100:.1f}%" if total else "0.0%"

def fmt_eta(done: int, total: int, elapsed_s: float) -> str:
    if done == 0 or elapsed_s < 1:
        return "--:--"
    rate = done / elapsed_s           # bytes/sec
    remaining = (total - done) / rate # seconds
    m, s = divmod(int(remaining), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m"
    return f"{m}m{s:02d}s"

# ── SFTP helpers ─────────────────────────────────────────────────────────────

def sftp_connect() -> tuple:
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport

def sftp_close(sftp, transport) -> None:
    for obj in (sftp, transport):
        if obj is not None:
            try:
                obj.close()
            except Exception:
                pass

# ── Core: chunked file upload ────────────────────────────────────────────────

def upload_file(local_path: str, remote_path: str) -> bool:
    if not os.path.isfile(local_path):
        logging.error("Local file does not exist: %s", local_path)
        return False

    file_size = os.path.getsize(local_path)
    filename = os.path.basename(local_path)

    # ── Already done / Resume Offset ──────────────────────────────────────
    sftp, transport = sftp_connect()
    try:
        offset = sftp.stat(remote_path).st_size
    except IOError:
        offset = 0
    sftp_close(sftp, transport)

    if offset >= file_size:
        logging.info("    SKIP (already complete)  %s", filename)
        return True

    if offset > 0:
        logging.info("    RESUME  %s  at %s / %s", filename, fmt_bytes(offset), fmt_bytes(file_size))
    else:
        logging.info("    START   %s  (%s)", filename, fmt_bytes(file_size))

    consecutive_failures = 0
    start_time = time.monotonic()

    with open(local_path, "rb") as local_fh:
        while offset < file_size:
            if _interrupted:
                return False

            try:
                # Seek local file
                local_fh.seek(offset)
                to_read = min(CHUNK_SIZE, file_size - offset)
                chunk_data = local_fh.read(to_read)

                if not chunk_data:
                    break

                # Fresh connection every chunk
                sftp, transport = sftp_connect()
                
                # "wb" for the first chunk to ensure a clean slate, "ab" to resume/append
                remote_mode = "wb" if offset == 0 else "ab"
                remote_file = sftp.open(remote_path, remote_mode)
                
                # Write and close properly
                remote_file.write(chunk_data)
                remote_file.close()
                sftp_close(sftp, transport)

                offset += len(chunk_data)
                consecutive_failures = 0
                elapsed = time.monotonic() - start_time
                pct = fmt_pct(offset, file_size)
                eta = fmt_eta(offset, file_size, elapsed)

                print(f"\r      [{pct:>6}]  {fmt_bytes(offset):>10} / {fmt_bytes(file_size)}"
                      f"  ETA {eta:>7}  chunk {offset // CHUNK_SIZE}", end="", flush=True)

                if offset < file_size:
                    time.sleep(SLEEP_BETWEEN_CHUNKS)

            except Exception as exc:
                sftp_close(sftp, transport)

                consecutive_failures += 1
                logging.warning("\n    Chunk error (%d/%d): %s",
                                consecutive_failures, MAX_CHUNK_RETRIES, exc)

                if consecutive_failures >= MAX_CHUNK_RETRIES:
                    logging.error("    GIVING UP on %s after %d consecutive failures", filename, MAX_CHUNK_RETRIES)
                    return False

                time.sleep(RETRY_WAIT_SECS)

                # Ground truth from server
                sftp, transport = sftp_connect()
                try:
                    offset = sftp.stat(remote_path).st_size
                except IOError:
                    offset = 0
                sftp_close(sftp, transport)

    print()
    return True

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="PulseFetch single file uploader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("file", metavar="LOCAL_FILE",
                        help="The local file to upload")
    parser.add_argument("--remote-dir", default=REMOTE_DIR,
                        help=f"The remote directory to upload to (default: {REMOTE_DIR})")
    args = parser.parse_args()

    setup_logging()

    local_file = os.path.abspath(args.file)
    if not os.path.isfile(local_file):
        logging.error("File not found: %s", local_file)
        sys.exit(1)

    filename = os.path.basename(local_file)
    remote_path = args.remote_dir.rstrip("/") + "/" + filename

    # ── Header ────────────────────────────────────────────────────────────
    logging.info("=" * 64)
    logging.info("  PulseFetch — Single File Uploader")
    logging.info("  Source : %s", local_file)
    logging.info("  Dest   : sftp://%s:%s%s", SFTP_HOST, SFTP_PORT, remote_path)
    logging.info("  Chunk  : %s  |  Cooldown: %ss",
                 fmt_bytes(CHUNK_SIZE), SLEEP_BETWEEN_CHUNKS)
    logging.info("=" * 64)
    logging.info("")

    ok = upload_file(local_file, remote_path)

    logging.info("")
    logging.info("=" * 64)
    if _interrupted:
        logging.info("  STOPPED  —  Upload incomplete.")
    elif ok:
        logging.info("  FINISHED  —  Upload complete.")
    else:
        logging.info("  FAILED  —  Upload aborted.")
    logging.info("=" * 64)


if __name__ == "__main__":
    main()
