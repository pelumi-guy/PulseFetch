#!/usr/bin/env python3
"""
bulk_download.py  —  PulseFetch Bulk Subfolder Downloader
==========================================================
Iterates over every direct subfolder of a remote SFTP directory and
downloads it completely using the same paced-chunk logic as app.py.
Designed for the Huawei HG8145X7 whose tiny CPU/buffer cannot sustain
a continuous large transfer.

Resume behaviour
----------------
A JSON state file (.download_state.json) is written after every completed
subfolder.  Re-running the script skips already-finished subfolders and
resumes any partially downloaded file from its exact on-disk byte offset.

Usage
-----
    python bulk_download.py              # normal run / auto-resume
    python bulk_download.py --list       # list subfolders + status, no download
    python bulk_download.py --folder "Season 1"   # download ONE named subfolder only
    python bulk_download.py --reset      # clear saved state, start completely fresh

Ctrl-C is caught gracefully: the current chunk is finished, state is saved,
and the script exits cleanly.  Re-run to continue from the same point.
"""

import os
import sys
import stat
import time
import json
import signal
import logging
import argparse
from datetime import datetime
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

# The SFTP path that corresponds to G:\Watermarked_Listings\listings.
# Open PulseFetch in your browser, navigate to that folder, and copy the
# path shown in the breadcrumb — then paste it here.
REMOTE_BASE_PATH = "/Watermarked_Listings/listings"

# Where the files land on this machine
LOCAL_BASE_DIR = str(Path.home() / "Downloads" / "PulseFetch" / "listings")

# ── Tuning knobs ─────────────────────────────────────────────────────────────
CHUNK_SIZE            = 100 * 1024 * 1024   # 100 MB per chunk (same as app.py)
SLEEP_BETWEEN_CHUNKS  = 7                   # seconds — CPU+buffer cooldown inside a file
SLEEP_BETWEEN_FILES   = 3                   # seconds — micro-pause between consecutive files
SLEEP_BETWEEN_FOLDERS = 25                  # seconds — longer cooldown between subfolders
RETRY_WAIT_SECS       = 15                  # seconds — wait before retrying a failed chunk
MAX_CHUNK_RETRIES     = 5                   # abandon a file after this many consecutive chunk failures

# ── Internals ─────────────────────────────────────────────────────────────────
STATE_FILE = os.path.join(LOCAL_BASE_DIR, ".download_state.json")
LOG_FILE   = os.path.join(LOCAL_BASE_DIR, "download_log.txt")

# ══════════════════════════════════════════════════════════════════════════════

_interrupted = False   # set to True by Ctrl-C handler


def _handle_interrupt(sig, frame):
    global _interrupted
    _interrupted = True
    print("\n\n[!] Interrupt received — finishing current chunk, then stopping safely.\n"
          "    Re-run to resume from this point.\n", flush=True)


signal.signal(signal.SIGINT, _handle_interrupt)


# ── Logging ──────────────────────────────────────────────────────────────────

def setup_logging():
    os.makedirs(LOCAL_BASE_DIR, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)

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
    """Rough ETA string based on average throughput so far."""
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
    """Open a fresh Transport + SFTPClient.  Returns (sftp, transport)."""
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


# ── State management ─────────────────────────────────────────────────────────

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "completed_folders": [],
        "skipped_files":     [],
        "started_at":        None,
        "last_updated":      None,
    }


def save_state(state: dict) -> None:
    """Atomic write via a temp file so a crash never corrupts the state file."""
    state["last_updated"] = datetime.now().isoformat(timespec="seconds")
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


# ── Remote tree walk ─────────────────────────────────────────────────────────

def walk_remote(sftp, remote_dir: str, local_dir: str, file_list: list | None = None) -> list:
    """
    Recursively walk `remote_dir` and build a flat list of
    (remote_path, local_path, size_bytes) tuples for every file.
    Directories that cannot be listed are logged and skipped gracefully.
    """
    if file_list is None:
        file_list = []
    try:
        entries = sftp.listdir_attr(remote_dir)
    except Exception as exc:
        logging.warning("    Cannot list %s: %s", remote_dir, exc)
        return file_list

    for entry in sorted(entries, key=lambda a: a.filename.lower()):
        r_path = remote_dir.rstrip("/") + "/" + entry.filename
        l_path = os.path.join(local_dir, entry.filename)
        if stat.S_ISDIR(entry.st_mode or 0):
            walk_remote(sftp, r_path, l_path, file_list)
        else:
            file_list.append((r_path, l_path, entry.st_size or 0))
    return file_list


# ── Core: chunked file download ───────────────────────────────────────────────

def download_file(remote_path: str, local_path: str, file_size: int,
                  label: str = "", start_time: float = 0.0) -> bool:
    """
    Download a single remote file using the paced-chunk logic.

    Chunk / seek mechanics
    ----------------------
    Each iteration of the while-loop opens a *brand-new* SFTP connection,
    calls remote_file.seek(offset) — which instructs the SSH server to start
    reading from byte `offset` with zero wasted transfer — reads exactly
    CHUNK_SIZE bytes, then immediately closes the connection so the router
    can drain its buffers and cool its CPU before the next chunk.

    Resume safety
    -------------
    After any error, `offset` is re-derived from os.path.getsize(local_path).
    This is the only ground-truth of what is safely on disk; we never trust
    the in-memory counter after a failure.

    Returns True on success, False if the file was permanently skipped.
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # ── Already done? ─────────────────────────────────────────────────────
    offset = os.path.getsize(local_path) if os.path.exists(local_path) else 0
    if offset >= file_size:
        logging.info("    SKIP (already complete)  %s", label)
        return True

    if offset > 0:
        logging.info("    RESUME  %s  at %s / %s", label, fmt_bytes(offset), fmt_bytes(file_size))
    else:
        logging.info("    START   %s  (%s)", label, fmt_bytes(file_size))

    sftp = transport = remote_file = None
    consecutive_failures = 0
    chunk_start = time.monotonic()

    while offset < file_size:
        if _interrupted:
            return False

        try:
            # Fresh connection every chunk — router releases state between transfers
            sftp, transport = sftp_connect()
            remote_file = sftp.open(remote_path, "rb")
            remote_file.seek(offset)                          # ← true SSH-level seek
            to_read = min(CHUNK_SIZE, file_size - offset)
            data    = remote_file.read(to_read)               # blocks until chunk arrives
            remote_file.close(); remote_file = None
            sftp_close(sftp, transport); sftp = transport = None

            if not data:
                raise IOError("Server returned an empty payload")

            # Append to local file ("wb" only for the very first byte of a new file)
            with open(local_path, "wb" if offset == 0 else "ab") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())       # flush OS write-back cache → physical disk

            offset               += len(data)
            consecutive_failures  = 0
            elapsed               = time.monotonic() - (start_time or chunk_start)
            pct                   = fmt_pct(offset, file_size)
            eta                   = fmt_eta(offset, file_size, elapsed)

            print(f"\r      [{pct:>6}]  {fmt_bytes(offset):>10} / {fmt_bytes(file_size)}"
                  f"  ETA {eta:>7}  chunk {offset // CHUNK_SIZE}", end="", flush=True)

            # Cooldown — let router CPU and buffer recover before next chunk
            if offset < file_size:
                time.sleep(SLEEP_BETWEEN_CHUNKS)

        except Exception as exc:
            # Clean up dangling handles
            if remote_file is not None:
                try:
                    remote_file.close()
                except Exception:
                    pass
                remote_file = None
            sftp_close(sftp, transport)
            sftp = transport = None

            consecutive_failures += 1
            logging.warning("    Chunk error (%d/%d): %s",
                            consecutive_failures, MAX_CHUNK_RETRIES, exc)

            if consecutive_failures >= MAX_CHUNK_RETRIES:
                print()
                logging.error("    GIVING UP on %s after %d consecutive failures", label, MAX_CHUNK_RETRIES)
                return False

            time.sleep(RETRY_WAIT_SECS)

            # Recompute from actual on-disk file size — ground truth after failure
            offset = os.path.getsize(local_path) if os.path.exists(local_path) else 0

    print()   # newline after the inline progress bar
    return True


# ── Subfolder list ────────────────────────────────────────────────────────────

def list_top_subfolders(sftp, remote_path: str) -> list[tuple[str, str]]:
    """Return sorted [(name, full_remote_path)] for direct child directories."""
    try:
        entries = sftp.listdir_attr(remote_path)
    except Exception as exc:
        logging.error("Cannot list %s: %s", remote_path, exc)
        sys.exit(1)
    return sorted(
        [(e.filename, remote_path.rstrip("/") + "/" + e.filename)
         for e in entries if stat.S_ISDIR(e.st_mode or 0)],
        key=lambda x: x[0].lower(),
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="PulseFetch bulk subfolder downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--list",   action="store_true",
                        help="List subfolders and their download status, then exit")
    parser.add_argument("--folder", metavar="NAME",
                        help="Download a single named subfolder instead of all")
    parser.add_argument("--reset",  action="store_true",
                        help="Clear saved state and re-download everything from scratch")
    args = parser.parse_args()

    setup_logging()
    os.makedirs(LOCAL_BASE_DIR, exist_ok=True)

    state = load_state()
    if args.reset:
        state = {"completed_folders": [], "skipped_files": [], "started_at": None, "last_updated": None}
        save_state(state)
        logging.info("State cleared — starting fresh.")

    if not state["started_at"]:
        state["started_at"] = datetime.now().isoformat(timespec="seconds")

    # ── Header ────────────────────────────────────────────────────────────
    logging.info("=" * 64)
    logging.info("  PulseFetch — Bulk Subfolder Downloader")
    logging.info("  Source : sftp://%s:%s%s", SFTP_HOST, SFTP_PORT, REMOTE_BASE_PATH)
    logging.info("  Dest   : %s", LOCAL_BASE_DIR)
    logging.info("  Chunk  : %s  |  Cooldown: %ss  |  Folder gap: %ss",
                 fmt_bytes(CHUNK_SIZE), SLEEP_BETWEEN_CHUNKS, SLEEP_BETWEEN_FOLDERS)
    logging.info("=" * 64)

    # ── Fetch subfolder list ──────────────────────────────────────────────
    logging.info("Connecting to list subfolders…")
    sftp, transport = sftp_connect()
    subfolders = list_top_subfolders(sftp, REMOTE_BASE_PATH)
    sftp_close(sftp, transport)

    if not subfolders:
        logging.info("No subfolders found in %s — nothing to do.", REMOTE_BASE_PATH)
        sys.exit(0)

    # Filter to a single folder if --folder was supplied
    if args.folder:
        subfolders = [(n, p) for n, p in subfolders if n == args.folder]
        if not subfolders:
            logging.error("Subfolder '%s' not found in %s", args.folder, REMOTE_BASE_PATH)
            sys.exit(1)

    # ── List mode ────────────────────────────────────────────────────────
    logging.info("Found %d subfolder(s):", len(subfolders))
    for i, (name, _) in enumerate(subfolders, 1):
        mark = "✓ done" if name in state["completed_folders"] else "pending"
        logging.info("  %2d.  %-40s  [%s]", i, name, mark)

    if args.list:
        sys.exit(0)

    # ── Download loop ─────────────────────────────────────────────────────
    total_folders = len(subfolders)
    done_count    = 0
    skipped_files = list(state["skipped_files"])
    overall_start = time.monotonic()

    for folder_idx, (folder_name, remote_folder) in enumerate(subfolders, 1):
        if _interrupted:
            break

        if folder_name in state["completed_folders"]:
            logging.info("[%d/%d]  SKIP (already done) — %s", folder_idx, total_folders, folder_name)
            done_count += 1
            continue

        logging.info("")
        logging.info("━" * 64)
        logging.info("[%d/%d]  Subfolder: %s", folder_idx, total_folders, folder_name)
        logging.info("━" * 64)

        local_folder = os.path.join(LOCAL_BASE_DIR, folder_name)
        os.makedirs(local_folder, exist_ok=True)

        # Scan the subfolder tree (fresh connection)
        logging.info("  Scanning remote file tree…")
        try:
            sftp, transport = sftp_connect()
            file_list = walk_remote(sftp, remote_folder, local_folder)
            sftp_close(sftp, transport)
        except Exception as exc:
            logging.error("  Cannot scan '%s': %s — skipping folder.", folder_name, exc)
            continue

        if not file_list:
            logging.info("  Folder is empty — marking complete.")
            state["completed_folders"].append(folder_name)
            save_state(state)
            done_count += 1
            continue

        total_files = len(file_list)
        total_bytes = sum(sz for _, _, sz in file_list)
        logging.info("  %d file(s)   %s", total_files, fmt_bytes(total_bytes))

        folder_ok   = True
        folder_start = time.monotonic()

        for file_idx, (remote_path, local_path, file_size) in enumerate(file_list, 1):
            if _interrupted:
                folder_ok = False
                break

            rel   = os.path.relpath(local_path, local_folder)
            label = f"{rel}  [{file_idx}/{total_files}]"

            ok = download_file(
                remote_path, local_path, file_size,
                label=label,
                start_time=folder_start,
            )

            if not ok:
                skipped_files.append(remote_path)
                state["skipped_files"] = skipped_files
                save_state(state)
                if _interrupted:
                    folder_ok = False
                    break
                # Non-interrupt failure: log and continue to next file
                logging.warning("  Skipped (max retries exceeded): %s", remote_path)

            # Micro-pause between files — router breathing room
            if file_idx < total_files and not _interrupted:
                time.sleep(SLEEP_BETWEEN_FILES)

        if folder_ok and not _interrupted:
            state["completed_folders"].append(folder_name)
            save_state(state)
            done_count += 1
            folder_elapsed = time.monotonic() - folder_start
            m, s = divmod(int(folder_elapsed), 60)
            logging.info("  ✓  Complete in %dm%02ds — %s", m, s, folder_name)

        # Longer cooldown between subfolders — gives router the most recovery time
        if folder_idx < total_folders and not _interrupted:
            logging.info("  Cooling down %ds before next subfolder…", SLEEP_BETWEEN_FOLDERS)
            time.sleep(SLEEP_BETWEEN_FOLDERS)

    # ── Summary ───────────────────────────────────────────────────────────
    total_elapsed = time.monotonic() - overall_start
    h, rem = divmod(int(total_elapsed), 3600)
    m, s   = divmod(rem, 60)

    logging.info("")
    logging.info("=" * 64)
    if _interrupted:
        logging.info("  STOPPED  —  %d / %d subfolders fully complete.", done_count, total_folders)
        logging.info("  Re-run the script to resume from where you left off.")
    else:
        logging.info("  FINISHED  —  %d / %d subfolders complete.", done_count, total_folders)
    logging.info("  Total time : %dh %02dm %02ds", h, m, s)
    if skipped_files:
        logging.warning("  %d file(s) permanently skipped after %d failed chunks:",
                        len(skipped_files), MAX_CHUNK_RETRIES)
        for f in skipped_files:
            logging.warning("    - %s", f)
    logging.info("  Local path : %s", LOCAL_BASE_DIR)
    logging.info("  Log file   : %s", LOG_FILE)
    logging.info("=" * 64)


if __name__ == "__main__":
    main()
