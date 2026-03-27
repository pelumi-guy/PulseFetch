# PulseFetch — Paced SFTP File Explorer

A browser-based SFTP file explorer and downloader built specifically for **weak-CPU SFTP servers** — think home routers with USB storage (e.g. Huawei HG8145X7, TP-Link, ASUS).

> Vibe coded with [Claude Code](https://claude.ai/claude-code) by Anthropic.

---

## The Problem

Consumer routers that double as NAS/SFTP servers have tiny CPUs and small memory buffers. Attempting a standard large-file transfer saturates the CPU and fills the buffer, causing the connection to drop mid-transfer — leaving you with a corrupt, incomplete file and no easy way to resume.

## The Solution

PulseFetch downloads files in **discrete chunks**, closing the connection completely after each one and pausing for a configurable number of seconds before opening a new connection for the next chunk. This gives the router time to release all buffers, cool its CPU, and be ready for the next burst.

Key behaviour:
- **True byte-level seek** — uses `paramiko`'s `SFTPFile.seek(offset)` which maps to a single `SSH_FXP_READ` packet with the exact byte offset. No bytes before the target are transmitted over the wire.
- **Automatic resume** — if a download is interrupted, the next run re-derives the byte offset from the actual on-disk file size and picks up exactly where it left off. No double-writes, no gaps.
- **Fresh connection per chunk** — the router is never asked to hold a long-lived connection across the full transfer.
- **fsync after every chunk** — each chunk is flushed all the way to physical disk before the next connection is opened.

---

## Components

### `app.py` — Web UI

A Flask web application that provides:
- A file browser to navigate the remote SFTP server
- One-click download of individual files or entire folders
- Live progress tracking (bytes, percentage, chunk count, status)
- Cancel button that responds within ~1 second even during a long cooldown pause
- Concurrent download jobs tracked by UUID

Run it with:
```bash
python app.py
```
Then open [http://127.0.0.1:5000](http://127.0.0.1:5000) in your browser.

### `bulk_download.py` — CLI Bulk Downloader

A command-line script for unattended bulk downloads of entire directory trees. Designed for scenarios like downloading a full media library overnight.

Features:
- Iterates over every direct subfolder of a configured remote path
- Saves state to `.download_state.json` after each completed subfolder — re-running skips already-finished folders
- Graceful `Ctrl-C` handling: finishes the current chunk, saves state, exits cleanly
- Configurable pauses between chunks, between files, and between folders

```bash
python bulk_download.py              # normal run / auto-resume
python bulk_download.py --list       # list subfolders + status, no download
python bulk_download.py --folder "Season 1"   # download one named subfolder
python bulk_download.py --reset      # clear state, start fresh
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure credentials

Create a `.env` file in the project root (never commit this):

```env
SFTP_HOST=192.168.1.1
SFTP_PORT=22
SFTP_USER=your_username
SFTP_PASS=your_password
```

### 3. Tune the knobs (optional)

Edit the constants near the top of `app.py` or `bulk_download.py`:

| Setting | Default | Effect |
|---|---|---|
| `CHUNK_SIZE` | 100 MB | Size of each transfer burst. Reduce if the router still crashes. |
| `SLEEP_BETWEEN_CHUNKS` | 7 s | Cooldown pause between bursts. Increase for weaker routers. |
| `RETRY_WAIT_SECS` | 15 s | How long to wait before retrying after a connection error. |

---

## Requirements

- Python 3.11+
- `flask` — web interface
- `paramiko` — SFTP client
- `python-dotenv` — environment variable loading from `.env`
