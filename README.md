# Fetch MTA GTFS-realtime sample

This small utility fetches a sample GTFS-realtime feed from the MTA API and saves it to `sample_gtfs_realtime.pb`.

Prerequisites
- Python 3.8+
- An MTA developer key (set in the environment variable `MTA_API_KEY`).

Install

```bash
pip install -r requirements.txt
```

On Windows (PowerShell):

```powershell
$env:MTA_API_KEY = "YOUR_KEY_HERE"
python scripts/fetch_mta_gtfs.py
```

On macOS / Linux:

```bash
export MTA_API_KEY=YOUR_KEY_HERE
python scripts/fetch_mta_gtfs.py
```

The script will:
- save the raw protobuf to `sample_gtfs_realtime.pb`
- attempt to parse and print up to 10 entities (requires `gtfs-realtime-bindings`)
