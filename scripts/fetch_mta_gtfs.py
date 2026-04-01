#!/usr/bin/env python3
"""Fetch a sample GTFS-realtime feed from the MTA API and optionally parse it.

Usage:
  - set environment variable `MTA_API_KEY` to your MTA developer key
  - install requirements from `requirements.txt` (optional parsing)
  - run: `python scripts/fetch_mta_gtfs.py`
"""
import os
import sys
import requests


def main():
    url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"

    try:
        resp = requests.get(url, timeout=15)
    except requests.RequestException as e:
        print(f"Request error: {e}")
        sys.exit(1)

    if resp.status_code != 200:
        print(f"Request failed: {resp.status_code} {resp.reason}")
        print(resp.text[:1000])
        sys.exit(1)

    out_file = "sample_gtfs_realtime.pb"
    with open(out_file, "wb") as f:
        f.write(resp.content)

    print(f"Saved {len(resp.content)} bytes to {out_file}")

    # Try to parse if gtfs-realtime bindings are installed
    try:
        from google.transit import gtfs_realtime_pb2
    except Exception:
        try:
            from gtfs_realtime_bindings import gtfs_realtime_pb2
        except Exception:
            print("gtfs-realtime bindings not installed; skipping parse step.")
            print("To parse, install: pip install -r requirements.txt")
            return

    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(resp.content)
    except Exception as e:
        print(f"Failed to parse protobuf: {e}")
        return

    ts = getattr(feed.header, "timestamp", None)
    print(f"Feed header timestamp: {ts}")

    print("Entities (up to 10):")
    for i, ent in enumerate(feed.entity[:10]):
        ent_id = getattr(ent, "id", "(no id)")
        if ent.HasField("vehicle"):
            v = ent.vehicle
            trip = getattr(v, "trip", None)
            route = getattr(trip, "route_id", "") if trip is not None else ""
            trip_id = getattr(trip, "trip_id", "") if trip is not None else ""
            print(f"- {i} id={ent_id} vehicle trip_id={trip_id} route={route}")
        elif ent.HasField("trip_update"):
            tu = ent.trip_update
            trip = getattr(tu, "trip", None)
            route = getattr(trip, "route_id", "") if trip is not None else ""
            trip_id = getattr(trip, "trip_id", "") if trip is not None else ""
            print(f"- {i} id={ent_id} trip_update trip_id={trip_id} route={route}")
        else:
            print(f"- {i} id={ent_id} (unknown entity type)")


if __name__ == "__main__":
    main()
