#!/usr/bin/env python3
"""Convert a GTFS-realtime .pb file into pandas DataFrame(s).

Creates two CSVs (by default) in the output directory:
 - vehicles.csv: current vehicle positions
 - trip_updates.csv: stop time updates

Usage:
  python scripts/pb_to_dataframe.py sample_gtfs_realtime.pb
"""
import argparse
import os
import sys
import pandas as pd


def load_feed(pb_path):
    try:
        from google.transit import gtfs_realtime_pb2
    except Exception:
        from gtfs_realtime_bindings import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    with open(pb_path, "rb") as f:
        data = f.read()
    feed.ParseFromString(data)
    return feed


def feed_to_dataframes(feed):
    vehicles = []
    trip_updates = []

    header_ts = getattr(feed.header, "timestamp", None)

    for ent in feed.entity:
        ent_id = getattr(ent, "id", None)

        if ent.HasField("vehicle"):
            v = ent.vehicle
            trip = getattr(v, "trip", None)
            pos = getattr(v, "position", None)
            vehicle_obj = getattr(v, "vehicle", None)

            vehicles.append({
                "entity_id": ent_id,
                "header_timestamp": header_ts,
                "trip_id": getattr(trip, "trip_id", None) if trip is not None else None,
                "route_id": getattr(trip, "route_id", None) if trip is not None else None,
                "vehicle_id": getattr(vehicle_obj, "id", None) if vehicle_obj is not None else None,
                "vehicle_label": getattr(vehicle_obj, "label", None) if vehicle_obj is not None else None,
                "latitude": getattr(pos, "latitude", None) if pos is not None else None,
                "longitude": getattr(pos, "longitude", None) if pos is not None else None,
                "bearing": getattr(pos, "bearing", None) if pos is not None else None,
                "speed": getattr(pos, "speed", None) if pos is not None else None,
                "timestamp": getattr(v, "timestamp", None),
                "stop_id": getattr(v, "stop_id", None),
                "current_status": getattr(v, "current_status", None),
            })

        elif ent.HasField("trip_update"):
            tu = ent.trip_update
            trip = getattr(tu, "trip", None)
            for stu in tu.stop_time_update:
                arrival = getattr(stu, "arrival", None)
                departure = getattr(stu, "departure", None)
                trip_updates.append({
                    "entity_id": ent_id,
                    "header_timestamp": header_ts,
                    "trip_id": getattr(trip, "trip_id", None) if trip is not None else None,
                    "route_id": getattr(trip, "route_id", None) if trip is not None else None,
                    "stop_sequence": getattr(stu, "stop_sequence", None),
                    "stop_id": getattr(stu, "stop_id", None),
                    "arrival_time": getattr(arrival, "time", None) if arrival is not None else None,
                    "departure_time": getattr(departure, "time", None) if departure is not None else None,
                })

        elif ent.HasField("alert"):
            # alerts are not expanded into a table by default; skip or handle separately
            continue

    vehicles_df = pd.DataFrame(vehicles)
    trip_updates_df = pd.DataFrame(trip_updates)
    return vehicles_df, trip_updates_df


def main():
    parser = argparse.ArgumentParser(description="Convert GTFS-rt .pb to pandas DataFrame/CSV")
    parser.add_argument("pb_file", help="Path to .pb file", nargs="?", default="sample_gtfs_realtime.pb")
    parser.add_argument("--out-dir", help="Directory to write CSVs", default=".")
    parser.add_argument("--save-csv", help="Save DataFrames to CSV", action="store_true")
    args = parser.parse_args()

    if not os.path.exists(args.pb_file):
        print(f"File not found: {args.pb_file}")
        sys.exit(1)

    feed = load_feed(args.pb_file)
    vehicles_df, trip_updates_df = feed_to_dataframes(feed)

    print(f"Loaded feed header timestamp: {getattr(feed.header, 'timestamp', None)}")
    print(f"Vehicles: {len(vehicles_df)} rows")
    print(f"Trip updates: {len(trip_updates_df)} rows")

    if args.save_csv:
        os.makedirs(args.out_dir, exist_ok=True)
        vpath = os.path.join(args.out_dir, "vehicles.csv")
        tpath = os.path.join(args.out_dir, "trip_updates.csv")
        vehicles_df.to_csv(vpath, index=False)
        trip_updates_df.to_csv(tpath, index=False)
        print(f"Wrote {vpath} and {tpath}")

    # also show a small preview
    if not vehicles_df.empty:
        print("\nVehicles preview:")
        print(vehicles_df.head().to_string(index=False))
    if not trip_updates_df.empty:
        print("\nTrip updates preview:")
        print(trip_updates_df.head().to_string(index=False))


if __name__ == "__main__":
    main()
