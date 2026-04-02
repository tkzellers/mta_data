import time
import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2

DEFAULT_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"


def ingest_mta_data(url=DEFAULT_URL):
    response = requests.get(url)
    response.raise_for_status()

    file_size = len(response.content)
    print(f"Downloaded file size: {file_size:,} bytes ({file_size / 1024:.1f} KB)")

    # MTA GTFS-Realtime feeds are protobuf payloads (.pb), not zipped CSV files.
    t0 = time.perf_counter()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    parse_time = time.perf_counter() - t0
    print(f"Protobuf parse time: {parse_time * 1000:.1f} ms")

    t1 = time.perf_counter()
    rows = []
    for entity in feed.entity:
        row = {
            "id": entity.id,
            "is_deleted": entity.is_deleted,
        }

        if entity.HasField("trip_update"):
            row["type"] = "trip_update"
            row["trip_id"] = entity.trip_update.trip.trip_id
            row["route_id"] = entity.trip_update.trip.route_id
            row["start_date"] = entity.trip_update.trip.start_date
        elif entity.HasField("vehicle"):
            row["type"] = "vehicle"
            row["trip_id"] = entity.vehicle.trip.trip_id
            row["route_id"] = entity.vehicle.trip.route_id
            row["start_date"] = entity.vehicle.trip.start_date
        elif entity.HasField("alert"):
            row["type"] = "alert"
        else:
            row["type"] = "unknown"

        rows.append(row)

    df = pd.DataFrame(rows)
    df_time = time.perf_counter() - t1
    print(f"DataFrame build time: {df_time * 1000:.1f} ms")

    return df


if __name__ == "__main__":
    stops_df = ingest_mta_data()
    print(stops_df)
    print(f"\nTotal rows: {len(stops_df)}")
