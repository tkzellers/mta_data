import json
import time

import requests


SUBWAY_ALERTS_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json"
BUS_ALERTS_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fbus-alerts.json"


def fetch_mta_alerts(url, label):
    response = requests.get(url)
    response.raise_for_status()

    file_size = len(response.content)
    print(f"{label} downloaded file size: {file_size:,} bytes ({file_size / 1024:.1f} KB)")

    parse_start = time.perf_counter()
    payload = json.loads(response.content)
    parse_time = time.perf_counter() - parse_start
    print(f"{label} JSON parse time: {parse_time * 1000:.1f} ms")

    return payload


if __name__ == "__main__":
    subway_alerts = fetch_mta_alerts(SUBWAY_ALERTS_URL, "Subway alerts")
    if isinstance(subway_alerts, dict):
        print(f"Subway alerts top-level keys: {list(subway_alerts.keys())}")
    else:
        print(f"Subway alerts parsed JSON type: {type(subway_alerts).__name__}")

    print()

    bus_alerts = fetch_mta_alerts(BUS_ALERTS_URL, "Bus alerts")
    if isinstance(bus_alerts, dict):
        print(f"Bus alerts top-level keys: {list(bus_alerts.keys())}")
    else:
        print(f"Bus alerts parsed JSON type: {type(bus_alerts).__name__}")