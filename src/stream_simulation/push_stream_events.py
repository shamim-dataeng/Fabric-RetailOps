"""
Pushes orders_stream_events.jsonl into a Fabric Eventstream Custom App source,
simulating a web application emitting live checkout events.

SETUP (one-time, in Fabric):
  1. Open your Eventstream item -> "Add source" -> "Custom App".
  2. It gives you a connection string + Event hub name. Copy both.
  3. Wire the source into your Eventstream, then add a destination
     (Lakehouse table "bronze_orders_stream", or Eventhouse if you use one).
  4. Publish the Eventstream.

USAGE:
  python push_stream_events.py \
      --conn-str "Endpoint=sb://....servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=..." \
      --eventhub-name "es_xxxxx" \
      --file output/orders_stream_events.jsonl \
      --events-per-sec 2

Notes for F2 capacity:
  - Keep --events-per-sec low (1-3). You don't need volume to prove the pattern,
    and it keeps you off the capacity-throttling edge.
  - Ctrl+C stops cleanly at any point; already-sent events are already in Bronze.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

from azure.eventhub import EventHubProducerClient, EventData

try:
    from dotenv import load_dotenv

    load_dotenv()  # reads .env in the current directory, if present
except ImportError:
    pass  # dotenv is optional; --conn-str/--eventhub-name flags still work without it


def load_events(path: Path):
    events = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def push_events(conn_str, eventhub_name, events, events_per_sec, batch_size=10):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=conn_str, eventhub_name=eventhub_name
    )

    delay = 1.0 / events_per_sec if events_per_sec > 0 else 0
    sent = 0
    total = len(events)

    try:
        with producer:
            batch = producer.create_batch()
            for i, event in enumerate(events):
                payload = json.dumps(event).encode("utf-8")
                try:
                    batch.add(EventData(payload))
                except ValueError:
                    # batch is full, send it and start a new one
                    producer.send_batch(batch)
                    sent += len(batch)
                    print(f"  sent batch, total sent: {sent}/{total}")
                    batch = producer.create_batch()
                    batch.add(EventData(payload))

                if (i + 1) % batch_size == 0:
                    producer.send_batch(batch)
                    sent += len(batch)
                    print(f"  sent batch, total sent: {sent}/{total}")
                    batch = producer.create_batch()
                    time.sleep(delay * batch_size)

            # flush whatever's left in the final partial batch
            if len(batch) > 0:
                producer.send_batch(batch)
                sent += len(batch)
                print(f"  sent final batch, total sent: {sent}/{total}")

    except KeyboardInterrupt:
        print(f"\nStopped early. Sent {sent}/{total} events.")
        sys.exit(0)

    print(f"\nDone. Sent {sent}/{total} events to '{eventhub_name}'.")


def main():
    parser = argparse.ArgumentParser(
        description="Replay JSONL events into a Fabric Eventstream."
    )
    parser.add_argument(
        "--conn-str",
        default=os.environ.get("EVENTSTREAM_CONN_STR"),
        help="Eventstream Custom App connection string (defaults to .env EVENTSTREAM_CONN_STR)",
    )
    parser.add_argument(
        "--eventhub-name",
        default=os.environ.get("EVENTSTREAM_EVENTHUB_NAME"),
        help="Eventstream Custom App event hub name (defaults to .env EVENTSTREAM_EVENTHUB_NAME)",
    )
    parser.add_argument(
        "--file", default="output/orders_stream_events.jsonl", help="Path to JSONL file"
    )
    parser.add_argument(
        "--events-per-sec",
        type=float,
        default=2.0,
        help="Target send rate (keep low on F2)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=10, help="Events per network send"
    )
    args = parser.parse_args()

    if not args.conn_str or not args.eventhub_name:
        print(
            "Missing connection details. Either set EVENTSTREAM_CONN_STR and "
            "EVENTSTREAM_EVENTHUB_NAME in a .env file, or pass --conn-str/--eventhub-name."
        )
        sys.exit(1)

    events = load_events(Path(args.file))
    print(f"Loaded {len(events)} events from {args.file}")
    print(
        f"Pushing to eventhub '{args.eventhub_name}' at ~{args.events_per_sec} events/sec...\n"
    )

    push_events(
        args.conn_str, args.eventhub_name, events, args.events_per_sec, args.batch_size
    )


if __name__ == "__main__":
    main()