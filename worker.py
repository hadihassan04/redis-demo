import redis
import time
from elasticsearch import Elasticsearch

STREAM = "events"
GROUP = "workers"
CONSUMER = "worker-1"
ES_INDEX = "events"

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
es = Elasticsearch("http://localhost:9200")

def create_index():
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX, body={
            "mappings": {
                "properties": {
                    "user_id":   {"type": "integer"},
                    "action":    {"type": "keyword"},
                    "value":     {"type": "integer"},
                    "timestamp": {"type": "date", "format": "epoch_millis"},
                }
            }
        })
        print(f"Created index '{ES_INDEX}'")

def create_group():
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        print(f"Consumer group '{GROUP}' created on stream '{STREAM}'")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"Consumer group '{GROUP}' already exists — resuming from last position")
        else:
            raise

def process_messages():
    indexed = 0
    print("Worker started. Waiting for events...\n")
    while True:
        messages = r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=10, block=1000)
        if not messages:
            continue
        for _, entries in messages:
            for msg_id, fields in entries:
                try:
                    doc = {
                        "user_id":   int(fields["user_id"]),
                        "action":    fields["action"],
                        "value":     int(fields["value"]),
                        "timestamp": int(fields["timestamp"]),
                    }
                    es.index(index=ES_INDEX, document=doc)
                    r.xack(STREAM, GROUP, msg_id)
                    indexed += 1
                    print(f"[{indexed}] Indexed → user={doc['user_id']} action={doc['action']} value={doc['value']}")
                except Exception as exc:
                    print(f"Failed to index {msg_id}: {exc} — will retry on restart")

if __name__ == "__main__":
    print("Waiting for Elasticsearch to be ready...")
    for _ in range(30):
        try:
            if es.ping():
                break
        except Exception:
            pass
        time.sleep(2)
    else:
        print("Elasticsearch not reachable after 60s. Is it running?")
        raise SystemExit(1)

    create_index()
    create_group()
    try:
        process_messages()
    except KeyboardInterrupt:
        pending = r.xpending(STREAM, GROUP)
        print(f"\nStopped. {pending['pending']} messages pending in Redis — will replay on restart.")
