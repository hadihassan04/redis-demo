import redis
import random
import time

ACTIONS = ["click", "view", "purchase"]

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

print("Producer started. Pushing events to Redis stream 'events'...")
print("Press Ctrl+C to stop.\n")

count = 0
try:
    while True:
        event = {
            "user_id": str(random.randint(1, 20)),
            "action": random.choice(ACTIONS),
            "value": str(random.randint(1, 100)),
            "timestamp": str(int(time.time() * 1000)),
        }
        msg_id = r.xadd("events", event)
        count += 1
        print(f"[{count}] {msg_id} → user={event['user_id']} action={event['action']} value={event['value']}")
        time.sleep(random.uniform(0.5, 1.0))
except KeyboardInterrupt:
    stream_len = r.xlen("events")
    print(f"\nStopped. Stream length: {stream_len} messages waiting in Redis.")
