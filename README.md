# Redis as a Real-Time Buffer for Elasticsearch

**YZV 322E — Applied Data Engineering | Tool #50 | Spring 2026 | Hadi Hassan | 150230926**

---

## 1. What Is This Tool?

Redis is an open-source, in-memory data structure store that operates as a database, cache, message broker, and streaming engine. In this demo, Redis acts as a **real-time ingestion buffer** between a data producer and Elasticsearch — decoupling event generation from indexing so neither side blocks or overwhelms the other. Its Redis Streams feature provides persistent, consumer-group-based message delivery with at-least-once guarantees.

---

## 2. Prerequisites

Docker
Python 3.10+
Redis, Elasticsearch, and Kibana all run in Docker.

---

## 3. Installation

```bash
# 1. Clone the repository
git clone https://github.com/hadihassan04/redis-demo.git
cd redis-demo

# 2. Start all services (Redis, Elasticsearch, Kibana)
docker compose up -d

# 3. Wait ~30 seconds for Elasticsearch to be healthy, then verify
docker compose ps

# 4. Install Python dependencies
pip install -r requirements.txt
```

---

## 4. Running the Example

Open **two terminals** in the project directory.

**Terminal 1 — Producer** (pushes events into Redis):

```bash
python producer.py
```

**Terminal 2 — Worker** (reads from Redis, indexes into Elasticsearch):

```bash
python worker.py
```

**Validate data in Elasticsearch:**

```bash
curl http://localhost:9200/events/_count
```

**Open Kibana:**

```
http://localhost:5601
```

Go to **Analytics → Discover**, create a data view with index pattern `events` and time field `timestamp`.

---

## 5. Expected Output

**producer.py:**

```
Producer started. Pushing events to Redis stream 'events'...
[1] 1714390000000-0 → user=7 action=click value=42
[2] 1714390000731-0 → user=3 action=purchase value=88
[3] 1714390001412-0 → user=15 action=view value=17
```

**worker.py:**

```
Consumer group 'workers' created on stream 'events'
Worker started. Waiting for events...

[1] Indexed → user=7 action=click value=42
[2] Indexed → user=3 action=purchase value=88
```

**Elasticsearch count:**

```json
{"count": 42, "_shards": {"total": 1, "successful": 1}}
```

### The Buffering Demo

This is the key demo that proves Redis's value:

```bash
# 1. Run producer (terminal 1)
python producer.py

# 2. Run worker (terminal 2)
python worker.py

# 3. Stop the worker (Ctrl+C in terminal 2)
#    → producer keeps running, Redis buffers all messages

# 4. Check buffered messages (backlog)
# For backlog while the worker is stopped, check the consumer group lag/pending.
docker exec -it redis redis-cli XINFO GROUPS events
# Look for fields like:
# - lag: how many new entries are waiting to be delivered to the group
# - pending: how many entries were delivered but not Acknowledged

# 5. Restart the worker
python worker.py
# → drains the backlog immediately, zero data lost
```

---

## 6. Architecture

```
producer.py
    │
    │  XADD events *  (Redis Streams)
    ▼
┌─────────┐
│  Redis  │  ← buffer / decoupler
└─────────┘
    │
    │  XREADGROUP (consumer group)
    ▼
 worker.py
    │
    │  es.index(...)
    ▼
┌───────────────┐     ┌────────┐
│ Elasticsearch │────▶│ Kibana │
└───────────────┘     └────────┘
```

**Why Redis in the middle?**

- **Buffer:** absorbs burst traffic; worker processes at its own pace
- **Decoupler:** producer and worker are independent processes
- **Replay:** unacknowledged messages replay automatically on worker restart

---

---

## 8. Teardown

```bash
docker compose down
```

To also delete all stored data:

```bash
docker compose down -v
```

---

## 9. AI Usage Disclosure

Claude Code (Anthropic) was used to generate the initial versions of `producer.py`, `worker.py`, `docker-compose.yml`. All generated output was reviewed, tested, and edited by me before submission.