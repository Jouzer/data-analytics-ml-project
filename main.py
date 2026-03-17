import json
import threading
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import hashlib
import time

load_dotenv()

# Lisättiin .env jottei salasanat yms. päädy GitHubiin
BROKER = os.getenv("MQTT_BROKER")
PORT = int(os.getenv("MQTT_PORT", "1883"))
USERNAME = os.getenv("MQTT_USERNAME")
PASSWORD = os.getenv("MQTT_PASSWORD")
TOPIC = os.getenv("MQTT_TOPIC", "automaatio")
MONGO_URI = os.getenv("MONGO_URI")

# =========================
# DATA PARSER
# =========================

# Robogaragen datassa ei timestamp, tehty fallback
def parse_timestamp(data: dict):
    candidates = [
        ("DateTime", "%d %b %Y %H:%M:%S"),
        ("Time", "%d %b %Y %H:%M:%S"),
    ]

    for field, fmt in candidates:
        value = data.get(field)
        if not value:
            continue
        try:
            # parsitaan ja tehdään siitä UTC-aware datetime
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    # fallback: nykyhetki
    return datetime.now(timezone.utc)


def parse_person_count(data: dict):
    if "person count" in data:
        return data.get("person count", 0)
    if "pCount" in data:
        return data.get("pCount", 0)
    return 0


def parse_message(payload: str) -> dict | None:
    try:
        data = json.loads(payload)

        parsed = {
            "device_id": data.get("id"),
            "person_count": parse_person_count(data),
            "db_name": data.get("db_name", "data_ml"),
            "collection": data.get("coll_name", "default"),
            "timestamp": parse_timestamp(data),
            "temperature": data.get("T"),
            "humidity": data.get("H"),
            "dew_point": data.get("DP"),
            "co2": data.get("CO2"),
            "raw": data,
        }

        return parsed

    except Exception as e:
        print("❌ JSON parse error:", e)
        return None

# =========================
# MQTT HANDLER CLASS
# =========================
class MQTTIngestor:
    def __init__(self, on_data_callback=None):
        self.client = mqtt.Client()
        self.client.username_pw_set(USERNAME, PASSWORD)

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

        self.on_data_callback = on_data_callback

        self.lock = threading.Lock()

    def on_connect(self, client, userdata, flags, rc):
        print("✅ Connected to MQTT broker with code:", rc)
        client.subscribe(TOPIC)

    def on_disconnect(self, client, userdata, rc):
        print(f"⚠️ Disconnected from MQTT (rc={rc})")

        while True:
            try:
                print("🔄 Trying to reconnect...")
                client.reconnect()
                print("✅ Reconnected!")
                break
            except Exception as e:
                print(f"❌ Reconnect failed: {e}")
                time.sleep(5)

    def on_message(self, client, userdata, msg):
        payload = msg.payload.decode()

        parsed = parse_message(payload)
        if not parsed:
            return

        # thread-safe käsittely
        with self.lock:
            print("📥 Received:", parsed)

            if self.on_data_callback:
                self.on_data_callback(parsed)

    def start(self):
        print("🚀 Starting MQTT ingestor...")
        self.client.connect(BROKER, PORT)
        self.client.loop_forever()


# =========================
# MONGO CALLBACK
# =========================
mongo = MongoClient(MONGO_URI)
db = mongo["data_ml"]
collection = db["readings"]

collection.create_index(
    "message_hash",
    unique=True,
    partialFilterExpression={"message_hash": {"$exists": True}}
)
collection.create_index([("device_id", 1), ("timestamp", 1)])

def make_message_hash(data: dict) -> str:
    hashable = {
        k: v for k, v in data.items()
    }
    hash_str = json.dumps(hashable, sort_keys=True, default=str)
    return hashlib.md5(hash_str.encode("utf-8")).hexdigest()

def handle_data(data):
    message_hash = make_message_hash(data)

    doc = {
        "device_id": data.get("device_id"),
        "timestamp": data.get("timestamp"),
        "person_count": data.get("person_count", 0),
        "temperature": data.get("temperature"),
        "humidity": data.get("humidity"),
        "dew_point": data.get("dew_point"),
        "co2": data.get("co2"),
        "source_collection": data.get("collection"),
        "db_name": data.get("db_name"),
        "message_hash": message_hash,
        "raw": data.get("raw", {}),
    }

    try:
        result = collection.insert_one(doc)
        print(f"✅ Saved to MongoDB: {result.inserted_id}")

    except DuplicateKeyError:
        print(f"⚠️ Duplicate ignored: {message_hash}")

    except Exception as e:
        print(f"❌ MongoDB insert failed: {e}")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    ingestor = MQTTIngestor(on_data_callback=handle_data)
    ingestor.start()