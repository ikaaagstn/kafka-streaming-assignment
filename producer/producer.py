from datetime import datetime, timezone, timedelta
from faker import Faker
import random
import logging
import json
import os
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER = os.getenv("BROKER", "localhost:9092")
TOPIC = "transactions"
INTERVAL_SECONDS = random.uniform(1, 2)

admin = AdminClient({"bootstrap.servers": BROKER})
print(f"\nCreating topic '{TOPIC}'...")
result = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
try:
    result[TOPIC].result(timeout=10)
    print("Topic created successfully")
except Exception as error:
    error_str = str(error)
    if "TOPIC_ALREADY_EXISTS" in error_str:
        print("Topic already exists")
    else:
        print(f"Error: {error_str}")
        raise

producer = Producer({"bootstrap.servers": BROKER})


def on_delivery(err, msg):
    if err is not None:
        print(f"Gagal kirim ke Kafka: {err}")
    else:
        print(
            f"Sukses kirim -> topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


fake = Faker()
INVALID_EVENT_RATE = 0.1
INVALID_SOURCES = ["unknown", "zzzz", "null"]
INVALID_TIMESTAMPS = [
    "2026-13-40T99:99:99Z",  # bulan/tanggal/jam di luar range
    "2026-08-19",  # tanggal doang tanpa jam (format beda dari yang biasa dikirim)
    "19/08/2026 10:30:00",  # format lokal, bukan ISO 8601
    "2099-01-01T00:00:00Z",  # valid secara format tapi timestamp masa depan jauh
    "1900-01-01T00:00:00Z",  # valid secara format tapi terlalu lampau
]

sequence = 0
late_events_sent = 0
while True:
    sequence += 1
    event_time = datetime.now(timezone.utc)

    # buat menentukan is late (timestamp nya dikurangin dr aslinya)
    is_late = False
    if (
        random.random() < 0.1 and late_events_sent <= 5
    ):  # late_events_sent masih di bawah 5 dan dapat chance utk kirim event late
        is_late = True
        event_time -= timedelta(
            seconds=random.uniform(180, 300)
        )  # event time - random detik
        late_events_sent += 1

    event = {
        "user_id": "U" + str(fake.pyint(min_value=12345, max_value=99999)),
        "amount": fake.pyint(min_value=1, max_value=5_000_000, step=1000),
        "timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": fake.random_element(["mobile", "web", "pos"]),
        # "is_invalid": False,
        # "is_late": is_late,
    }

    if random.random() < INVALID_EVENT_RATE:
        # event["is_invalid"] = True
        possible_corruptions = ["amount", "timestamp", "source"]
        fields_to_corrupt = random.sample(
            possible_corruptions, k=random.randint(1, len(possible_corruptions))
        )

        if "amount" in fields_to_corrupt:
            event["amount"] = random.choice(
                [
                    fake.pyint(min_value=-5_000_000, max_value=-1000),  # negatif amount
                    fake.pyint(
                        min_value=50_000_000, max_value=999_999_999
                    ),  # melebihi max value
                ]
            )
        if "timestamp" in fields_to_corrupt:
            event["timestamp"] = random.choice(INVALID_TIMESTAMPS)
        if "source" in fields_to_corrupt:
            event["source"] = random.choice(INVALID_SOURCES)

    print(event)

    producer.produce(
        TOPIC, key=event["user_id"], value=json.dumps(event), callback=on_delivery
    )
    producer.flush()
    logging.info(f"Produced event: {event}")
    time.sleep(INTERVAL_SECONDS)
