#!/usr/bin/env python3

# Copyright 2026 The OKDP Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Continuous producer simulating French customs (douane) activity, for the
real-time demo: container arrivals, parcel scans, fraud detections (weapons,
drugs, counterfeits), suspicious companies. One JSON event per message on a
single Kafka topic, an event_type field tells them apart."""

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "demo-kafka-main.demo.svc.cluster.local:9092"
)
TOPIC = os.environ.get("KAFKA_TOPIC", "douane.evenements")
EVENTS_PER_SEC = float(os.environ.get("EVENTS_PER_SEC", "2"))
# Tuned high on purpose: a demo needs a fraud alert every few seconds, not
# once an hour like a real customs office.
FRAUD_RATE = float(os.environ.get("FRAUD_RATE", "0.10"))

PORTS = ["Le Havre", "Marseille-Fos", "Calais", "Dunkerque", "Roissy CDG", "Marseille"]
PAYS_ORIGINE = ["Chine", "Turquie", "Maroc", "Brésil", "États-Unis", "Inde", "Vietnam"]
MOTIFS_ENTREPRISE = [
    "sous-évaluation répétée",
    "faux certificats d'origine",
    "importateur radié toujours actif",
    "flux incohérent avec l'activité déclarée",
]
# (sous_type, unite, (min, max))
FRAUDE_SOUS_TYPES = [
    ("arme", "unité", (1, 5)),
    ("drogue", "kg", (1, 50)),
    ("contrefacon", "articles", (10, 5000)),
]

running = True


def _stop(signum, frame):
    global running
    running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def _base_event(event_type):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "port": random.choice(PORTS),
    }


def arrivage_container():
    event = _base_event("arrivage_container")
    event.update(
        container_id=f"CONT{random.randint(1000000, 9999999)}",
        pays_origine=random.choice(PAYS_ORIGINE),
        poids_kg=random.randint(2000, 28000),
        nb_colis=random.randint(50, 4000),
    )
    return event


def scan_colis():
    event = _base_event("scan_colis")
    event.update(
        colis_id=f"COL{random.randint(10000000, 99999999)}",
        scanner_id=f"SCAN-{random.randint(1, 12):02d}",
        poids_kg=round(random.uniform(0.1, 40), 2),
        score_risque=random.randint(0, 100),
    )
    return event


def detection_fraude():
    event = _base_event("detection_fraude")
    sous_type, unite, (lo, hi) = random.choice(FRAUDE_SOUS_TYPES)
    event.update(
        sous_type=sous_type,
        colis_id=f"COL{random.randint(10000000, 99999999)}",
        quantite_estimee=random.randint(lo, hi),
        unite=unite,
    )
    return event


def entreprise_suspecte():
    event = _base_event("entreprise_suspecte")
    event.update(
        siren=f"{random.randint(100000000, 999999999)}",
        nom_entreprise=(
            f"Société {random.choice(['Atlas', 'Meridian', 'Horizon', 'Delta', 'Continental'])} "
            f"{random.choice(['Import', 'Trading', 'Logistics', 'Négoce'])}"
        ),
        motif=random.choice(MOTIFS_ENTREPRISE),
    )
    return event


# Relative weights for the non-fraud event types; detection_fraude is drawn
# separately at FRAUD_RATE so its rate stays tunable independently of the mix.
OTHER_GENERATORS = [
    (0.70, scan_colis),
    (0.25, arrivage_container),
    (0.05, entreprise_suspecte),
]


def pick_generator():
    if random.random() < FRAUD_RATE:
        return detection_fraude
    total = sum(weight for weight, _ in OTHER_GENERATORS)
    threshold = random.uniform(0, total)
    upto = 0.0
    for weight, generator in OTHER_GENERATORS:
        upto += weight
        if threshold <= upto:
            return generator
    return scan_colis


def delivery_report(err, _msg):
    if err is not None:
        print(f"delivery failed: {err}", file=sys.stderr)


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    print(
        f"producing to {BOOTSTRAP_SERVERS} topic={TOPIC} "
        f"rate={EVENTS_PER_SEC}/s fraud_rate={FRAUD_RATE}"
    )
    interval = 1.0 / EVENTS_PER_SEC if EVENTS_PER_SEC > 0 else 1.0
    while running:
        event = pick_generator()()
        producer.produce(
            TOPIC,
            key=event["event_type"],
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(interval)
    print("shutting down, flushing pending messages")
    producer.flush(10)


if __name__ == "__main__":
    main()
