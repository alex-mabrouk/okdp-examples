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

"""Spark Structured Streaming job for the real-time customs (douane) demo.

Reads the single `douane.evenements` Kafka topic (see docker/douane_producer.py)
and routes each JSON event, by its `event_type` key, into its own gold Iceberg
table. One streaming query per event type, all run concurrently in the same
SparkSession so the driver stays up as long as any of them does.

Not run through spark_submit.submit_and_wait: that helper polls for a
COMPLETED state a streaming job never reaches. Submitted directly as a
long-running SparkApplication instead (restartPolicy Always, no
timeToLiveSeconds) -- see clusters/sandbox/project-demo/75-douane-streaming.yaml
in okdp-sandbox.

Usage: douane_streaming_job.py <kafka_bootstrap_servers> <kafka_topic> <catalog>
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

COMMON_FIELDS = [
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("port", StringType()),
]

# event_type -> (gold table name, extra columns beyond COMMON_FIELDS)
EVENT_TABLES = {
    "arrivage_container": (
        "containers",
        [
            StructField("container_id", StringType()),
            StructField("pays_origine", StringType()),
            StructField("poids_kg", LongType()),
            StructField("nb_colis", LongType()),
        ],
    ),
    "scan_colis": (
        "scans",
        [
            StructField("colis_id", StringType()),
            StructField("scanner_id", StringType()),
            StructField("poids_kg", DoubleType()),
            StructField("score_risque", LongType()),
        ],
    ),
    "detection_fraude": (
        "alertes",
        [
            StructField("sous_type", StringType()),
            StructField("colis_id", StringType()),
            StructField("quantite_estimee", LongType()),
            StructField("unite", StringType()),
        ],
    ),
    "entreprise_suspecte": (
        "entreprises_suspectes",
        [
            StructField("siren", StringType()),
            StructField("nom_entreprise", StringType()),
            StructField("motif", StringType()),
        ],
    ),
}


def main():
    bootstrap_servers, topic, catalog = sys.argv[1:4]

    # The credential is not in the SparkApplication's sparkConf (it would land
    # in a manifest checked into git): assembled here from the mounted Secret,
    # same convention as the batch gold jobs (e.g. einvoicing_gold_job.py).
    client_id = os.getenv("POLARIS_CLIENT_ID", "")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET are not set")

    spark = (
        SparkSession.builder.appName("douane-streaming")
        .config(f"spark.sql.catalog.{catalog}.credential", f"{client_id}:{client_secret}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Iceberg REST catalogs require the namespace to exist before a table is
    # created in it; toTable() below creates the tables but not the namespace.
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.douane")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(key AS STRING) AS event_type_key", "CAST(value AS STRING) AS json")
    )

    for event_type, (table, extra_fields) in EVENT_TABLES.items():
        schema = StructType(COMMON_FIELDS + extra_fields)
        parsed = (
            raw.filter(col("event_type_key") == event_type)
            .select(from_json(col("json"), schema).alias("data"))
            .select("data.*")
            .withColumn("timestamp", col("timestamp").cast(TimestampType()))
        )
        (
            parsed.writeStream.format("iceberg")
            .outputMode("append")
            .trigger(processingTime="5 seconds")
            .option("checkpointLocation", f"s3a://gold/_checkpoints/douane/{table}")
            .toTable(f"{catalog}.douane.{table}")
        )

    # One query per event type; the driver stays up as long as any is running.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
