#!/usr/bin/env python3
"""
T24 files -> Avro -> Kafka producer.

Key behavior:
- No CLI arguments.
- One physical source line = one Kafka change event.
- A source file may contain many rows.
- All envelope fields are nullable EXCEPT XMLRECORD.
- XMLRECORD is transported as-is; it is never parsed.
- Routes source folder -> Kafka topic from topic_definitions.json.
- Optimized for many source files:
    * one Spark file stream for all topics
    * recursive file discovery
    * bounded maxFilesPerTrigger
    * dynamic Kafka "topic" column
    * one Kafka write per micro-batch, not one write per topic
    * AvailableNow trigger drains all currently available files and exits
"""

import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.types import BinaryType


# ==============================================================================
# Runtime configuration
# ==============================================================================

BASE_DIR = Path(__file__).resolve().parent

INPUT_ROOT = os.environ.get("T24_INPUT_ROOT", "/data/T24")
TOPIC_DEFINITIONS = BASE_DIR / "topic_definitions.json"
SCHEMA_FILE = BASE_DIR / "t24-envelope.avsc"
SCHEMA_ID_FILE = BASE_DIR / "schema_id.txt"

CHECKPOINT_DIR = os.environ.get(
    "T24_CHECKPOINT_DIR",
    "/home/rise37432/files-to-kafka/gg-applier-spark/all-topics"
)

KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    (
        "cdp-worker-04-uat-app-387081.hodomain.local:9092,"
        "cdp-worker-05-uat-app-387084.hodomain.local:9092"
    ),
)

KAFKA_SECURITY_PROTOCOL = os.environ.get(
    "KAFKA_SECURITY_PROTOCOL",
    "SASL_PLAINTEXT",
)
KAFKA_SASL_MECHANISM = os.environ.get(
    "KAFKA_SASL_MECHANISM",
    "GSSAPI",
)
KAFKA_SASL_SERVICE_NAME = os.environ.get(
    "KAFKA_SASL_SERVICE_NAME",
    "kafka",
)

# File-ingestion tuning. Increase/decrease based on utility node capacity.
MAX_FILES_PER_TRIGGER = int(os.environ.get("MAX_FILES_PER_TRIGGER", "500"))
MAX_BYTES_PER_TRIGGER = os.environ.get("MAX_BYTES_PER_TRIGGER", "1g")

# Number of Spark partitions used immediately before Kafka write.
KAFKA_OUTPUT_PARTITIONS = int(
    os.environ.get("KAFKA_OUTPUT_PARTITIONS", "48")
)


# ==============================================================================
# Helpers
# ==============================================================================

def load_topic_routes():
    with TOPIC_DEFINITIONS.open("r", encoding="utf-8") as f:
        doc = json.load(f)

    topics = doc.get("topics")
    if not isinstance(topics, dict) or not topics:
        raise RuntimeError(
            f"{TOPIC_DEFINITIONS} must contain a non-empty 'topics' object"
        )

    routes = []

    for topic, cfg in topics.items():
        cfg = cfg or {}
        default_name = topic.split(".", 1)[1] if "." in topic else topic

        folder = (
            cfg.get("source_folder")
            or cfg.get("sourceFolder")
            or cfg.get("folder")
            or cfg.get("source")
            or default_name
        )

        table = (
            cfg.get("table")
            or cfg.get("table_name")
            or default_name
        )

        routes.append(
            {
                "folder": str(folder),
                "table": str(table),
                "topic": str(topic),
            }
        )

    return routes


def load_schema():
    with SCHEMA_FILE.open("r", encoding="utf-8") as f:
        schema = json.load(f)

    fields = {x["name"]: x for x in schema.get("fields", [])}

    required_names = {
        "table",
        "op_ts",
        "optype",
        "csn",
        "xid",
        "opseqno",
        "pos",
        "RECID",
        "XMLRECORD",
    }

    missing = required_names - fields.keys()
    if missing:
        raise RuntimeError(
            f"Avro schema missing fields: {sorted(missing)}"
        )

    # XMLRECORD must be required/non-null.
    xml_type = fields["XMLRECORD"]["type"]
    if isinstance(xml_type, list) and "null" in xml_type:
        raise RuntimeError(
            "t24-envelope.avsc still allows XMLRECORD=null. "
            "Update XMLRECORD type to 'string'."
        )

    # Every other envelope field is allowed to be null.
    for name in required_names - {"XMLRECORD"}:
        avro_type = fields[name]["type"]
        if not (
            isinstance(avro_type, list)
            and "null" in avro_type
            and "string" in avro_type
        ):
            raise RuntimeError(
                f"{name} must be nullable in t24-envelope.avsc "
                f"(expected ['null','string'])."
            )

    return json.dumps(schema, separators=(",", ":"))


def load_schema_id():
    env_value = os.environ.get("SCHEMA_ID")

    if env_value:
        raw = env_value.strip()
    elif SCHEMA_ID_FILE.is_file():
        raw = SCHEMA_ID_FILE.read_text(encoding="utf-8").strip()
    else:
        raise RuntimeError(
            "Schema ID not found. Set SCHEMA_ID or create schema_id.txt."
        )

    try:
        schema_id = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid schema ID: {raw!r}"
        ) from exc

    if schema_id < 0 or schema_id > 0xFFFFFFFF:
        raise RuntimeError(
            f"Schema ID out of 4-byte range: {schema_id}"
        )

    return schema_id


def nullable_capture(col):
    """
    Normalize optional capture fields:
      empty string / whitespace-only -> NULL
      surrounding capture quotes -> removed
      doubled quotes -> single quote representation

    Spark 3.3 substring() requires a literal integer length, so use
    anchored regexp_replace() instead of a Column expression for length.
    """
    trimmed = F.trim(col)

    unquoted = F.regexp_replace(
        trimmed,
        r'^"(.*)"$',
        r'$1',
    )
    unquoted = F.regexp_replace(unquoted, '""', '"')

    return F.when(
        unquoted.isNull() | (unquoted == ""),
        F.lit(None).cast("string"),
    ).otherwise(unquoted)


def required_xmlrecord(col):
    """
    Remove capture-file outer quoting only.
    XML content is otherwise opaque and untouched.

    Spark 3.3 substring() does not accept a Column for the length argument,
    so outer quotes are removed with an anchored regular expression.
    """
    trimmed = F.trim(col)

    unquoted = F.regexp_replace(
        trimmed,
        r'^"(.*)"$',
        r'$1',
    )

    return F.regexp_replace(unquoted, '""', '"')


def route_column(source_path_col, routes, value_key):
    """
    Build a Spark expression mapping /<SOURCE_FOLDER>/ in the source path
    to either table or topic.
    """
    expr = None

    for route in routes:
        condition = source_path_col.contains(f"/{route['folder']}/")
        value = F.lit(route[value_key])

        expr = (
            F.when(condition, value)
            if expr is None
            else expr.when(condition, value)
        )

    return expr.otherwise(F.lit(None).cast("string"))


# ==============================================================================
# Main
# ==============================================================================

ROUTES = load_topic_routes()
AVRO_SCHEMA_JSON = load_schema()
SCHEMA_ID = load_schema_id()

header_bytes = bytes([0]) + SCHEMA_ID.to_bytes(4, byteorder="big")

spark = (
    SparkSession.builder
    .appName("t24-files-to-kafka-all-topics")
    # Many-small-files tuning.
    .config(
        "spark.sql.files.maxPartitionBytes",
        os.environ.get(
            "SPARK_MAX_PARTITION_BYTES",
            str(128 * 1024 * 1024),
        ),
    )
    .config(
        "spark.sql.files.openCostInBytes",
        os.environ.get(
            "SPARK_FILE_OPEN_COST_BYTES",
            str(16 * 1024 * 1024),
        ),
    )
    .config(
        "spark.sql.shuffle.partitions",
        os.environ.get("SPARK_SHUFFLE_PARTITIONS", "200"),
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    os.environ.get("SPARK_LOG_LEVEL", "WARN")
)

# One stream scans all batches/source folders. Every physical line is one row.
reader = (
    spark.readStream
    .format("text")
    .option("encoding", "UTF-8")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.txt")
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
)

# Available on modern Spark file sources; harmless to omit by setting empty.
if MAX_BYTES_PER_TRIGGER:
    reader = reader.option(
        "maxBytesPerTrigger",
        MAX_BYTES_PER_TRIGGER,
    )

source = (
    reader
    .load(f"file://{INPUT_ROOT}")
    .withColumn("source_file", F.input_file_name())
)

# Split on at most 9 delimiters => max 10 physical fields.
# Semicolons inside XMLRECORD therefore stay inside XMLRECORD.
parts = F.split(F.col("value"), ";", 10)

parsed = (
    source
    .withColumn("physical_field_count", F.size(parts))
    .withColumn("table", route_column(F.col("source_file"), ROUTES, "table"))
    .withColumn("topic", route_column(F.col("source_file"), ROUTES, "topic"))
    .withColumn("op_ts", nullable_capture(parts.getItem(0)))
    .withColumn("optype", nullable_capture(parts.getItem(1)))
    .withColumn("csn", nullable_capture(parts.getItem(2)))
    # field 3 = capture_ts; intentionally not sent to Kafka
    .withColumn("xid", nullable_capture(parts.getItem(4)))
    .withColumn("opseqno", nullable_capture(parts.getItem(5)))
    .withColumn("pos", nullable_capture(parts.getItem(6)))
    .withColumn("RECID", nullable_capture(parts.getItem(7)))
    # field 8 = reserved; intentionally not sent to Kafka
    .withColumn("XMLRECORD", required_xmlrecord(parts.getItem(9)))
)

# Validator runs before this producer and rejects corrupt source files.
# These guards make the producer fail-safe if invalid data reaches Spark anyway.
valid = (
    parsed
    .filter(F.col("physical_field_count") == 10)
    .filter(F.col("topic").isNotNull())
    .filter(F.col("XMLRECORD").isNotNull())
    .filter(F.length(F.col("XMLRECORD")) > 0)
)

envelope = F.struct(
    F.col("table"),
    F.col("op_ts"),
    F.col("optype"),
    F.col("csn"),
    F.col("xid"),
    F.col("opseqno"),
    F.col("pos"),
    F.col("RECID"),
    F.col("XMLRECORD"),
)

kafka_rows = (
    valid
    .withColumn(
        "avro_payload",
        to_avro(envelope, AVRO_SCHEMA_JSON),
    )
    .withColumn(
        "value",
        F.concat(
            F.lit(bytearray(header_bytes)).cast(BinaryType()),
            F.col("avro_payload"),
        ),
    )
    # RECID is optional now, so Kafka key may also be NULL.
    .withColumn(
        "key",
        F.col("RECID").cast("string"),
    )
    .select("topic", "key", "value")
)


def publish_batch(batch_df, batch_id):
    """
    Publish all topics in one Kafka sink write.

    Dynamic `topic` column avoids issuing one Spark/Kafka write per topic.
    This is materially faster when thousands of source files are processed.
    """
    if batch_df.rdd.isEmpty():
        return

    outgoing = batch_df.repartition(
        KAFKA_OUTPUT_PARTITIONS,
        "topic",
    )

    (
        outgoing.write
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP,
        )
        .option(
            "kafka.security.protocol",
            KAFKA_SECURITY_PROTOCOL,
        )
        .option(
            "kafka.sasl.mechanism",
            KAFKA_SASL_MECHANISM,
        )
        .option(
            "kafka.sasl.kerberos.service.name",
            KAFKA_SASL_SERVICE_NAME,
        )
        .option("kafka.acks", "all")
        .option(
            "kafka.compression.type",
            os.environ.get(
                "KAFKA_COMPRESSION_TYPE",
                "lz4",
            ),
        )
        .option(
            "kafka.linger.ms",
            os.environ.get("KAFKA_LINGER_MS", "50"),
        )
        .option(
            "kafka.batch.size",
            os.environ.get(
                "KAFKA_BATCH_SIZE",
                "131072",
            ),
        )
        .option(
            "kafka.max.in.flight.requests.per.connection",
            os.environ.get(
                "KAFKA_MAX_IN_FLIGHT",
                "5",
            ),
        )
        .save()
    )

    print(
        f"[batch={batch_id}] published successfully",
        flush=True,
    )


query = (
    kafka_rows.writeStream
    .foreachBatch(publish_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
