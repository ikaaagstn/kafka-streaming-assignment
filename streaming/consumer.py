from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    expr,
    window,
    current_timestamp,
)
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BOOTSTRAP = "localhost:9092"
SOURCE_TOPIC = "transactions"
VALID_TOPIC = "transactions_valid"
DLQ_TOPIC = "transactions_dlq"

admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
for output_topic in (VALID_TOPIC, DLQ_TOPIC):
    print(f"\nCreating topic '{output_topic}'...")
    result = admin.create_topics(
        [NewTopic(output_topic, num_partitions=1, replication_factor=1)]
    )
    try:
        result[output_topic].result(timeout=10)
        print("Topic created successfully")
    except Exception as error:
        error_str = str(error)
        if "TOPIC_ALREADY_EXISTS" in error_str:
            print("Topic already exists")
        else:
            print(f"Error: {error_str}")
            raise

spark = (
    SparkSession.builder.appName("TransactionValidator")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    )
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = (
    StructType()
    .add("user_id", StringType())
    .add("amount", StringType())
    .add("timestamp", StringType())
    .add("source", StringType())
)

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

parsed = parsed.withColumn("event_time", to_timestamp(col("timestamp")))
parsed = parsed.withWatermark("event_time", "3 minutes")
parsed = parsed.dropDuplicates(["user_id", "timestamp"])
parsed = parsed.withColumn("amount_numeric", col("amount").cast(IntegerType()))

is_mandatory_ok = (
    col("user_id").isNotNull()
    & (col("user_id") != "")
    & col("amount").isNotNull()
    & (col("amount") != "")
    & col("timestamp").isNotNull()
    & (col("timestamp") != "")
)
is_type_ok = col("amount_numeric").isNotNull() & col("event_time").isNotNull()
is_range_ok = (
    col("amount_numeric").isNotNull()
    & (col("amount_numeric") >= 1)
    & (col("amount_numeric") <= 10_000_000)
)
is_source_ok = col("source").isin("mobile", "web", "pos")
is_not_late = col("event_time") >= expr("current_timestamp() - INTERVAL 3 MINUTES")

parsed = parsed.withColumn(
    "error_reason",
    when(~is_mandatory_ok, "missing_mandatory_field")
    .when(~is_type_ok, "invalid_type")
    .when(~is_range_ok, "amount_out_of_range")
    .when(~is_source_ok, "invalid_source")
    .when(~is_not_late, "late_event")
    .otherwise(None),
)
parsed = parsed.withColumn("is_valid", col("error_reason").isNull())
parsed = parsed.withColumn(
    "routed_to", when(col("is_valid"), VALID_TOPIC).otherwise(DLQ_TOPIC)
)

valid_events = parsed.filter(col("is_valid"))
invalid_events = parsed.filter(~col("is_valid"))

events_debug_query = (
    parsed.select(
        "user_id",
        "amount",
        "timestamp",
        "source",
        "is_valid",
        "error_reason",
        "routed_to",
    )
    .writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", "./checkpoints/events_debug")
    .start()
)

kafka_columns = "user_id, amount, timestamp, source, is_valid, error_reason"

valid_payload = valid_events.selectExpr(
    "user_id as key", f"to_json(struct({kafka_columns})) as value"
)
invalid_payload = invalid_events.selectExpr(
    "user_id as key", f"to_json(struct({kafka_columns})) as value"
)

valid_query = (
    valid_payload.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", VALID_TOPIC)
    .option("checkpointLocation", "./checkpoints/valid")
    .outputMode("append")
    .start()
)

dlq_query = (
    invalid_payload.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", DLQ_TOPIC)
    .option("checkpointLocation", "./checkpoints/dlq")
    .outputMode("append")
    .start()
)

windowed_counts = (
    parsed.groupBy(window(col("event_time"), "1 minute"))
    .count()
    .selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "count as total_transaksi",
    )
)

monitoring_query = (
    windowed_counts.writeStream.format("console")
    .outputMode("update")
    .option("truncate", False)
    .option("checkpointLocation", "./checkpoints/monitoring")
    .start()
)

running_total = (
    valid_events.groupBy()
    .count()
    .withColumnRenamed("count", "running_total")
    .withColumn("timestamp", current_timestamp())
)

running_total_query = (
    running_total.writeStream.format("console")
    .outputMode("complete")
    .option("truncate", False)
    .option("checkpointLocation", "./checkpoints/running_total")
    .start()
)

spark.streams.awaitAnyTermination()
