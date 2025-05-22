from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, concat_ws, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("SensorJoinStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Skema data sensor suhu
schema_suhu = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("suhu", IntegerType(), True)
])

# Skema data sensor kelembaban
schema_kelembaban = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("kelembaban", IntegerType(), True)
])

# Membaca stream Kafka sensor suhu
df_suhu_raw = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "sensor-suhu-gudang")\
    .option("startingOffsets", "latest")\
    .load()

df_suhu = df_suhu_raw.select(
    from_json(col("value").cast("string"), schema_suhu).alias("data"),
    col("timestamp").alias("ts_suhu")
).select(
    col("data.gudang_id").alias("gudang_id"),
    col("data.suhu").alias("suhu"),
    col("ts_suhu")
).withWatermark("ts_suhu", "30 seconds").alias("suhu")

# Membaca stream Kafka sensor kelembaban
df_kelembaban_raw = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "sensor-kelembaban-gudang")\
    .option("startingOffsets", "latest")\
    .load()

df_kelembaban = df_kelembaban_raw.select(
    from_json(col("value").cast("string"), schema_kelembaban).alias("data"),
    col("timestamp").alias("ts_kelembaban")
).select(
    col("data.gudang_id").alias("gudang_id"),
    col("data.kelembaban").alias("kelembaban"),
    col("ts_kelembaban")
).withWatermark("ts_kelembaban", "30 seconds").alias("kelembaban")

# Join stream berdasarkan gudang_id dan timestamp dalam interval 10 detik
joined_df = df_suhu.join(
    df_kelembaban,
    "gudang_id"
)

# Menambahkan kolom status berdasarkan suhu dan kelembaban
status_expr = """
    CASE
        WHEN suhu.suhu > 80 AND kelembaban.kelembaban > 70 THEN 'Bahaya: suhu & kelembaban tinggi'
        WHEN suhu.suhu > 80 THEN 'Suhu tinggi, kelembaban normal'
        WHEN kelembaban.kelembaban > 70 THEN 'Kelembaban tinggi, suhu normal'
        ELSE 'Aman'
    END
"""

result_df = joined_df.withColumn("status", expr(status_expr))

# Membuat kolom laporan dengan format yang rapi
laporan_df = result_df.withColumn(
    "laporan",
    concat_ws("\n",
        concat_ws("", lit("Gudang "), col("suhu.gudang_id")),
        lit("----------"),
        concat_ws("", lit("Suhu      : "), col("suhu.suhu"), lit("°C")),
        concat_ws("", lit("Kelembaban: "), col("kelembaban.kelembaban"), lit("%")),
        concat_ws("", lit("Status    : "), col("status"))
    )
).select("laporan")

# Output stream ke console dengan interval 2 detik
query = laporan_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
