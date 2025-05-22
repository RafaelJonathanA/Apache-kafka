from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("KelembabanFilter") \
    .getOrCreate()

# Schema data kelembaban dari Kafka (JSON di value)
schema_kelembaban = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

# Baca stream Kafka topic sensor-kelembaban-gudang
df_kelembaban = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .option("startingOffsets", "latest") \
    .load()

# Parsing JSON dan pilih kolom penting
df_kelembaban_parsed = df_kelembaban.select(
    from_json(col("value").cast("string"), schema_kelembaban).alias("data")
).select("data.*")

# Filter kelembaban > 70
df_kelembaban_warning = df_kelembaban_parsed.filter(col("kelembaban") > 70)

# Format output peringatan
df_output = df_kelembaban_warning.selectExpr(
    "concat('[Peringatan Kelembaban Tinggi] Gudang ', gudang_id, ': Kelembaban ', kelembaban, '%') as warning"
)

# Tulis ke console streaming
query = df_output.writeStream.format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
