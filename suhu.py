from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("SuhuFilter") \
    .getOrCreate()

# Schema data suhu dari Kafka (JSON di value)
schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

# Baca stream Kafka topic sensor-suhu-gudang
df_suhu = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

# Parsing JSON dan pilih kolom penting
df_suhu_parsed = df_suhu.select(
    from_json(col("value").cast("string"), schema_suhu).alias("data")
).select("data.*")

# Filter suhu > 80
df_suhu_warning = df_suhu_parsed.filter(col("suhu") > 80)

# Format output peringatan
df_output = df_suhu_warning.selectExpr(
    "concat('[Peringatan Suhu Tinggi] Gudang ', gudang_id, ': Suhu ', suhu, '°C') as warning"
)

# Tulis ke console streaming
query = df_output.writeStream.format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
