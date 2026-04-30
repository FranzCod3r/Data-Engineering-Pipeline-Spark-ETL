import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# Inizializza SparkSession
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"] = r"C:\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Python311\python.exe"

spark = SparkSession.builder \
    .appName("ETL_Processed_Sales") \
    .config("spark.sql.shuffle.partitions", "32") \
    .getOrCreate()

# Paths
BASE_DIR = "./data_local"
PARQUET_DIR = f"{BASE_DIR}/parquet"
OUTPUT_DIR = f"{BASE_DIR}/processed_sales"

# Extract: leggi i parquet
transactions = spark.read.parquet(f"{PARQUET_DIR}/transactions_batch_*.parquet")
products = spark.read.parquet(f"{PARQUET_DIR}/products.parquet")
regions = spark.read.parquet(f"{PARQUET_DIR}/regions.parquet")

# Check dei timestamp corrotti:
# Creiamo una colonna temporanea per verificare se il dato originale era leggibile
transactions = transactions.withColumn(
    "ts_parsed", F.to_timestamp("ts")
)
# Flag corrupted timestamps:
transactions = transactions.withColumn(
    "is_ts_corrupt",
    F.col("ts").isNotNull() & F.col("ts_parsed").isNull()
)

# Creaiamo un log se i timestamp sono corrotti:
corrupt_count = transactions.filter("is_ts_corrupt").count()
if corrupt_count > 0:
    transactions.filter("is_ts_corrupt").write.mode("append").saveAsTable("rejected_records")
    print(f"Attenzione: trovati {corrupt_count} timestamp invalidi.")

transactions = transactions.drop("ts_parsed")
transactions = transactions.drop("is_ts_corrupt")

# Vogliamo fare Join con products per ottenere 'category'
# Se products o regions sono piccoli, usa Broadcast per evitare Shuffle:
products_count = products.count()
regions_count = regions.count()

if products_count < 100000:
    products = F.broadcast(products)

if regions_count < 100000:
    regions = F.broadcast(regions)

# Arrotondiamo le colonne 'amount' e 'price' a 2 decimali: 
transactions = transactions.withColumn("amount",F.col("amount").cast(DecimalType(10, 2)))
products = products.withColumn("price", F.col("price").cast(DecimalType(10,2)))

# Join transazioni e prodotti:
tx_prod = transactions.join(products, on="product_id", how="left")
final_df = tx_prod.join(regions, on="region_id", how="left")

# Check valori nulli & join mismatch:
print("--- CHECKING NULLS ---\n")
null_count = final_df.filter(F.col("price").isNull()).count()
if null_count > 0:
    print("NULL in price:", null_count)
else:
    print("OK: nessun NULL in price")

# mostra eventuali product_id con valori nulli
final_df.filter(F.col("price").isNull()).select("product_id").distinct().show()

# Load: scrivi in parquet partizionato per 'year'
final_df.repartition("year") \
    .write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(OUTPUT_DIR)

spark.stop()