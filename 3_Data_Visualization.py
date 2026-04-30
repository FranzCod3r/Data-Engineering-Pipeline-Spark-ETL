
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import numpy as np

# Inizializza SparkSession
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"] = r"C:\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Python311\python.exe"

spark = SparkSession.builder \
    .appName("reporting_data") \
    .config("spark.sql.shuffle.partitions", "32") \
    .getOrCreate()

# Paths
BASE_DIR = "./data_local"
PROCESSED_DIR = f"{BASE_DIR}/processed_sales" # dataset elaborated previously
OUTPUT_DIR = f"{BASE_DIR}"

# Read processed data
df = spark.read.parquet(PROCESSED_DIR)

# Aggregazione Spark - fatturato per categoria
spark_revenues = (
    df.groupBy("category")
      .agg(F.sum("amount").alias("total_revenue"))
      .orderBy("total_revenue", ascending=False)
)

# Aggregazione Year_Month:
df_monthly = (
    df.withColumn("year_month", F.date_format("ts", "yyyy-MM"))
      .groupBy("year_month", "category")
      .agg(F.sum("amount").alias("total_revenue"))
      .orderBy("year_month")
)

# Conversione in Pandas
df_revenues = spark_revenues.toPandas()
timed_df = df_monthly.toPandas()


# Funzione per formattazione big numbers
def format_big_numbers(x, pos):
    """formatta i tickers dei grafici per big numbers, 
    restituisce Billions, Millions, K"""
    if x >= 1e9:
        return f'{x/1e9:.1f}B'
    if x >= 1e6:
        return f'{x/1e6:.1f}M'
    if x >= 1e3:
        return f'{x/1e3:.1f}K'
    return f'{int(x)}'

# ==============
# LINEPLOT CHART
# ==============
plt.figure(figsize=(14, 7))

ax = sns.lineplot(
    data=timed_df,
    x="year_month",
    y="total_revenue",
    hue="category",
    marker="o",
    palette="YlGnBu_r"
)

# Asse Y con formatter K/M/B
ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_big_numbers))
# grid
ax.grid(axis='y', linestyle='--', alpha=0.4)

plt.title("Andamento Mensile del Fatturato per Categoria")
plt.xlabel("Mese")
plt.ylabel("Fatturato (€)")
plt.xticks(rotation=45)
plt.tight_layout()
# Print
plt.savefig("revenue_category_Lineplot.png", dpi=300)
plt.show()


# =============
# BARPLOT CHART
# =============

plt.figure(figsize=(10, 6))
ax = sns.barplot(data=df_revenues, 
                 x="category", 
                 y="total_revenue", 
                 palette="Greens_r"
)

# Asse Y con formatter K/M/B
ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_big_numbers))

plt.title("Fatturato Totale per Categoria")
plt.xlabel("Categoria")
plt.ylabel("Fatturato (€)")
plt.xticks(rotation=45)
plt.tight_layout()
# Print
plt.savefig("revenue_category_bar.png", dpi=300)
plt.show()

# ============
# DONUT CHART
# ============

# Ordine decrescente per migliorare la visualizzazione degli slices
df_revenues = df_revenues.sort_values(by="total_revenue", ascending=False)
# Calcolo percentuali
df_revenues["perc"] = df_revenues["total_revenue"] / df_revenues["total_revenue"].sum() * 100

plt.figure(figsize=(8, 8))
colors = sns.color_palette("Blues", len(df_revenues))

# Donut
plt.pie(
    df_revenues["perc"],
    labels=df_revenues["category"],
    autopct='%1.1f%%',
    startangle=140,
    colors=colors,
    textprops={'fontweight':600},
    wedgeprops={"width": 0.35, # << crea il donut
                "edgecolor": "white"},
                #"linewidth": 5},   
    pctdistance=0.8
)

plt.title("Percentuale Fatturato per Categoria", fontweight='semibold')
plt.tight_layout()
# Print
plt.savefig("donut_revenues.png", dpi=300)
plt.show()