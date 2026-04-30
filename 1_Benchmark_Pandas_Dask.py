
import os
import gc
import glob
import numpy as np
import pandas as pd
import dask.dataframe as dd
import time

BASE_DIR = "./data_local"
PARQUET_DIR = os.path.join(BASE_DIR, "parquet")
JSON_DIR = os.path.join(BASE_DIR, "json")

#==========================
# CONFRONTO PANDAS VS DASK
#==========================

# PANDAS JSON PROCESSING
# JSONl File list
json_data = glob.glob(os.path.join(JSON_DIR, "*.jsonl"))

def pandas_somma(file_list):
    # Counter 'amount'
    total = 0

    for f in file_list:
        # dataframe temporaneo
        df_temp = pd.read_json(f, lines=True)
        # operazioni somma
        sum_file = df_temp["amount"].sum()
        total += sum_file

        del df_temp

    print(f"--Pandas--\nTotale Complessivo: {total:.2f}")

#==========================
# DASK Processing
#==========================

# Legge JSONl sfruttando wildcars
df = dd.read_json(
    os.path.join(JSON_DIR, "*.jsonl"), # wildacards
    lines=True
)

def dask_processing(df):
    # operazioni su groupby (lazy)
    temp_df = df.groupby("customer_id")["amount"].sum()
    result = temp_df.sum()
    # Esegui il calcolo
    output = result.compute()

    print(f"\n--DASK--\nTotale Complessivo: {output:.2f}")

#==========================
# Esecuzione DEMO
#==========================

if __name__ == "__main__":
    from dask.distributed import Client

    # 1. Avvia il motore Dask
    # Client per ottimizzare l'uso dei core
    with Client() as client:
        print(f"Dask Dashboard disponibile su: {client.dashboard_link}")

        # --- BENCHMARK PANDAS ---
        t0 = time.perf_counter()
        pandas_somma(json_data)
        print(f"Tempo processing Pandas: {time.perf_counter() - t0:.2f} sec.")

        # Pulizia RAM tra i due test
        gc.collect()

        # --- BENCHMARK DASK ---
        # DEFINISCI IL DF QUI DENTRO per assicurarti che usi il client attivo
        df_dask = dd.read_json(os.path.join(JSON_DIR, "*.jsonl"), lines=True)
        
        t1 = time.perf_counter()
        dask_processing(df_dask)
        print(f"Tempo processing Dask: {time.perf_counter() - t1:.2f} sec.")

    # Client Dask chiuso automaticamente grazie a 'with'.
    gc.collect()
    print("\nBenchmark concluso e risorse liberate.")
