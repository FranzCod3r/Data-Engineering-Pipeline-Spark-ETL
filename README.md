# Spark | Dask | Pandas - Data Engineering Pipeline

## Overview

This project demonstrates a practical **Big-Data** workflow using Python with **Dask** and **Apache Spark**. It highlights the **limits of Pandas** on large datasets, and shows how Dask and Spark can be used to handle bigger volumes of data more efficiently.

The repository contains four main scripts:

| **File** | **Purpose** | **Note** |
| --- | --- | --- |
| ``0_dataset_generator.py`` | Testing Dataset Generation | Generates a synthetic dataset for testing. |
| ``1_Benchmark_Pandas_Dask.py`` | Pandas vs. Dask Performance Comparison |  Benchmarking script to evaluate processing speeds. |
| ``2_Spark_ETL_Pipeline.py`` | End-to-End Spark ETL Pipeline | Reads, transforms, and saves data in partitioned Parquet format. |
| ``3_Data_Visualization.py`` | Matplotlib/Seaborn visualizations | Reads processed Spark data and converts it to a Pandas DataFrame for plotting. |

## Goals

- Demonstrate how to generate large synthetic datasets for experimentation.
- Show the performance and scalability differences between pandas and Dask.
- Build a full Spark ETL pipeline for ingesting, processing, and storing data.
- Use Spark as the primary data engine, and then convert results to pandas only when visualization requires it.

## Project Structure

- `0_dataset_generator.py`: creates synthetic transaction data, simulating realistic sales and events for later use with Dask and Spark.
- `1_Benchmark_Pandas_Dask.py`: benchmarks common data operations, showing how pandas can struggle in memory and how Dask scales better for larger datasets.
- `2_Spark_ETL_Pipeline.py`: builds a Spark workflow to read raw data, transform it, and write partitioned output. This script emphasizes Spark’s strength for distributed ETL.
- `3_Data_Visualization.py`: extracts data from Spark, converts it into a pandas DataFrame, and generates visual analysis from the processed dataset.
- `data_local/`: contains sample data files used by the Spark pipeline and for development.

## Why Dask and Spark?

- `pandas` is easy to use, but it is limited by a single-machine, in-memory execution model.
- `Dask` extends pandas-style operations to larger-than-memory datasets and parallel execution.
- `Spark` is designed for distributed processing and excels in production-scale ETL pipelines.

This repository shows the progression from dataset generation to a production-style Spark ETL flow and finally to visualization with Pandas.

## Recommended Workflow

- Use `0_dataset_generator.py` first to create the synthetic data files used by the downstream scripts.
- Run `1_Benchmark_Pandas_Dask.py` to evaluate pandas vs Dask limits.
- Run `2_Spark_ETL_Pipeline.py` to process raw data into partitioned Spark output.
- Run `3_Data_Visualization.py` to load Spark output and produce charts or tables.

## Notes

`BASE_DIR = "./data_local"` is set as default, you can customize it as needed.

If running Spark locally, remember to configure your paths for:

`os.environ["HADOOP_HOME"]` = r"C:\hadoop"
`os.environ["PYSPARK_PYTHON"]` = r"C:\ your Python path"
`os.environ["PYSPARK_DRIVER_PYTHON"]` = r"C:\ your python path"

## Requirements

- Python 3.x
- pandas
- dask
- pyspark
- matplotlib / seaborn (for visualization)

## Conclusion

This Spark project is a compact demonstration of how to move from classical pandas analysis to scalable tools like Dask and Spark. It validates the superiority of distributed processing when managing large datasets while still leveraging pandas for final visual analysis when appropriate.
