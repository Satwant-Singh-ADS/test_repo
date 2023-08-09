from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd

for col in numeric_columns:
    plt.figure(figsize=(8, 6))
    plt.hist(Df.select(col).dropna().toPandas()[col], bins=20, color='blue', alpha=0.7)
    plt.xlabel(col)
    plt.ylabel('Frequency')
    plt.title(f'Univariate Histogram of {col}')
    plt.grid(True)
    plt.show()

percentiles = [0.01, 0.02, 0.03, ..., 0.99]  # Specify the percentiles you want to compute

percentiles_data = []
for col_name in numeric_columns:
    percentiles_values = Df.approxQuantile(col_name, percentiles, 0.0)
    percentiles_data.append([col_name] + percentiles_values)

percentile_columns = ["Column"] + [f"P{int(p * 100)}" for p in percentiles]
percentiles_df = spark.createDataFrame(percentiles_data, percentile_columns)
