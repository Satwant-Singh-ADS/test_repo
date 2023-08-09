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
