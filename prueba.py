import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

df = pd.read_csv('air_traffic_data.csv')
n = df.shape[0]
columnas = df.columns
print(f'Nuestros tipos de datos Son los siguientes:\n\n{df.dtypes}\n')

df_china = df[df[columnas[1]] == 'China Airlines']
df_berlin = df[df[columnas[1]] == 'Air Berlin']
df_berlin_g = df_berlin[df[columnas[10]] == 'G']

print(df_berlin_g)

spark = SparkSession.builder.getOrCreate()
dfs = spark.createDataFrame(df)

print(dfs.head())
