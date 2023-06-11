#vamos a leer los pelis utilizando pyspark
#importamos los modulos necesarios
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import os

#creamos la sesion de spark usamos el master para 2 cores
spark = SparkSession.builder.master("local[2]").appName("leer_Pelis").getOrCreate()

#leemos el archivo csv
df = spark.read.csv("lista_Pelis.csv", header=True, inferSchema=True)



#guardamos el dataframe en formato csv

df.write.csv("lista_Pelis_unique.csv", header=True, mode="overwrite")


spark.stop()


