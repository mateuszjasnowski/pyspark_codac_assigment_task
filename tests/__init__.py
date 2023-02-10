""" Inizializing tests """
from pyspark.sql import SparkSession

#Declaring SparkSession for Chispa testing
SPARK = SparkSession.builder.master("local").appName("ChispaTest").getOrCreate()
