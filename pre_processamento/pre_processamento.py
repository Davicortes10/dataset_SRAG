from sqlalchemy import create_engine
import pandas as pd
import pymysql
from pyspark.sql import SparkSession

class PreprocessDataset:
    def __init__(self,file_path):
        self.file_path = file_path
    
    def spark(self):
        # Criar uma sessão do Spark
        spark = SparkSession.builder.appName("srag").getOrCreate()

        print(spark)
    def ler_arquivo(self):
        try:
            arquivo_csv = "/content/INFLUD20-01-05-2023.csv"

            # Ler o arquivo CSV com separador ";"
            print(f"[INFO] Tentando carregar o arquivo: {arquivo_csv}")
            df = spark.read.format("csv").option("header", "true").option("sep", ";").option("inferSchema", "true").load(arquivo_csv)

            print("[INFO] Arquivo CSV carregado com sucesso.")

            # Exibir as 5 primeiras linhas
            df.show(5)

            # Exibir o esquema do DataFrame
            df.printSchema()
        except Exception as e:
            print(f"[ERROR] Erro ao processar o arquivo CSV: {e}")

        finally:
            # Encerrar a sessão do Spark
            try:
                spark.stop()
                print("[INFO] SparkSession encerrada.")
            except Exception as e:
                print(f"[ERROR] Erro ao encerrar SparkSession: {e}")
