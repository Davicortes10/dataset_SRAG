from sqlalchemy import create_engine
import pandas as pd
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


class PreprocessDataset:
    def __init__(self,file_path):
        self.file_path = file_path
        self.db_connection = "jdbc:mysql://34.170.252.6:3306/srag_datalake"

        self.spark = SparkSession.builder \
            .appName("Atualizar Data Lake") \
            .config("spark.jars", "/path/to/mysql-connector-java-8.0.13.jar") \
            .getOrCreate()
    
    def atualizar_Data_Lake(self):
        print("Iniciando o processo de atualização do Data Lake com PySpark...")
        
        # Leitura do arquivo CSV
        try:
            print(f"Lendo o arquivo CSV em: {self.file_path}")
            df = self.spark.read.option("delimiter", ";") \
                .option("header", "true") \
                .csv(self.file_path)
            print(f"Arquivo CSV lido com sucesso! Linhas: {df.count()}, Colunas: {len(df.columns)}")
            print(f"Colunas detectadas: {df.columns}")
            self.conectar_DB(df)
        except Exception as e:
            print(f"Erro ao ler o arquivo CSV: {e}")
            return

    def conectar_DB(self, df):
        # Upload para o banco de dados
        try:
            print(f"Iniciando o upload para a tabela 'srag_datalake' no banco de dados...")
            df.write \
                .format("jdbc") \
                .option("url", self.db_connection) \
                .option("dbtable", "srag_datalake") \
                .option("user", "devdavi") \
                .option("password", "12345678") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("overwrite") \
                .save()
            print("Upload concluído com sucesso!")
        except Exception as e:
            print(f"Erro ao fazer o upload para o banco de dados: {e}")
            return

        print("Processo de atualização do Data Lake finalizado com sucesso!")

