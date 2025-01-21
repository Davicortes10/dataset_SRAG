from sqlalchemy import create_engine
import pandas as pd
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


class PreprocessDataset:
    def __init__(self,file_path):
        self.file_path = file_path
        self.spark = SparkSession.builder.appName("srag").getOrCreate()
        self.db_connection = "jdbc:mysql://34.170.252.6:3306/srag_datalake"
    
    def atualizar_Data_Lake(self):
        print("Iniciando o processo de atualização do Data Lake com PySpark...")

        # Iniciar a sessão Spark
        try:
            spark = SparkSession.builder.appName("Atualizar Data Lake").getOrCreate()
            print("Sessão Spark iniciada com sucesso!")
        except Exception as e:
            print(f"Erro ao iniciar a sessão Spark: {e}")
            return

        # Conectar ao banco de dados
        print(f"Conectando ao banco de dados: {self.db_connection}")
        try:
            conn = create_engine(self.db_connection)
            print("Conexão com o banco de dados criada com sucesso!")
        except Exception as e:
            print(f"Erro ao criar a conexão com o banco: {e}")
            return

        # Ler o arquivo CSV com PySpark
        try:
            print(f"Lendo o arquivo CSV em: {self.file_path}")
            df = spark.read.option("delimiter", ";").option("header", True).csv(self.file_path)
            print(f"Arquivo CSV lido com sucesso! Número de linhas: {df.count()}, Número de colunas: {len(df.columns)}")
            print(f"Colunas detectadas: {df.columns}")
        except Exception as e:
            print(f"Erro ao ler o arquivo CSV: {e}")
            return

        # Escrever no banco de dados
        try:
            print("Iniciando o upload para a tabela 'srag_datalake'...")
            df.write.format("jdbc").option("url", self.db_connection).option("dbtable", "srag_datalake").option("user", "devdavi").option("password", "12345678").option("driver", "com.mysql.cj.jdbc.Driver").mode("overwrite").save()
            print("Upload concluído com sucesso!")
        except Exception as e:
            print(f"Erro ao fazer o upload para o banco de dados: {e}")
            return

        print("Processo de atualização do Data Lake finalizado com sucesso!")

