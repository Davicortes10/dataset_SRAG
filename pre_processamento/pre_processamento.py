from sqlalchemy import create_engine
import pandas as pd
import pymysql
from pyspark.sql import SparkSession

class PreprocessDataset:
    def __init__(self,file_path):
        self.file_path = file_path
        self.spark = SparkSession.builder.appName("srag").getOrCreate()
        self.db_connection = "mysql+pymysql://devdavi:12345678@34.170.252.6/srag_datalake"

    def ler_arquivo(self):
        try:
            arquivo_csv = "/content/INFLUD20-01-05-2023.csv"

            # Ler o arquivo CSV com separador ";"
            print(f"[INFO] Tentando carregar o arquivo: {arquivo_csv}")
            df = self.spark.read.format("csv").option("header", "true").option("sep", ";").option("inferSchema", "true").load(arquivo_csv)

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
                self.spark.stop()
                print("[INFO] SparkSession encerrada.")
            except Exception as e:
                print(f"[ERROR] Erro ao encerrar SparkSession: {e}")
    
    def atualizar_Data_Lake(self):
        print("Iniciando o processo de atualização do Data Lake...")
        
        print(f"Conectando ao banco de dados: {self.db_connection}")

        # Criar a conexão com o banco
        try:
            conn = create_engine(self.db_connection)
            print("Conexão com o banco de dados criada com sucesso!")
        except Exception as e:
            print(f"Erro ao criar a conexão com o banco: {e}")
            return

        # Leitura do CSV
        try:
            print(f"Lendo o arquivo CSV em: {self.file_path}")
            df = pd.read_csv(self.file_path, sep=";")
            print(f"Arquivo CSV lido com sucesso! Dimensões do DataFrame: {df.shape}")
            print(f"Colunas detectadas no DataFrame: {list(df.columns)}")
        except Exception as e:
            print(f"Erro ao ler o arquivo CSV: {e}")
            return

        # Upload para o banco de dados
        try:
            print(f"Iniciando o upload para a tabela 'srag_datalake'...")
            df.to_sql("srag_datalake", con=conn, if_exists="replace", index=False)
            print("Upload concluído com sucesso!")
        except Exception as e:
            print(f"Erro ao fazer o upload para o banco de dados: {e}")
            return

        print("Processo de atualização do Data Lake finalizado com sucesso!")

