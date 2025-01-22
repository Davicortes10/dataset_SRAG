from sqlalchemy import create_engine
import pandas as pd
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


class PreprocessDataset:
    def __init__(self):
        self.gcp_db_connection = "mysql+pymysql://devdavi:12345678@34.170.252.6/srag_datalake"
        self.conn = create_engine(self.gcp_db_connection)
        self.df = self.ler_gcp_DB()

    def ler_gcp_DB(self):
        # Escrevendo a consulta SQL para ler os dados da tabela
        query = "SELECT * FROM srag_warehouse"
        pd.set_option("display.max_columns", None)
        # Lendo os dados para um DataFrame Pandas
        df = pd.read_sql(query, con=self.conn)
        # Exibir informações iniciais sobre os dados
        print("Primeiros registros:")
        print(df.head())  # Visualizar os primeiros registros
        print("\nInformações da tabela:")
        print(df.info())  # Tipos de dados e valores ausentes
        print("\nResumo estatístico:")
        print(df.describe(include="all"))  # Resumo para valores numéricos e categóricos
        return df

    def processar_tipos_colunas(self):
        # Colunas do tipo 'data' (começam com 'DT')
        date_columns = [col for col in self.df.columns if col.startswith("DT")]

        # Conversão das colunas de data
        for col in date_columns:
            try:
                self.df[col] = pd.to_datetime(self.df[col], errors="coerce",dayfirst=True)
                print(f"Coluna '{col}' convertida para datetime.")
            except Exception as e:
                print(f"Erro ao converter coluna '{col}': {e}")

        print("Conversão de tipos finalizada.")
        print(self.df[date_columns].info())




