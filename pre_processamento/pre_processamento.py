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
        query = "SELECT * FROM srag_datalake"
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

    def remove_columns(self):
        """
        Remove colunas específicas de um DataFrame pandas.
        """
        columns_to_remove = ["TP_IDADE", "SEM_NOT", "SEM_PRI", "COD_IDADE", "CO_MUN_RES", "SURTO",
                             "CO_RG_INTE", "CO_MU_INTE", "HISTO_VGM", "PCR_SARS2", "PAC_COCBO",
                             "ID_REGIONA", "CO_MU_NOT", "CO_UNI_NOT", "CO_PAIS", "COD_RG_RESI",
                             "SURTO_SG", "DT_RAIOX", "DT_ENCERRA", "PAIS_VGM", "CO_VGM", "LO_PS_VGM",
                             "DT_RT_VGM", "DT_TOMO", "DT_RES_AN", "DT_CO_SOR", "DT_RES"]
        # Filtra apenas as colunas que existem no DataFrame
        existing_columns = [col for col in columns_to_remove if col in self.df.columns]
        self.df = self.df.drop(columns=existing_columns)
        print("As colunas foram removidas com sucesso.")

    def processar_colunas_data(self):
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

    def tem_texto(series):
        """Verifica se a coluna contém qualquer caractere não numérico (excluindo espaços)."""
        return series.astype(str).str.strip().str.contains(r"[a-zA-Z]", regex=True, na=False).any()


    def processar_colunas_numericas(self):
        for col in self.df.select_dtypes(include=['object']).columns:
            self.df
            # Remover espaços em branco extras antes de verificar
            self.df[col] = self.df[col].astype(str).str.strip()

            if not self.tem_texto(self.df[col]):  # Só converte se não houver letras
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                print(f"✅ Coluna '{col}' convertida para numérico.")
            else:
                print(f"🔤 Coluna '{col}' contém texto e foi mantida como string.")

    def processar_colunas_texto(self):

        # Converte as colunas para texto (string), ou para categoria se necessário
        for col in self.df.select_dtypes(include=['object']).columns:
            try:
                # Verifica se o tamanho único de valores na coluna é grande o suficiente para considerar como categoria
                unique_vals = self.df[col].dropna().unique()
                if unique_vals < 0.1 * len(self.df):
                    self.df[col] = self.df[col].astype('category')  # Coluna com poucas categorias
                    print(f"Coluna '{col}' convertida para 'category'.")
                else:
                    self.df[col] = self.df[col].astype(str)  # Caso contrário, converte para string
                    print(f"Coluna '{col}' convertida para string.")
            except Exception as e:
                print(f"Erro ao converter coluna '{col}': {e}")




    def executar_pipeline(self):
        """
        Executa todos os passos da pipeline em sequência.
        """
        print("Iniciando o processo de remoção de colunas...")
        self.remove_columns()
        print("\nIniciando o processo de conversão de tipos...")
        self.processar_colunas_data()
        self.processar_colunas_numericas()
        self.processar_colunas_texto()

        print(self.df.info())
        print("\nPipeline executada com sucesso.")





