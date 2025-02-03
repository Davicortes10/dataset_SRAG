from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import pymysql
import numpy as np


class GCP_Dataset:
    def __init__(self):
        self.gcp_db_connection = "mysql+pymysql://devdavi:12345678@34.170.252.6/srag_datalake"
        self.conn = create_engine(self.gcp_db_connection)
        self.df = self.ler_gcp_DB()
    
    def ler_gcp_DB(self):
        """
        Conecta ao banco de dados GCP e lê os dados da tabela 'srag_datalake'.

        Funcionalidades:
        - Executa uma consulta SQL para buscar todos os dados da tabela.
        - Carrega os dados em um DataFrame Pandas.
        - Exibe uma prévia dos dados para facilitar a análise.
        - Mostra informações sobre os tipos de dados e valores ausentes.
        - Apresenta um resumo estatístico dos dados.

        Parâmetros:
        - self (objeto): Deve conter a conexão ativa com o banco de dados GCP na variável `self.conn`.

        Retorna:
        - df (pd.DataFrame): DataFrame contendo os dados da tabela.

        Tratamento de Erros:
        - Captura falhas na conexão ou na leitura dos dados e exibe mensagens detalhadas.
        """

        try:
            # 🚀 Definir a consulta SQL
            query = "SELECT * FROM srag_datalake"
            print("🔍 Executando consulta no banco de dados...")

            # 🚀 Ativar exibição de todas as colunas no Pandas
            pd.set_option("display.max_columns", None)

            # 🚀 Ler os dados para um DataFrame Pandas
            df = pd.read_sql(query, con=self.conn)

            # 🚀 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ A tabela 'srag_datalake' está vazia!")
                return df

            # 🔍 Exibir informações iniciais sobre os dados
            print("\n📊 Primeiros registros da tabela:")
            print(df.head())

            print("\nℹ️ Informações da tabela:")
            print(df.info())

            print("\n📈 Resumo estatístico:")
            print(df.describe(include="all"))

            print("\n✅ Leitura concluída com sucesso!")

            return df

        except Exception as e:
            print(f"❌ Erro ao ler os dados do banco GCP: {str(e)}")
            return None