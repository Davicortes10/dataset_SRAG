from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
import numpy as np


class PreprocessDataset:
    def __init__(self, df):
        self.df = df

    def converter_tipos_colunas(df):
        """
        Converte automaticamente as colunas do DataFrame para os tipos apropriados:
        
        - Se todos os valores forem numéricos, converte para int.
        - Se a maioria dos valores estiver em formato de data, converte para datetime.
        - Caso contrário, converte para string.

        Parâmetros:
        - df (pd.DataFrame): O DataFrame a ser processado.

        Retorna:
        - pd.DataFrame: O DataFrame atualizado com os tipos de colunas convertidos.
        """

        try:
            # 🚀 Criar uma cópia do DataFrame para evitar modificar o original
            df = df.copy()

            print("🔄 Iniciando conversão automática de tipos...\n")

            for col in df.columns:
                # Remover NaNs temporariamente para evitar interferência na análise de tipo
                valores_validos = df[col].dropna()

                if valores_validos.empty:
                    print(f"⚠️ Coluna '{col}' vazia. Mantendo como está.")
                    continue

                # 🚀 Tentar converter para número inteiro
                if pd.to_numeric(valores_validos, errors='coerce').notna().all():
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                    print(f"✅ Coluna '{col}' convertida para INT.")

                # 🚀 Tentar converter para datetime
                elif pd.to_datetime(valores_validos, errors='coerce', dayfirst=True).notna().sum() > (len(valores_validos) * 0.8):
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                    print(f"📅 Coluna '{col}' convertida para DATETIME.")

                # 🚀 Caso contrário, converter para string
                else:
                    df[col] = df[col].astype(str)
                    print(f"🔤 Coluna '{col}' convertida para STRING.")

            print("\n✅ Conversão de tipos concluída!")
            return df

        except Exception as e:
            print(f"❌ Erro ao converter tipos: {str(e)}")
            return df

    def executar_pipeline(self):
        """
        Executa todos os passos da pipeline em sequência.
        """
        print("\nIniciando o processo de conversão de tipos...")
        self.converter_tipos_colunas()

        print(self.df.info())
        print("\nPipeline executada com sucesso.")





