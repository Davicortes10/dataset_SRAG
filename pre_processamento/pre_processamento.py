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
    
    import pandas as pd

    def converter_tipos_colunas(self, df):
        """
        Converte automaticamente as colunas do DataFrame para os tipos apropriados:

        - Se o nome da coluna contiver "DT", converte para DATETIME.
        - Se todos os valores forem numéricos, converte para INT.
        - Caso contrário, converte para STRING.

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
                # Remover valores nulos temporariamente
                valores_validos = df[col].dropna()

                if valores_validos.empty:
                    print(f"⚠️ Coluna '{col}' vazia. Mantendo como está.")
                    continue

                # 🚀 Se o nome da coluna contém 'DT', forçar conversão para DATETIME
                if "DT" in col.upper():
                    df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                    print(f"📅 Coluna '{col}' convertida para DATETIME.")

                # 🚀 Se todos os valores são numéricos, converter para INT
                elif pd.to_numeric(valores_validos, errors="coerce").notna().all():
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
                    print(f"✅ Coluna '{col}' convertida para INT.")

                # 🚀 Caso contrário, converter para STRING
                else:
                    df[col] = df[col].astype(str)
                    print(f"🔤 Coluna '{col}' convertida para STRING.")

            print("\n✅ Conversão de tipos concluída!")
            print(df.info())
            return df

        except Exception as e:
            print(f"❌ Erro ao converter tipos: {str(e)}")
            return df




        





