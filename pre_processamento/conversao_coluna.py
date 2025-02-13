from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


class PreprocessDataset:
    def __init__(self, df):
        self.df = df

    def converter_tipos_colunas(self):
        """
        Converte automaticamente as colunas do DataFrame para os tipos apropriados:

        - Se o nome da coluna contiver "DT", converte para DATETIME.
        - Se a coluna for 'NU_IDADE_N', converte para INT.
        - Se todos os valores forem numéricos, converte para INT.
        - Se houver mistura de números e texto, converte para STRING.
        - Nenhuma coluna permanecerá com o tipo OBJECT.

        Parâmetros:
        - self.df (pd.DataFrame): O DataFrame a ser processado.

        Retorna:
        - pd.DataFrame: O DataFrame atualizado com os tipos de colunas convertidos.
        """
        try:
            print("🔄 Iniciando conversão automática de tipos...\n")

            for col in self.df.columns:
                # Remover valores nulos temporariamente para análise
                valores_validos = self.df[col].dropna()

                if valores_validos.empty:
                    print(f"⚠️ Coluna '{col}' vazia. Mantendo como está.")
                    continue

                # 🚀 Se o nome da coluna contém 'DT', forçar conversão para DATETIME
                if "DT" in col.upper():
                    self.df[col] = pd.to_datetime(self.df[col], errors="coerce", dayfirst=True)
                    print(f"📅 Coluna '{col}' convertida para DATETIME.")

                # 🚀 Se a coluna for 'NU_IDADE_N', converter para INT
                elif col == "NU_IDADE_N":
                    self.df[col] = pd.to_numeric(self.df[col], errors="coerce").fillna(0).astype(int)
                    print(f"✅ Coluna '{col}' convertida para INT.")

                # 🚀 Se todos os valores são numéricos, converter para INT
                elif valores_validos.apply(lambda x: str(x).replace(".", "").isdigit()).all():
                    self.df[col] = pd.to_numeric(self.df[col], errors="coerce").fillna(0).astype(int)
                    print(f"✅ Coluna '{col}' convertida para INT.")

                # 🚀 Se há mistura de números e texto, converter para STRING
                else:
                    self.df[col] = self.df[col].astype(str)
                    print(f"🔤 Coluna '{col}' convertida para STRING.")

            print("\n✅ Conversão de tipos concluída!")
            print(self.df.info())
            return self.df

        except Exception as e:
            print(f"❌ Erro ao converter tipos: {str(e)}")
            return self.df





        





