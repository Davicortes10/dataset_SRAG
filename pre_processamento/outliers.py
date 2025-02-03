import pandas as pd
import numpy as np

class Outliers:
    def __init__(self,df):
        self.df = df

    def verificar_outliers(df):
        """
        Identifica outliers em um DataFrame analisando colunas de idade, datas e texto.

        Parâmetros:
        - df (pd.DataFrame): DataFrame Pandas contendo os dados.

        Retorna:
        - Um dicionário com detalhes dos outliers detectados.
        """

        outliers = {"idade": [], "datas": [], "texto": []}

        print("🔍 Verificando outliers...\n")

        # 📌 Verificar colunas de idade (assumindo que são numéricas)
        for col in df.select_dtypes(include=[np.number]).columns:
            if "idade" in col.lower():
                outliers_idade = df[(df[col] < 0) | (df[col] > 120)][col]
                if not outliers_idade.empty:
                    print(f"⚠️ Outliers encontrados na coluna '{col}':")
                    print(outliers_idade)
                    outliers["idade"].append({col: outliers_idade.tolist()})

        # 📌 Verificar colunas de datas
        for col in df.select_dtypes(include=[object]).columns:
            if "data" in col.lower() or "dt_" in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                    outliers_datas = df[(df[col] < "1900-01-01") | (df[col] > pd.Timestamp.today())][col]
                    if not outliers_datas.empty:
                        print(f"⚠️ Outliers em datas na coluna '{col}':")
                        print(outliers_datas)
                        outliers["datas"].append({col: outliers_datas.tolist()})
                except Exception as e:
                    print(f"❌ Erro ao processar coluna de data '{col}': {e}")

        # 📌 Verificar colunas de texto
        for col in df.select_dtypes(include=[object]).columns:
            if df[col].nunique() < (len(df) * 0.5):  # Considera colunas categóricas
                comprimento_texto = df[col].dropna().apply(len)
                outliers_texto = df[(comprimento_texto < 2) | (comprimento_texto > 50)][col]
                if not outliers_texto.empty:
                    print(f"⚠️ Possíveis outliers em texto na coluna '{col}':")
                    print(outliers_texto)
                    outliers["texto"].append({col: outliers_texto.tolist()})

        print("\n✅ Verificação concluída!")

    def remover_outliers(df):
        """
        Remove outliers do DataFrame em colunas de idade, datas e texto.

        Parâmetros:
        - df (pd.DataFrame): DataFrame Pandas contendo os dados.

        Retorna:
        - df_limpo (pd.DataFrame): DataFrame sem outliers.
        """

        print("🔍 Removendo outliers...\n")

        df_limpo = df.copy()  # Criar uma cópia para evitar alterar o original
        linhas_removidas = set()

        # 📌 Remover outliers em colunas de idade (valores <0 ou >120)
        for col in df.select_dtypes(include=[np.number]).columns:
            if "idade" in col.lower():
                outliers_idade = df_limpo[(df_limpo[col] < 0) | (df_limpo[col] > 120)].index
                linhas_removidas.update(outliers_idade)

        # 📌 Remover outliers em colunas de datas
        for col in df.select_dtypes(include=[object]).columns:
            if "data" in col.lower() or "dt_" in col.lower():
                try:
                    df_limpo[col] = pd.to_datetime(df_limpo[col], errors="coerce", dayfirst=True)
                    outliers_datas = df_limpo[(df_limpo[col] < "1900-01-01") | (df_limpo[col] > pd.Timestamp.today())].index
                    linhas_removidas.update(outliers_datas)
                except Exception as e:
                    print(f"❌ Erro ao processar coluna de data '{col}': {e}")

        # 📌 Remover outliers em colunas de texto
        for col in df.select_dtypes(include=[object]).columns:
            if df[col].nunique() < (len(df) * 0.5):  # Considera colunas categóricas
                comprimento_texto = df_limpo[col].dropna().apply(len)
                outliers_texto = df_limpo[(comprimento_texto < 2) | (comprimento_texto > 50)].index
                linhas_removidas.update(outliers_texto)

        # 📌 Remover todas as linhas com outliers
        df_limpo = df_limpo.drop(index=list(linhas_removidas))

        print(f"✅ Total de {len(linhas_removidas)} linhas removidas por outliers.")
        print("📊 DataFrame limpo retornado.")

        return df_limpo  # Retorna o DataFrame sem outliers
    
    def executar_outliers(self):
        self.verificar_outliers(self.df)
        self.remover_outliers(self.df)

