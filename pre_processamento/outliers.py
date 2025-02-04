import pandas as pd
import numpy as np

class Outliers:
    def __init__(self,df):
        self.df = df

    def verificar_outliers(self, df):
        """
        Identifica e trata outliers em colunas de idade, datas, texto e variáveis categóricas.

        Regras:
        - Idade: Remove valores <= 0 ou > 120 (especialmente para 'NU_IDADE_N').
        - Datas: Detecta valores antes de 1900 ou no futuro.
        - Texto: Verifica campos categóricos e identifica possíveis inconsistências.
        - Categorias: Checa colunas que devem ter valores fixos.

        **Ignora colunas específicas que não devem ser processadas.**

        Parâmetros:
        - df (pd.DataFrame): DataFrame Pandas contendo os dados.

        Retorna:
        - df_corrigido (pd.DataFrame): DataFrame corrigido, com ajustes aplicados.
        - outliers (dict): Dicionário contendo os outliers detectados.
        """

        # Criando um dicionário para armazenar os outliers detectados
        outliers = {"idade": [], "datas": [], "texto": [], "categóricos": []}

        print("\n🔍 Iniciando verificação de outliers...\n")

        df_corrigido = df.copy()  # Criar cópia do DataFrame para evitar modificar o original

        # 📌 Lista de colunas a serem ignoradas
        colunas_ignoradas = ["CS_ZONA", "OUTRO_DES", "OUT_AMOST", "MORB_DESC", "FATOR_RISC"]

        # 📌 Remover outliers na coluna de idade 'NU_IDADE_N'
        if "NU_IDADE_N" in df_corrigido.columns:
            outliers_idade = df_corrigido[(df_corrigido["NU_IDADE_N"] <= 0) | (df_corrigido["NU_IDADE_N"] > 120)]["NU_IDADE_N"]
            if not outliers_idade.empty:
                print(f"⚠️ Outliers encontrados na coluna 'NU_IDADE_N' (valores <= 0 ou > 120):")
                print(outliers_idade)
                df_corrigido = df_corrigido[~df_corrigido["NU_IDADE_N"].isin(outliers_idade)]
                outliers["idade"].append({"NU_IDADE_N": outliers_idade.tolist()})

        # 📌 Verificar colunas de idade (excluindo a já tratada 'NU_IDADE_N')
        for col in df_corrigido.select_dtypes(include=[np.number]).columns:
            if "idade" in col.lower() and col not in colunas_ignoradas and col != "NU_IDADE_N":
                outliers_idade = df_corrigido[(df_corrigido[col] < 0) | (df_corrigido[col] > 120)][col]
                if not outliers_idade.empty:
                    print(f"⚠️ Outliers encontrados na coluna '{col}' (valores fora do intervalo 0-120):")
                    print(outliers_idade)
                    df_corrigido = df_corrigido[~df_corrigido[col].isin(outliers_idade)]
                    outliers["idade"].append({col: outliers_idade.tolist()})

        # 📌 Verificar colunas de datas (colunas que contêm 'DT' no nome)
        for col in df_corrigido.columns:
            if "DT" in col.upper() and col not in colunas_ignoradas:
                try:
                    df_corrigido[col] = pd.to_datetime(df_corrigido[col], errors="coerce", dayfirst=True)
                    outliers_datas = df_corrigido[(df_corrigido[col] < "1900-01-01") | (df_corrigido[col] > pd.Timestamp.today())][col]
                    if not outliers_datas.empty:
                        print(f"⚠️ Outliers em datas na coluna '{col}' (fora do intervalo esperado):")
                        print(outliers_datas)
                        df_corrigido = df_corrigido[~df_corrigido[col].isin(outliers_datas)]
                        outliers["datas"].append({col: outliers_datas.tolist()})
                except Exception as e:
                    print(f"❌ Erro ao processar coluna de data '{col}': {e}")

        # 📌 Verificar colunas categóricas com valores esperados (excluindo colunas ignoradas)
        categorias_esperadas = {
            "CS_RACA": [1, 2, 3, 4, 5, 9],
            "CS_ZONA": [1, 2, 3, 9],
            "OUTRO_DES": [9],
            "OUT_AMOST": [9],
            "MORB_DESC": [9]
        }

        for col, valores_validos in categorias_esperadas.items():
            if col in df_corrigido.columns and col not in colunas_ignoradas:
                outliers_categoricos = df_corrigido[~df_corrigido[col].isin(valores_validos)][col]
                if not outliers_categoricos.empty:
                    print(f"⚠️ Valores inválidos na coluna categórica '{col}':")
                    print(outliers_categoricos)
                    df_corrigido = df_corrigido[~df_corrigido[col].isin(outliers_categoricos)]
                    outliers["categóricos"].append({col: outliers_categoricos.tolist()})

        # 📌 Verificar colunas de texto para detecção de possíveis outliers (excluindo colunas ignoradas)
        for col in df_corrigido.select_dtypes(include=[object]).columns:
            if col not in categorias_esperadas and col not in colunas_ignoradas:
                comprimento_texto = df_corrigido[col].dropna().apply(len)
                outliers_texto = df_corrigido[(comprimento_texto < 2) | (comprimento_texto > 50)][col]
                if not outliers_texto.empty:
                    print(f"⚠️ Possíveis outliers em texto na coluna '{col}':")
                    print(outliers_texto)
                    df_corrigido = df_corrigido[~df_corrigido[col].isin(outliers_texto)]
                    outliers["texto"].append({col: outliers_texto.tolist()})

        print("\n✅ Verificação de outliers concluída!")

    def remover_outliers(self):
        """
        Remove outliers do DataFrame em colunas de idade, datas, texto e categóricas.

        Regras:
        - Idade: Remove valores <= 0 ou > 120 (especialmente para 'NU_IDADE_N').
        - Datas: Remove valores antes de 1900 ou no futuro.
        - Categorias: Remove valores inválidos em colunas categóricas predefinidas.
        - Texto: Remove valores muito curtos (<2 caracteres) ou longos (>50 caracteres).

        **Ignora colunas específicas que não devem ser processadas.**

        Parâmetros:
        - self.df (pd.DataFrame): DataFrame Pandas contendo os dados.

        Retorna:
        - self.df (pd.DataFrame): DataFrame sem outliers.
        """

        print("\n🔍 Iniciando remoção de outliers...\n")

        # Criar um conjunto para armazenar índices das linhas que serão removidas
        linhas_removidas = set()

        # 📌 Lista de colunas que não devem ser alteradas
        colunas_ignoradas = ["CS_SEXO", "CS_ZONA", "OUTRO_DES", "OUT_AMOST", "MORB_DESC", "FATOR_RISC"]

        # 📌 Remover outliers na coluna de idade 'NU_IDADE_N'
        if "NU_IDADE_N" in self.df.columns:
            outliers_idade = self.df[(self.df["NU_IDADE_N"] <= 0) | (self.df["NU_IDADE_N"] > 120)].index
            linhas_removidas.update(outliers_idade)
            if len(outliers_idade) > 0:
                print(f"⚠️ {len(outliers_idade)} outliers removidos na coluna 'NU_IDADE_N' (valores <= 0 ou > 120).")

        # 📌 Remover outliers em outras colunas de idade (excluindo 'NU_IDADE_N')
        for col in self.df.select_dtypes(include=[np.number]).columns:
            if "idade" in col.lower() and col not in colunas_ignoradas and col != "NU_IDADE_N":
                outliers_idade = self.df[(self.df[col] < 0) | (self.df[col] > 120)].index
                linhas_removidas.update(outliers_idade)
                if len(outliers_idade) > 0:
                    print(f"⚠️ {len(outliers_idade)} outliers removidos na coluna '{col}' (valores <0 ou >120).")

        # 📌 Remover outliers em colunas de datas (colunas que contêm 'DT' no nome)
        for col in self.df.columns:
            if "DT" in col.upper() and col not in colunas_ignoradas:
                try:
                    self.df[col] = pd.to_datetime(self.df[col], errors="coerce", dayfirst=True)
                    outliers_datas = self.df[(self.df[col] < "1900-01-01") | (self.df[col] > pd.Timestamp.today())].index
                    linhas_removidas.update(outliers_datas)
                    if len(outliers_datas) > 0:
                        print(f"⚠️ {len(outliers_datas)} outliers removidos na coluna '{col}' (datas inválidas).")
                except Exception as e:
                    print(f"❌ Erro ao processar coluna de data '{col}': {e}")

        # 📌 Remover outliers em colunas categóricas (excluindo colunas ignoradas)
        categorias_esperadas = {
            "CS_RACA": [1, 2, 3, 4, 5, 9],
            "CS_ZONA": [1, 2, 3, 9],
            "OUTRO_DES": [9],
            "OUT_AMOST": [9],
            "MORB_DESC": [9]
        }

        for col, valores_validos in categorias_esperadas.items():
            if col in self.df.columns and col not in colunas_ignoradas:
                outliers_categoricos = self.df[~self.df[col].isin(valores_validos)].index
                linhas_removidas.update(outliers_categoricos)
                if len(outliers_categoricos) > 0:
                    print(f"⚠️ {len(outliers_categoricos)} outliers removidos na coluna categórica '{col}'.")

        # 📌 Remover outliers em colunas de texto (excluindo colunas ignoradas)
        for col in self.df.select_dtypes(include=[object]).columns:
            if col not in categorias_esperadas and col not in colunas_ignoradas:
                comprimento_texto = self.df[col].dropna().apply(len)
                outliers_texto = self.df[(comprimento_texto < 2) | (comprimento_texto > 50)].index
                linhas_removidas.update(outliers_texto)
                if len(outliers_texto) > 0:
                    print(f"⚠️ {len(outliers_texto)} outliers removidos na coluna de texto '{col}'.")

        # 📌 Remover todas as linhas com outliers detectados
        self.df = self.df.drop(index=list(linhas_removidas))

        print(f"\n✅ Remoção de outliers concluída! Total de {len(linhas_removidas)} linhas removidas.")
        print("📊 DataFrame finalizado e pronto para análise.\n")

        return self.df  # Retorna o DataFrame sem outliers

    def executar_outliers(self):
        self.verificar_outliers(self.df)
        self.df = self.remover_outliers()
        return self.df

