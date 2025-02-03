import pandas as pd
import numpy as np

class Outliers:
    def __init__(self,df):
        self.df = df

    def verificar_outliers(self, df):
        """
        Identifica e trata outliers em colunas de idade, datas, texto e variáveis categóricas.

        Regras:
        - Idade: Remove valores < 0 ou > 120.
        - Datas: Detecta valores antes de 1900 ou no futuro.
        - Texto: Verifica campos categóricos e identifica possíveis inconsistências.
        - Categorias: Checa colunas que devem ter valores fixos.

        **Ignora as colunas específicas que não devem ser processadas.**

        Parâmetros:
        - df (pd.DataFrame): DataFrame Pandas contendo os dados.

        Retorna:
        - self.df (pd.DataFrame): DataFrame corrigido, com ajustes aplicados.
        - outliers (dict): Dicionário contendo os outliers detectados.
        """

        # Criando um dicionário para armazenar os outliers detectados
        outliers = {"idade": [], "datas": [], "texto": [], "categóricos": []}

        print("\n🔍 Iniciando verificação de outliers...\n")

        self.df = df.copy()  # Criar cópia do DataFrame para evitar modificar o original

        # 📌 Lista de colunas a serem ignoradas
        colunas_ignoradas = ["CS_SEXO", "CS_ZONA", "OUTRO_DES", "OUT_AMOST", "MORB_DESC"]

        # 📌 Verificar colunas de idade
        for col in self.df.select_dtypes(include=[np.number]).columns:
            if "idade" in col.lower() and col not in colunas_ignoradas:
                outliers_idade = self.df[(self.df[col] < 0) | (self.df[col] > 120)][col]
                if not outliers_idade.empty:
                    print(f"⚠️ Outliers encontrados na coluna '{col}' (valores fora do intervalo 0-120):")
                    print(outliers_idade)
                    self.df = self.df[~self.df[col].isin(outliers_idade)]
                    outliers["idade"].append({col: outliers_idade.tolist()})

        # 📌 Verificar colunas de datas (colunas que contêm 'DT' no nome)
        for col in self.df.columns:
            if "DT" in col.upper() and col not in colunas_ignoradas:
                try:
                    self.df[col] = pd.to_datetime(self.df[col], errors="coerce", dayfirst=True)
                    outliers_datas = self.df[(self.df[col] < "1900-01-01") | (self.df[col] > pd.Timestamp.today())][col]
                    if not outliers_datas.empty:
                        print(f"⚠️ Outliers em datas na coluna '{col}' (fora do intervalo esperado):")
                        print(outliers_datas)
                        self.df = self.df[~self.df[col].isin(outliers_datas)]
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
            if col in self.df.columns and col not in colunas_ignoradas:
                outliers_categoricos = self.df[~self.df[col].isin(valores_validos)][col]
                if not outliers_categoricos.empty:
                    print(f"⚠️ Valores inválidos na coluna categórica '{col}':")
                    print(outliers_categoricos)
                    self.df = self.df[~self.df[col].isin(outliers_categoricos)]
                    outliers["categóricos"].append({col: outliers_categoricos.tolist()})

        # 📌 Verificar colunas de texto para detecção de possíveis outliers (excluindo colunas ignoradas)
        for col in self.df.select_dtypes(include=[object]).columns:
            if col not in categorias_esperadas and col not in colunas_ignoradas:
                comprimento_texto = self.df[col].dropna().apply(len)
                outliers_texto = self.df[(comprimento_texto < 2) | (comprimento_texto > 50)][col]
                if not outliers_texto.empty:
                    print(f"⚠️ Possíveis outliers em texto na coluna '{col}':")
                    print(outliers_texto)
                    self.df = self.df[~self.df[col].isin(outliers_texto)]
                    outliers["texto"].append({col: outliers_texto.tolist()})

        print("\n✅ Verificação de outliers concluída!")

    def remover_outliers(self, df):
        """
        Remove outliers do DataFrame em colunas de idade, datas, texto e categóricas.

        Regras:
        - Idade: Remove valores < 0 ou > 120.
        - Datas: Remove valores antes de 1900 ou no futuro.
        - Categorias: Remove valores inválidos em colunas categóricas predefinidas.
        - Texto: Remove valores muito curtos (<2 caracteres) ou longos (>50 caracteres).

        **Ignora colunas que não devem ser processadas.**

        Parâmetros:
        - df (pd.DataFrame): DataFrame Pandas contendo os dados.

        Retorna:
        - self.df (pd.DataFrame): DataFrame sem outliers.
        """

        print("\n🔍 Iniciando remoção de outliers...\n")

        # Criar uma cópia do DataFrame para evitar modificar o original
        linhas_removidas = set()

        # 📌 Lista de colunas que não devem ser alteradas
        colunas_ignoradas = ["CS_SEXO", "CS_ZONA", "OUTRO_DES", "OUT_AMOST", "MORB_DESC"]

        # 📌 Normalizar a coluna CS_SEXO (substituir 'M' → 1 e 'F' → 2)
        if "CS_SEXO" in self.df.columns:
            self.df["CS_SEXO"] = self.df["CS_SEXO"].replace({"M": 1, "F": 2})
        
        # 📌 Normalizar a coluna FATOR_RISC (substituir 'S' → 1 e 'N' → 2)
        if "FATOR_RISC" in self.df.columns:
            self.df["FATOR_RISC"] = self.df["FATOR_RISC"].replace({"S": 1, "N": 2})

        # 📌 Remover outliers em colunas de idade
        for col in self.df.select_dtypes(include=[np.number]).columns:
            if "idade" in col.lower() and col not in colunas_ignoradas:
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

