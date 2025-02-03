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
        - Categorias: Checa colunas que devem ter valores fixos (exemplo: CS_SEXO, CS_RACA).

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

        # 📌 Corrigir a coluna CS_SEXO (substituir 'M' → 1 e 'F' → 2)
        if "CS_SEXO" in df_corrigido.columns:
            df_corrigido["CS_SEXO"] = df_corrigido["CS_SEXO"].replace({"M": 1, "F": 2})
            print("✅ Coluna 'CS_SEXO' normalizada (M → 1, F → 2).")

        # 📌 Corrigir a coluna FATOR_RISC (substituir 'S' → 1 e 'N' → 2)
        if "FATOR_RISC" in df_corrigido.columns:
            df_corrigido["FATOR_RISC"] = df_corrigido["FATOR_RISC"].replace({"S": 1, "N": 2})
            print("✅ Coluna 'FATOR_RISC' normalizada (S → 1, N → 2).")

        # 📌 Verificar colunas de idade (assumindo que são numéricas)
        for col in df_corrigido.select_dtypes(include=[np.number]).columns:
            if "idade" in col.lower():  # Identifica colunas relacionadas à idade
                outliers_idade = df_corrigido[(df_corrigido[col] < 0) | (df_corrigido[col] > 120)][col]
                if not outliers_idade.empty:
                    print(f"⚠️ Outliers encontrados na coluna '{col}' (valores fora do intervalo 0-120):")
                    print(outliers_idade)
                    outliers["idade"].append({col: outliers_idade.tolist()})

        # 📌 Verificar colunas categóricas com valores esperados
        categorias_esperadas = {
            "CS_SEXO": [1, 2, 9],  # Apenas valores válidos: 1 (M), 2 (F), 9 (Ignorado)
            "CS_RACA": [1, 2, 3, 4, 5, 9],
            "CS_ZONA": [1, 2, 3, 9],
            "OUTRO_DES": [9],
            "OUT_AMOST": [9],
            "MORB_DESC": [9]
        }

        for col, valores_validos in categorias_esperadas.items():
            if col in df_corrigido.columns:
                outliers_categoricos = df_corrigido[~df_corrigido[col].isin(valores_validos)][col]
                if not outliers_categoricos.empty:
                    print(f"⚠️ Valores inválidos na coluna categórica '{col}':")
                    print(outliers_categoricos)
                    outliers["categóricos"].append({col: outliers_categoricos.tolist()})

        # 📌 Verificar colunas de datas corretamente
        for col in df_corrigido.select_dtypes(include=[object]).columns:
            if "data" in col.lower() or "dt_" in col.lower():  # Identifica colunas de data
                try:
                    df_corrigido[col] = pd.to_datetime(df_corrigido[col], errors="coerce", dayfirst=True)
                    outliers_datas = df_corrigido[(df_corrigido[col] < "1900-01-01") | (df_corrigido[col] > pd.Timestamp.today())][col]
                    if not outliers_datas.empty:
                        print(f"⚠️ Outliers em datas na coluna '{col}' (fora do intervalo esperado):")
                        print(outliers_datas)
                        outliers["datas"].append({col: outliers_datas.tolist()})
                except Exception as e:
                    print(f"❌ Erro ao processar coluna de data '{col}': {e}")

        # 📌 Verificar colunas de texto para detecção de possíveis outliers
        for col in df_corrigido.select_dtypes(include=[object]).columns:
            if col not in categorias_esperadas:  # Evita colunas categóricas com valores fixos
                comprimento_texto = df_corrigido[col].dropna().apply(len)
                outliers_texto = df_corrigido[(comprimento_texto < 2) | (comprimento_texto > 50)][col]
                if not outliers_texto.empty:
                    print(f"⚠️ Possíveis outliers em texto na coluna '{col}':")
                    print(outliers_texto)
                    outliers["texto"].append({col: outliers_texto.tolist()})

        print("\n✅ Verificação de outliers concluída!")

    def remover_outliers(self, df):
        """
        Remove outliers do DataFrame em colunas de idade, datas, texto e categóricas.

        Parâmetros:
        - df (pd.DataFrame): DataFrame Pandas contendo os dados.

        Retorna:
        - df_limpo (pd.DataFrame): DataFrame sem outliers.
        """

        print("\n🔍 Iniciando remoção de outliers...\n")

        # Criar cópia para evitar modificar o DataFrame original

        linhas_removidas = set()

        # 📌 Corrigir a coluna CS_SEXO (substituir 'M' → 1 e 'F' → 2)
        if "CS_SEXO" in df_limpo.columns:
            df_limpo["CS_SEXO"] = df_limpo["CS_SEXO"].replace({"M": 1, "F": 2})
        
        # 📌 Corrigir a coluna CS_SEXO (substituir 'M' → 1 e 'F' → 2)
        if "FATOR_RISC" in df_limpo.columns:
            df_limpo["FATOR_RISC"] = df_limpo["FATOR_RISC"].replace({"S": 1, "N": 2})
            print("✅ Coluna 'CS_SEXO' normalizada (M → 1, F → 2).")

        # 📌 Remover outliers em colunas de idade (valores <0 ou >120)
        for col in df.select_dtypes(include=[np.number]).columns:
            if "idade" in col.lower():
                outliers_idade = df_limpo[(df_limpo[col] < 0) | (df_limpo[col] > 120)].index
                linhas_removidas.update(outliers_idade)
                if len(outliers_idade) > 0:
                    print(f"⚠️ {len(outliers_idade)} outliers removidos na coluna '{col}' (valores <0 ou >120).")

        # 📌 Remover outliers em colunas de datas
        for col in df.select_dtypes(include=[object]).columns:
            if "data" in col.lower() or "dt_" in col.lower():
                try:
                    df_limpo[col] = pd.to_datetime(df_limpo[col], errors="coerce", dayfirst=True)
                    outliers_datas = df_limpo[(df_limpo[col] < "1900-01-01") | (df_limpo[col] > pd.Timestamp.today())].index
                    linhas_removidas.update(outliers_datas)
                    if len(outliers_datas) > 0:
                        print(f"⚠️ {len(outliers_datas)} outliers removidos na coluna '{col}' (datas inválidas).")
                except Exception as e:
                    print(f"❌ Erro ao processar coluna de data '{col}': {e}")

        # 📌 Remover outliers em colunas categóricas (ex: CS_SEXO)
        categorias_esperadas = {
            "CS_RACA": [1, 2, 3, 4, 5, 9],  # Exemplo para outra coluna categórica
            "CS_ZONA" : [1, 2, 3, 9],
            "OUTRO_DES": [9],
            "OUT_AMOST": [9],
            "MORB_DESC": [9]
        }

        for col, valores_validos in categorias_esperadas.items():
            if col in df.columns:
                outliers_categoricos = df_limpo[~df_limpo[col].isin(valores_validos)].index
                linhas_removidas.update(outliers_categoricos)
                if len(outliers_categoricos) > 0:
                    print(f"⚠️ {len(outliers_categoricos)} outliers removidos na coluna categórica '{col}'.")

        # 📌 Remover outliers em colunas de texto
        for col in df.select_dtypes(include=[object]).columns:
            if df[col].nunique() < (len(df) * 0.5):  # Considera colunas categóricas
                comprimento_texto = df_limpo[col].dropna().apply(len)
                outliers_texto = df_limpo[(comprimento_texto < 2) | (comprimento_texto > 50)].index
                linhas_removidas.update(outliers_texto)
                if len(outliers_texto) > 0:
                    print(f"⚠️ {len(outliers_texto)} outliers removidos na coluna de texto '{col}'.")

        # 📌 Remover todas as linhas com outliers detectados
        df_limpo = df_limpo.drop(index=list(linhas_removidas))

        print(f"\n✅ Remoção de outliers concluída! Total de {len(linhas_removidas)} linhas removidas.")
        print("📊 DataFrame finalizado e pronto para análise.\n")

        return df_limpo  # Retorna o DataFrame sem outliers
    
    def executar_outliers(self):
        self.verificar_outliers(self.df)
        self.df = self.remover_outliers(self.df)
        return self.df

