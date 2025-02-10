import pandas as pd

class Normalizacao:
    def __init__(self, df):
        self.df = df
    
    def contar_sintomas_fatores(self):
        """
        Conta a quantidade de sintomas e fatores de risco em cada linha do DataFrame e cria duas novas colunas:
        - 'QTD_SINT': Quantidade de sintomas presentes.
        - 'QTD_FATOR_RISC': Quantidade de fatores de risco presentes.

        Parâmetros:
        - self.df (pd.DataFrame): DataFrame contendo as colunas de sintomas e fatores de risco.

        Retorna:
        - self.df (pd.DataFrame): DataFrame atualizado com as colunas 'QTD_SINT' e 'QTD_FATOR_RISC'.
        """

        print("\n🔍 Iniciando contagem de sintomas e fatores de risco...\n")

        # Definir as colunas que representam sintomas e fatores de risco
        colunas_sintomas = ["FEBRE", "TOSSE", "GARGANTA", "DISPNEIA", "DESC_RESP", "SATURACAO",
                            "DIARREIA", "VOMITO", "DOR_ABD", "FADIGA", "PERD_OLFT", "PERD_PALA"]

        colunas_fatores_risco = ["CARDIOPATI", "ASMA", "RENAL", "OBESIDADE", "NEUROLOGIC",
                                "PNEUMOPATI", "IMUNODEPRE", "HEMATOLOGI", "SIND_DOWN",
                                "HEPATICA", "DIABETES", "PUERPERA"]

        # 📌 Verificar se todas as colunas existem no DataFrame antes de prosseguir
        colunas_faltantes_sintomas = [col for col in colunas_sintomas if col not in self.df.columns]
        colunas_faltantes_fatores = [col for col in colunas_fatores_risco if col not in self.df.columns]

        if colunas_faltantes_sintomas:
            print(f"⚠️ Atenção! As seguintes colunas de sintomas estão ausentes no DataFrame: {colunas_faltantes_sintomas}")

        if colunas_faltantes_fatores:
            print(f"⚠️ Atenção! As seguintes colunas de fatores de risco estão ausentes no DataFrame: {colunas_faltantes_fatores}")

        # Filtrar apenas as colunas que existem no DataFrame para evitar erros
        colunas_sintomas = [col for col in colunas_sintomas if col in self.df.columns]
        colunas_fatores_risco = [col for col in colunas_fatores_risco if col in self.df.columns]

        # Converter os valores para numérico (evita erros com strings)
        for col in colunas_sintomas + colunas_fatores_risco:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

        # Contar quantidade de sintomas presentes (valores 1)
        self.df["QTD_SINT"] = self.df[colunas_sintomas].apply(lambda row: (row == 1).sum(), axis=1)

        # Contar quantidade de fatores de risco presentes (valores 1)
        self.df["QTD_FATOR_RISC"] = self.df[colunas_fatores_risco].apply(lambda row: (row == 1).sum(), axis=1)

        # 📌 Exibir estatísticas da contagem para depuração
        print("\n📊 Estatísticas das colunas calculadas:")
        print(f"🔹 Média de sintomas por paciente: {self.df['QTD_SINT'].mean():.2f}")
        print(f"🔹 Média de fatores de risco por paciente: {self.df['QTD_FATOR_RISC'].mean():.2f}")

        print("\n✅ Contagem de sintomas e fatores de risco concluída.\n")

        return self.df

    def classificar_idade(self, df):
        """
        Classifica os indivíduos de acordo com a idade na coluna 'NU_IDADE_N' e cria uma nova coluna 'CLASSIF_IDADE'.

        Faixas etárias:
        - 0 a 12 anos  → 'Criança'
        - 13 a 17 anos → 'Adolescente'
        - 18 a 59 anos → 'Adulto'
        - 60+ anos     → 'Idoso'
        - Idade inválida (NaN, < 0 ou > 120) → 'Desconhecido'

        Parâmetros:
        - df (pd.DataFrame): DataFrame Pandas contendo a coluna 'NU_IDADE_N'.

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com a nova coluna 'CLASSIF_IDADE'.
        """

        print("\n🔍 Iniciando classificação de idade...\n")

        # 📌 Verificar se a coluna existe no DataFrame
        if "NU_IDADE_N" not in df.columns:
            print("⚠️ Erro: Coluna 'NU_IDADE_N' não encontrada no DataFrame!")
            return df

        # 📌 Exibir valores únicos antes da conversão para verificar inconsistências
        print("🔹 Valores únicos antes da conversão:", df["NU_IDADE_N"].unique())

        # Garantir que a coluna seja numérica
        df["NU_IDADE_N"] = pd.to_numeric(df["NU_IDADE_N"], errors="coerce")

        # 📌 Identificar valores inválidos antes da classificação
        valores_invalidos = df[(df["NU_IDADE_N"] < 0) | (df["NU_IDADE_N"] > 120)]["NU_IDADE_N"]
        if not valores_invalidos.empty:
            print(f"⚠️ Atenção! Foram encontrados {len(valores_invalidos)} valores inválidos na coluna 'NU_IDADE_N'.")
            print(valores_invalidos)

        # Criar a nova coluna CLASSIF_IDADE com base na idade
        df["CLASSIF_IDADE"] = df["NU_IDADE_N"].apply(lambda x:
            "Criança" if 0 <= x <= 12 else
            "Adolescente" if 13 <= x <= 17 else
            "Adulto" if 18 <= x <= 59 else
            "Idoso" if 60 <= x <= 120 else
            "Desconhecido"
        )

        # 📌 Exibir estatísticas da classificação para depuração
        print("\n📊 Estatísticas da nova coluna 'CLASSIF_IDADE':")
        print(df["CLASSIF_IDADE"].value_counts())

        print("\n✅ Classificação de idade concluída!\n")
        return df

    def normalizar_sexo_sinto(self, df):
        """
        Normaliza as colunas 'CS_SEXO' e 'FATOR_RISC' no DataFrame.

        Regras:
        - 'CS_SEXO': Substitui 'M' por 1, 'F' por 2, 'I' por 1 e converte para INT.
        - 'FATOR_RISC': Substitui 'S' por 1 e 'N' por 2 e converte para INT.

        Parâmetros:
        - df (pd.DataFrame): DataFrame Pandas contendo as colunas a serem normalizadas.

        Retorna:
        - pd.DataFrame: DataFrame atualizado com os valores normalizados.
        """

        # 📌 Normalizar a coluna CS_SEXO e converter para int
        if "CS_SEXO" in df.columns:
            df["CS_SEXO"] = df["CS_SEXO"].replace({"M": 1, "F": 2, "I": 1}).astype("Int64")
            print("✅ Coluna 'CS_SEXO' normalizada (M → 1, F → 2, I → 1) e convertida para INT.")

        # 📌 Normalizar a coluna FATOR_RISC e converter para int
        if "FATOR_RISC" in df.columns:
            df["FATOR_RISC"] = df["FATOR_RISC"].replace({"S": 1, "N": 2}).astype("Int64")
            print("✅ Coluna 'FATOR_RISC' normalizada (S → 1, N → 2) e convertida para INT.")

        return df

    def executar_normalizacao(self):
        self.df = self.contar_sintomas_fatores()
        self.df = self.classificar_idade(self.df)

        return self.df
    


