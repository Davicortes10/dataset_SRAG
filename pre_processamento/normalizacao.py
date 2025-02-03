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
        - df (pd.DataFrame): DataFrame contendo as colunas de sintomas e fatores de risco.

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com as colunas 'QTD_SINT' e 'QTD_FATOR_RISC'.
        """

        # Definir as colunas que representam sintomas e fatores de risco
        colunas_sintomas = ["FEBRE", "TOSSE", "GARGANTA", "DISPNEIA", "DESC_RESP", "SATURACAO",
                            "DIARREIA", "VOMITO", "DOR_ABD", "FADIGA", "PERD_OLFT", "PERD_PALA"]
        
        colunas_fatores_risco = ["CARDIOPATI", "ASMA", "RENAL", "OBESIDADE", "NEUROLOGIC",
                                "PNEUMOPATI", "IMUNODEPRE", "HEMATOLOGI", "SIND_DOWN",
                                "HEPATICA", "DIABETES", "PUERPERA"]

        # Converter os valores para numérico (evita erros com strings)
        for col in colunas_sintomas + colunas_fatores_risco:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

        # Contar quantidade de sintomas presentes (valores 1)
        self.df["QTD_SINT"] = self.df[colunas_sintomas].apply(lambda row: (row == 1).sum(), axis=1)

        # Contar quantidade de fatores de risco presentes (valores 1)
        self.df["QTD_FATOR_RISC"] = self.df[colunas_fatores_risco].apply(lambda row: (row == 1).sum(), axis=1)

        print("✅ Contagem de sintomas e fatores de risco concluída.")
        return self.df

   def classificar_idade(self, df):
    """
    Classifica os indivíduos de acordo com a idade na coluna 'NU_IDADE_N' e cria uma nova coluna 'CLASSIF_IDADE'.

    Faixas etárias:
    - 0 a 12 anos  → 'Criança'
    - 13 a 17 anos → 'Adolescente'
    - 18 a 59 anos → 'Adulto'
    - 60+ anos     → 'Idoso'
    - Idade inválida (NaN ou < 0) → 'Desconhecido'

    Parâmetros:
    - df (pd.DataFrame): DataFrame Pandas contendo a coluna 'NU_IDADE_N'.

    Retorna:
    - df (pd.DataFrame): DataFrame atualizado com a nova coluna 'CLASSIF_IDADE'.
    """

    # Garantir que a coluna seja numérica
    df["NU_IDADE_N"] = pd.to_numeric(df["NU_IDADE_N"], errors="coerce")

    # Criar a nova coluna CLASSIF_IDADE com base na idade
    df["CLASSIF_IDADE"] = df["NU_IDADE_N"].apply(lambda x: 
        "Criança" if 0 <= x <= 12 else
        "Adolescente" if 13 <= x <= 17 else
        "Adulto" if 18 <= x <= 59 else
        "Idoso" if x >= 60 else
        "Desconhecido"
    )

    print("✅ Classificação de idade concluída!")
    return df
    
    def executar_normalizacao(self):
        self.df = self.contar_sintomas_fatores()
        self.df = 
    


