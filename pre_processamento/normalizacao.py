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
    
    def executar_normalizacao(self):
        self.df = self.contar_sintomas_fatores()
        self.df = 
    


