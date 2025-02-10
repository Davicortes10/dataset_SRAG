import pandas as pd

class Classificacao:
    def __init__(self, df):
        self.df = df

    def classificar_pacientes(self):
        """
        Classifica os pacientes com Síndrome Respiratória Aguda Grave (SRAG) em LEVE, MODERADO ou GRAVE 
        com base em sintomas, fatores de risco e idade.

        Parâmetros:
        - self.df (pd.DataFrame): DataFrame contendo os dados clínicos dos pacientes.

        Retorna:
        - pd.DataFrame: DataFrame atualizado com a coluna "CLASSIFICACAO", indicando a gravidade da SRAG.

        Regras de Classificação:
        ------------------------
        LEVE:
            - Padrão inicial para todos os pacientes.

        MODERADO:
            - Dispneia (falta de ar) presente (DISPNEIA == 1).
            - Idade entre 49 e 59 anos (NU_IDADE_N entre 49 e 59).
            - Desconforto respiratório (DESC_RESP == 1).
            - Saturação baixa (SATURACAO == 1).
            - Presença de sintomas adicionais (fadiga, dor abdominal, vômito, diarreia, dor de garganta, febre, tosse).
            - Perda de olfato ou paladar (PERD_OLFT == 1 ou PERD_PALA == 1).

        GRAVE:
            - Saturação baixa (SATURACAO == 1).
            - Idade maior ou igual a 60 anos (NU_IDADE_N >= 60).
            - Presença de comorbidades graves (cardiopatia, diabetes, obesidade, insuficiência renal, 
            doença hepática, neurológica, imunodepressão, pneumopatia, síndrome de Down, doenças hematológicas).
        """

        print("🔍 Iniciando classificação dos pacientes...")

        # 🚀 Criar a coluna CLASSIFICACAO inicializando como "LEVE" para todos os pacientes
        self.df["CLASSIFICACAO"] = "LEVE"
        print("✅ Padrão inicial 'LEVE' atribuído a todos os pacientes.")

        # 📌 Definir condições para MODERADO:
        moderado_condicoes = (
            (self.df["DISPNEIA"] == 1) |
            (self.df["NU_IDADE_N"].between(49, 59)) |
            (self.df["DESC_RESP"] == 1) |
            (self.df["SATURACAO"] == 1) |  # Saturação considerada baixa
            (self.df["FADIGA"] == 1) |
            (self.df["DOR_ABD"] == 1) |
            (self.df["VOMITO"] == 1) |
            (self.df["DIARREIA"] == 1) |
            (self.df["GARGANTA"] == 1) |
            (self.df["FEBRE"] == 1) |
            (self.df["TOSSE"] == 1) |
            (self.df["PERD_OLFT"] == 1) | 
            (self.df["PERD_PALA"] == 1)
        )
        
        self.df.loc[moderado_condicoes, "CLASSIFICACAO"] = "MODERADO"
        print(f"⚠️ {moderado_condicoes.sum()} pacientes classificados como 'MODERADO'.")

        # 📌 Definir condições para GRAVE:
        grave_condicoes = (
            (self.df["SATURACAO"] == 1) |  # Saturação crítica
            (self.df["NU_IDADE_N"] >= 60) |
            (self.df["CARDIOPATI"] == 1) | 
            (self.df["DIABETES"] == 1) | 
            (self.df["OBESIDADE"] == 1) |
            (self.df["RENAL"] == 1) |
            (self.df["HEPATICA"] == 1) |
            (self.df["NEUROLOGIC"] == 1) |
            (self.df["IMUNODEPRE"] == 1) |
            (self.df["PNEUMOPATI"] == 1) |
            (self.df["SIND_DOWN"] == 1) |
            (self.df["HEMATOLOGI"] == 1)
        )

        self.df.loc[grave_condicoes, "CLASSIFICACAO"] = "GRAVE"
        print(f"🚨 {grave_condicoes.sum()} pacientes classificados como 'GRAVE'.")

        print("✅ Classificação de pacientes concluída com sucesso!")
        
        return self.df

