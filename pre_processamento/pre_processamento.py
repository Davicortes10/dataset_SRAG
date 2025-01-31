from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


class PreprocessDataset:
    def __init__(self):
        self.gcp_db_connection = "mysql+pymysql://devdavi:12345678@34.170.252.6/srag_datalake"
        self.conn = create_engine(self.gcp_db_connection)
        self.df = self.ler_gcp_DB()

    def ler_gcp_DB(self):
        # Escrevendo a consulta SQL para ler os dados da tabela
        query = "SELECT * FROM srag_datalake"
        pd.set_option("display.max_columns", None)
        # Lendo os dados para um DataFrame Pandas
        df = pd.read_sql(query, con=self.conn)
        # Exibir informações iniciais sobre os dados
        print("Primeiros registros:")
        print(df.head())  # Visualizar os primeiros registros
        print("\nInformações da tabela:")
        print(df.info())  # Tipos de dados e valores ausentes
        print("\nResumo estatístico:")
        print(df.describe(include="all"))  # Resumo para valores numéricos e categóricos
        return df

    def remove_columns(self):
        """
        Remove colunas específicas de um DataFrame pandas.
        """
        columns_to_remove = ["TP_IDADE", "SEM_NOT", "SEM_PRI", "COD_IDADE", "CO_MUN_RES", "SURTO",
                             "CO_RG_INTE", "CO_REGIONA", "CO_MU_INTE", "HISTO_VGM", "PCR_SARS2", "PAC_COCBO", "CO_MU_NOT", "CO_UNI_NOT", "CO_PAIS", "COD_RG_RESI",
                             "SURTO_SG", "PAIS_VGM", "CO_VGM", "LO_PS_VGM"]
        # Filtra apenas as colunas que existem no DataFrame
        existing_columns = [col for col in columns_to_remove if col in self.df.columns]
        self.df = self.df.drop(columns=existing_columns)
        print("As colunas foram removidas com sucesso.")

    def processar_colunas_data(self):
        # Colunas do tipo 'data' (começam com 'DT')
        date_columns = [col for col in self.df.columns if col.startswith("DT")]

        # Conversão das colunas de data
        for col in date_columns:
            try:
                self.df[col] = pd.to_datetime(self.df[col], errors="coerce",dayfirst=True)
                print(f"Coluna '{col}' convertida para datetime.")
            except Exception as e:
                print(f"Erro ao converter coluna '{col}': {e}")

        print("Conversão de tipos finalizada.")
        print(self.df[date_columns].info())

    def tem_texto(series):
        """Verifica se a coluna contém qualquer caractere não numérico (excluindo espaços)."""
        return series.astype(str).str.strip().str.contains(r"[a-zA-Z]", regex=True, na=False).any()


    def processar_colunas_numericas(self):
        """Converte colunas para numérico, ignorando NaN e texto."""
        for col in self.df.select_dtypes(include=['object']).columns:
            # Remove espaços e verifica se há texto
            self.df[col] = self.df[col].astype(str).str.strip()

            if not self.df[col].str.contains(r"[a-zA-Z]", regex=True, na=False).any():
                self.df[col] = self.df[col].apply(lambda x: pd.to_numeric(x, errors="coerce") if pd.notna(x) else x)
                print(f"🔢 Coluna '{col}' convertida para numérico (NaNs preservados).")
            else:
                print(f"🔤 Coluna '{col}' contém texto e foi mantida como string.")

    def processar_colunas_texto(self):

        # Converte as colunas para texto (string), ou para categoria se necessário
        for col in self.df.select_dtypes(include=['object']).columns:
            try:
                # Verifica se o tamanho único de valores na coluna é grande o suficiente para considerar como categoria
                unique_vals = self.df[col].dropna().unique()
                if unique_vals < 0.1 * len(self.df):
                    self.df[col] = self.df[col].astype('category')  # Coluna com poucas categorias
                    print(f"Coluna '{col}' convertida para 'category'.")
                else:
                    self.df[col] = self.df[col].astype(str)  # Caso contrário, converte para string
                    print(f"Coluna '{col}' convertida para string.")
            except Exception as e:
                print(f"Erro ao converter coluna '{col}': {e}")



    def tratar_dados_faltantes_pais(df, valor_exterior="EXTERIOR"):
        """
        Preenche valores nulos nas colunas especificadas com 'EXTERIOR'.
    
        Parâmetros:
        - df (DataFrame): O DataFrame Pandas contendo os dados.
        - colunas (list): Lista de colunas onde os valores nulos serão preenchidos.
        - valor_exterior (str): O valor a ser preenchido (padrão: 'EXTERIOR').
    
        Retorna:
        - df (DataFrame): O DataFrame atualizado.
        """
        colunas = ["SG_UF",
        "ID_RG_RESI",
        "ID_MN_RESI",
        "CS_ZONA"]
        # Verifica se todas as colunas existem no DataFrame
        colunas_faltantes = [col for col in colunas if col not in df.columns]
        if colunas_faltantes:
            raise ValueError(f"As colunas {colunas_faltantes} não existem no DataFrame.")
    
        # Preencher os valores nulos com "EXTERIOR"
        for col in colunas:
            df.loc[df[col].isnull(), col] = valor_exterior
            print(f"Valores nulos preenchidos na coluna {col} com '{valor_exterior}'.")
    
        return df  # Retorna o DataFrame atualizado


    def preencher_data_nascimento(self):
        """
        Preenche valores ausentes na coluna 'DT_NASC' com base na idade da pessoa.
        A idade é subtraída da data atual para estimar o ano de nascimento.
        """
        # Obtém a data atual
        data_atual = datetime.today()

        def calcular_data_nascimento(idade):
            """ Retorna a data de nascimento estimada com base na idade."""
            if pd.notna(idade):  # Se a idade não for NaN
                ano_nascimento = data_atual.year - int(idade)
                return datetime(ano_nascimento, 1, 1)  # Assume o primeiro dia do ano
            return None

        antes = self.df["DT_NASC"].isna().sum()
        # Aplica a função apenas onde a data de nascimento está ausente (NaN)
        self.df["DT_NASC"] = self.df.apply(
            lambda row: calcular_data_nascimento(row["NU_IDADE_N"]) if pd.isna(row["DT_NASC"]) else row["DT_NASC"],
            axis=1
        )
        # Conta quantas foram preenchidas
        depois = self.df["DT_NASC"].isna().sum()
        preenchidos = antes - depois

        print(f"✅ {preenchidos} registros de data de nascimento foram preenchidos!")

        print("✅ Dados de nascimento preenchidos com sucesso!")

    def preencher_com_moda(df, coluna = "DT_DIGITA"):
        """
        Preenche os valores nulos (NaN) de uma coluna com a moda (valor mais frequente).
    
        Parâmetros:
        - df (pd.DataFrame): DataFrame Pandas contendo os dados.
        - coluna (str): Nome da coluna a ser preenchida.
    
        Retorna:
        - pd.DataFrame: DataFrame atualizado com os valores nulos preenchidos.
        """
        if coluna not in df.columns:
            raise ValueError(f"A coluna '{coluna}' não existe no DataFrame.")
    
        # Encontrar a moda (o valor mais frequente)
        moda_valor = df[coluna].mode()[0] if not df[coluna].mode().empty else None
    
        # Preencher os valores nulos com a moda
        if moda_valor is not None:
            df[coluna].fillna(moda_valor, inplace=True)
            print(f"Valores nulos na coluna '{coluna}' foram preenchidos com a moda: {moda_valor}.")
        else:
            print(f"Não foi possível encontrar a moda para a coluna '{coluna}'.")
    
        return df

    def preencher_sintomas_com_9(df):
        """
        Preenche valores nulos (NaN) nas colunas de sintomas com 9.
    
        Parâmetros:
        - df (pd.DataFrame): DataFrame com os dados.
        - colunas_sintomas (list): Lista de colunas de sintomas a serem preenchidas.
    
        Retorna:
        - df atualizado com os valores nulos preenchidos com 9.
        """
        colunas_sintomas = ["FEBRE",
    "TOSSE","GARGANTA", "DISPNEIA", "DESC_RESP", "SATURACAO", "DIARREIA","VOMITO", "DOR_ABD", "FADIGA", "PERD_OLFT", "PERD_PALA"]
        df[colunas_sintomas] = df[colunas_sintomas].fillna(9)
        print(f"Valores nulos preenchidos com 9 nas colunas: {', '.join(colunas_sintomas)}")
        return df

    def preencher_amostra_com_9(self):
        """
               Preenche os valores ausentes na coluna 'AMOSTRA' com '9',
               define 'TP_AMOSTRA' como '9' e 'OUT_AMOST' como 'IGNORADO' para esses registros.
               """

        # Removendo espaços extras da coluna AMOSTRA
        self.df["AMOSTRA"] = self.df["AMOSTRA"].astype(str).str.strip()

        # Condição para identificar valores vazios ou nulos corretamente
        condicao = self.df["AMOSTRA"].isna() | (self.df["AMOSTRA"] == "") | (self.df["AMOSTRA"].str.lower() == "nan") | (
                    self.df["AMOSTRA"].str.lower() == "none")

        # Contar quantos registros estão vazios antes da modificação
        registros_vazios = condicao.sum()
        print(f"🔍 {registros_vazios} registros possuem 'AMOSTRA' vazia ou nula antes da correção.")

        # Preencher as colunas desejadas nos registros filtrados
        self.df.loc[condicao, ["AMOSTRA", "TP_AMOSTRA"]] = "9"
        self.df.loc[condicao, "OUT_AMOST"] = "IGNORADO"

        # Contar quantos ainda estão vazios depois da conversão
        depois = self.df["AMOSTRA"].isna().sum() + (self.df["AMOSTRA"] == "").sum()
        preenchidos = registros_vazios - depois

        print(f"✅ {preenchidos} registros foram preenchidos com '9' e 'IGNORADO'!")




    def executar_pipeline(self):
        """
        Executa todos os passos da pipeline em sequência.
        """
        print("Iniciando o processo de remoção de colunas...")
        self.remove_columns()
        print("\nIniciando o processo de conversão de tipos...")
        self.processar_colunas_data()
       # self.preencher_data_nascimento()
        self.preencher_amostra_com_9()

        print(self.df.info())
        print("\nPipeline executada com sucesso.")





