import pandas as pd

class Dados_Faltantes:
    def __init__(self,df):
        self.df = df
    
    def excluir_linhas_vazias(self, df, limite_perc=75):
        """
        Remove linhas que possuem mais de um certo percentual de valores ausentes.

        Parâmetros:
        - df (pd.DataFrame): O DataFrame de entrada.
        - limite_perc (float): Percentual máximo permitido de valores ausentes por linha (padrão = 75%).

        Processo:
        1. Calcula o número máximo de valores ausentes permitidos por linha com base no `limite_perc`.
        2. Conta a quantidade de valores ausentes por linha.
        3. Remove as linhas que ultrapassam esse limite.
        4. Retorna um DataFrame limpo e exibe estatísticas do processo.

        Retorna:
        - df_limpo (pd.DataFrame): DataFrame sem as linhas com muitos valores ausentes.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, retorna um aviso e não executa a remoção.
        - Se o `limite_perc` estiver fora da faixa 0-100, exibe um erro e interrompe o processo.
        """

        try:
            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma linha será removida.")
                return df

            # 🚨 Verificação: limite_perc deve estar entre 0 e 100
            if not (0 <= limite_perc <= 100):
                raise ValueError(f"⚠️ Erro: O parâmetro `limite_perc` deve estar entre 0 e 100. Valor recebido: {limite_perc}")

            # 🔹 Definir o número máximo de valores nulos permitidos por linha
            limite_nulos = (limite_perc / 100) * df.shape[1]

            # 🔍 Contagem de valores ausentes antes da remoção
            valores_nulos_por_linha = df.isnull().sum(axis=1)
            print(f"📊 Valores ausentes por linha antes da remoção:\n{valores_nulos_por_linha.describe()}")

            # 🚀 Remover linhas com valores ausentes acima do limite permitido
            df_limpo = df.dropna(thresh=int(df.shape[1] - limite_nulos))

            # 🔹 Estatísticas pós-limpeza
            linhas_removidas = df.shape[0] - df_limpo.shape[0]
            print(f"✅ Total de linhas removidas: {linhas_removidas}")
            print(f"📉 DataFrame final tem {df_limpo.shape[0]} linhas e {df_limpo.shape[1]} colunas.")

            return df_limpo

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df  # Retorna o DataFrame original caso ocorra um erro
    
    def remover_colunas_faltantes(self, df, limite_percentual=90):
        """
        Remove colunas que possuem um percentual de valores faltantes maior ou igual ao limite especificado.

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo os dados.
        - limite_percentual (float): Percentual limite para remoção das colunas (padrão: 90%).

        Processo:
        1. Calcula a porcentagem de valores nulos em cada coluna.
        2. Identifica colunas com valores nulos acima do limite permitido.
        3. Remove essas colunas e exibe um resumo da limpeza.
        4. Retorna um DataFrame atualizado e uma lista das colunas removidas.

        Retorna:
        - df_limpo (pd.DataFrame): DataFrame atualizado sem as colunas que excedem o limite de valores nulos.
        - colunas_removidas (list): Lista com os nomes das colunas removidas.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        - Se `limite_percentual` estiver fora da faixa 0-100, exibe um erro e interrompe o processo.
        """

        try:
            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será removida.")
                return df, []

            # 🚨 Verificação: limite_percentual deve estar entre 0 e 100
            if not (0 <= limite_percentual <= 100):
                raise ValueError(f"⚠️ Erro: O parâmetro `limite_percentual` deve estar entre 0 e 100. Valor recebido: {limite_percentual}")

            # 🔍 Calcula a porcentagem de valores nulos por coluna
            percentual_faltantes = (df.isnull().sum() / len(df)) * 100

            # 📊 Exibir estatísticas antes da remoção
            print(f"📊 Estatísticas dos valores ausentes antes da remoção:\n{percentual_faltantes.describe()}")

            # 🔹 Identifica colunas a serem removidas
            colunas_removidas = percentual_faltantes[percentual_faltantes >= limite_percentual].index.tolist()

            # 🚀 Remover as colunas identificadas
            df_limpo = df.drop(columns=colunas_removidas)

            # 🔹 Estatísticas pós-limpeza
            print(f"✅ {len(colunas_removidas)} colunas removidas ({limite_percentual}% ou mais de valores faltantes).")
            print(f"🗑️ Colunas removidas: {colunas_removidas}")
            print(f"📉 DataFrame final tem {df_limpo.shape[1]} colunas restantes.")

            return df_limpo

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df, []
    
    def remover_colunas_municipio_regional(self, df, termos_exclusao=None):
        """
        Remove colunas que contenham palavras-chave relacionadas a municípios e regionais de saúde,
        mantendo apenas informações gerais como 'estado', 'país' ou 'região de saude'.

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo os dados.
        - termos_exclusao (list, opcional): Lista personalizada de palavras-chave para remoção.
        Se não for informada, uma lista padrão será utilizada.

        Processo:
        1. Define uma lista de palavras-chave para identificar colunas indesejadas.
        2. Filtra as colunas que NÃO contêm os termos de exclusão.
        3. Retorna um DataFrame atualizado e exibe um resumo das colunas removidas.

        Retorna:
        - df_filtrado (pd.DataFrame): DataFrame atualizado sem as colunas indesejadas.
        - colunas_removidas (list): Lista das colunas que foram removidas.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será removida.")
                return df, []

            # Lista padrão de palavras-chave a serem excluídas se nenhuma for fornecida
            if termos_exclusao is None:
                termos_exclusao = [
                    "município", "regional de saúde", "código (ibge)", "residência", 
                    "municipios", "co_mun", "co_rg", "co_mu", "co_regiona", 
                    "id_rg", "id_reg", "id_mn","ID_UNIDADE"
                ]

            # 🔍 Identificar colunas a serem removidas
            colunas_removidas = [col for col in df.columns if any(term in col.lower() for term in termos_exclusao)]
            
            # 🚀 Filtrar colunas que NÃO contenham os termos de exclusão
            colunas_filtradas = [col for col in df.columns if col not in colunas_removidas]
            df_filtrado = df[colunas_filtradas]

            # 📊 Exibir estatísticas pós-limpeza
            print(f"✅ {len(colunas_removidas)} colunas removidas relacionadas a municípios/regionais.")
            print(f"🗑️ Colunas removidas: {colunas_removidas}")
            print(f"📉 DataFrame final tem {df_filtrado.shape[1]} colunas restantes.")

            return df_filtrado

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df, []
    
    def remover_colunas_automaticamente(self, df):
        """
        Remove colunas com base em padrões automaticamente, sem precisar de uma lista pré-definida.

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo os dados.

        Processo:
        1. Identifica colunas que começam com padrões específicos.
        2. Remove essas colunas e exibe um resumo das alterações.
        3. Retorna um DataFrame atualizado sem as colunas indesejadas.

        Retorna:
        - df_filtrado (pd.DataFrame): DataFrame atualizado sem as colunas removidas.
        - colunas_removidas (list): Lista com os nomes das colunas removidas.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será removida.")
                return df, []

            # 🔍 Identificar colunas que começam com os padrões especificados
            padroes_exclusao = (
                "TP_", "SEM_", "COD_", "CO_", "SURTO_", "PAIS_", "LO_", "HISTO_", 
                "DT_ENTUTI", "DT_SAIDUTI", "DT_TOMO", "DT_RAIOX", "DT_PCR", "DT_EVOLUCA", 
                "DT_ANTIVIR", "TP_AMOSTRA", "DT_COLETA", "TP_ANTIVIR", "TP_TES_AN", 
                "DT_CO_SOR", "DT_UT_DOSE", "TP_SOR", "DT_RES", "RAIOX_RES", "DT_NASC", "CRITERIO", "ID_UNIDADE"
            )

            colunas_removidas = [col for col in df.columns if col.startswith(padroes_exclusao)]

            # 🚀 Remover as colunas identificadas
            df_filtrado = df.drop(columns=colunas_removidas, errors="ignore")

            # 📊 Exibir estatísticas pós-limpeza
            print(f"✅ {len(colunas_removidas)} colunas removidas com base em padrões automáticos.")
            print(f"🗑️ Colunas removidas: {colunas_removidas}")
            print(f"📉 DataFrame final tem {df_filtrado.shape[1]} colunas restantes.")

            return df_filtrado

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df, []
    
    def preencher_com_9(self, df):
        """
        Preenche valores nulos (NaN) com 9 nas colunas de sintomas e fatores de risco.

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo os dados.

        Processo:
        1. Verifica se todas as colunas da lista estão no DataFrame.
        2. Substitui valores ausentes (`NaN`) por `9` apenas nas colunas existentes.
        3. Exibe um resumo da operação e retorna o DataFrame atualizado.

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com os valores nulos preenchidos com 9.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        - Se nenhuma das colunas estiver presente, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será preenchida.")
                return df

            # 🔍 Lista das colunas que devem ser preenchidas com 9
            colunas_sintomas = [
                "FEBRE", "TOSSE", "GARGANTA", "DISPNEIA", "DESC_RESP", "SATURACAO",
                "DIARREIA", "VOMITO", "DOR_ABD", "FADIGA", "PERD_OLFT", "PERD_PALA",
                "AVE_SUINO", "CS_RACA", "CARDIOPATI", "ASMA", "RENAL", "OBESIDADE",
                "NEUROLOGIC", "PNEUMOPATI", "IMUNODEPRE", "HEMATOLOGI", "SIND_DOWN",
                "HEPATICA", "DIABETES", "TP_AM_SOR", "OUT_AMOST", "OUTRO_SIN",
                "OUTRO_DES", "MORB_DESC", "OUT_MORBI", "PUERPERA", "POS_PCROUT",
                "PCR_RESUL", "RES_AN", "VACINA", "ANTIVIRAL", "HOSPITAL", "RES_IGG",
                "RES_IGM", "POS_PCRFLU", "TOMO_RES", "PCR_SARS2", "AMOSTRA"
            ]

            # 🔹 Filtrar apenas as colunas que existem no DataFrame
            colunas_existentes = [col for col in colunas_sintomas if col in df.columns]

            if not colunas_existentes:
                print("⚠️ Nenhuma das colunas especificadas está presente no DataFrame.")
                return df

            # 🚀 Preencher valores nulos com 9 nas colunas existentes
            df[colunas_existentes] = df[colunas_existentes].fillna(9)

            # 📊 Exibir estatísticas pós-preenchimento
            print(f"✅ Valores nulos preenchidos com 9 em {len(colunas_existentes)} colunas.")
            print(f"📝 Colunas modificadas: {colunas_existentes}")

            return df

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df
    
    def tratar_dados_faltantes_pais(self, df, valor_exterior="EXTERIOR"):
        """
        Preenche valores nulos em colunas específicas com um valor padrão.

        - 'CS_ZONA': Se nula, é preenchida com '10' (representando EXTERIOR).
        - Outras colunas: São preenchidas com o valor definido em `valor_exterior` (padrão: 'EXTERIOR').

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo os dados.
        - valor_exterior (str): Valor padrão para preenchimento de outras colunas (exceto 'CS_ZONA').

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com os valores preenchidos.
        """

        try:
            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será preenchida.")
                return df

            # 🔍 Colunas a preencher
            colunas_preenchimento = ["SG_UF", "CS_ZONA"]

            # 🔹 Filtrar apenas colunas que existem no DataFrame
            colunas_existentes = [col for col in colunas_preenchimento if col in df.columns]

            if not colunas_existentes:
                print("⚠️ Nenhuma das colunas especificadas está presente no DataFrame.")
                return df

            # 🚀 Preencher valores nulos
            for col in colunas_existentes:
                if col == "CS_ZONA":
                    df[col] = df[col].fillna(10)  # Preencher CS_ZONA com 10 (EXTERIOR)
                    print("✅ Valores nulos na coluna 'CS_ZONA' preenchidos com 10 (EXTERIOR).")
                else:
                    df[col] = df[col].fillna(valor_exterior)
                    print(f"✅ Valores nulos na coluna '{col}' preenchidos com '{valor_exterior}'.")

            # 📊 Exibir estatísticas pós-preenchimento
            print(f"\n📝 Colunas modificadas: {colunas_existentes}")

            return df

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df

    def preencher_com_mediana(self,df, coluna="CS_ESCOL_N"):
        """
        Preenche valores nulos em uma coluna específica com a mediana dos valores numéricos.

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo os dados.
        - coluna (str, opcional): Nome da coluna a ser preenchida com a mediana (padrão: 'CS_ESCOL_N').

        Processo:
        1. Verifica se a coluna existe no DataFrame.
        2. Converte os valores para numérico, transformando erros em NaN.
        3. Calcula a mediana da coluna ignorando valores nulos.
        4. Preenche os valores nulos com a mediana calculada.
        5. Retorna um DataFrame atualizado e exibe um resumo das alterações.

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com os valores nulos preenchidos.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        - Se a coluna não existir, exibe um aviso e retorna sem alteração.
        - Se a coluna não contiver valores válidos para calcular a mediana, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será preenchida.")
                return df

            # 🚨 Verificação: Se a coluna não existe no DataFrame
            if coluna not in df.columns:
                print(f"⚠️ A coluna '{coluna}' não existe no DataFrame.")
                return df

            # 🔹 Converter a coluna para numérico, tratando erros como NaN
            df[coluna] = pd.to_numeric(df[coluna], errors='coerce')

            # 🔍 Calcular a mediana ignorando valores NaN
            mediana_valor = df[coluna].median()

            # 🚨 Verificação: Se não for possível calcular a mediana
            if pd.isna(mediana_valor):
                print(f"⚠️ Não há valores suficientes para calcular a mediana da coluna '{coluna}'.")
                return df

            # 🚀 Preencher valores nulos com a mediana calculada
            df.loc[:, coluna] = df[coluna].fillna(mediana_valor)

            # 📊 Exibir estatísticas pós-preenchimento
            print(f"✅ Coluna '{coluna}' preenchida com a mediana: {mediana_valor}")

            return df

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df

    def preencher_com_moda(self, df, coluna = "DT_DIGITA"):
        """
        Preenche os valores nulos (NaN) de uma coluna com a moda (valor mais frequente).

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo os dados.
        - coluna (str): Nome da coluna a ser preenchida.

        Processo:
        1. Verifica se a coluna existe no DataFrame.
        2. Calcula a moda da coluna ignorando valores nulos.
        3. Preenche os valores nulos com a moda calculada.
        4. Retorna um DataFrame atualizado e exibe um resumo das alterações.

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com os valores nulos preenchidos.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        - Se a coluna não existir, exibe um aviso e retorna sem alteração.
        - Se a coluna não contiver valores válidos para calcular a moda, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será preenchida.")
                return df

            # 🚨 Verificação: Se a coluna não existe no DataFrame
            if coluna not in df.columns:
                print(f"⚠️ A coluna '{coluna}' não existe no DataFrame.")
                return df

            # 🔍 Calcular a moda ignorando valores NaN
            moda_valor = df[coluna].mode().dropna()

            # 🚨 Verificação: Se não há moda disponível
            if moda_valor.empty:
                print(f"⚠️ Não há valores suficientes para calcular a moda da coluna '{coluna}'.")
                return df

            # 🚀 Pega o primeiro valor da moda (caso haja múltiplas modas)
            moda_valor = moda_valor.iloc[0]

            # 🚀 Preencher valores nulos com a moda calculada
            df.loc[:, coluna] = df[coluna].fillna(moda_valor)

            # 📊 Exibir estatísticas pós-preenchimento
            print(f"✅ Coluna '{coluna}' preenchida com a moda: {moda_valor}")

            return df

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df

    def preencher_sg_uf_inte(self ,df):
        """
        Preenche a coluna 'SG_UF_INTE' com base nas seguintes condições:
        1️⃣ Se 'HOSPITAL' for 2 ou 9 e 'SG_UF_INTE' estiver vazio (None ou NaN), preenche com 'NHI'.
        2️⃣ Se 'HOSPITAL' for 1 e 'SG_UF_INTE' estiver vazio, preenche com o valor correspondente de 'SG_UF_NOT'.
        3️⃣ Se 'HOSPITAL' for 9 e 'SG_UF_INTE' não estiver vazio, substitui 'SG_UF_INTE' por 'NHI'.
        4️⃣ Remove as linhas onde 'HOSPITAL' foi convertido para -1 (indicando valores inválidos).

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo as colunas 'SG_UF_INTE', 'HOSPITAL' e 'SG_UF_NOT'.

        Processo:
        1. Converte a coluna 'HOSPITAL' para inteiro, substituindo NaN por -1 (valor inválido).
        2. Aplica regras de preenchimento usando `.loc[]` para otimizar o desempenho.
        3. Exibe estatísticas pós-preenchimento e remove registros inválidos de 'HOSPITAL'.
        4. Retorna o DataFrame atualizado.

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com os valores preenchidos.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        - Se as colunas necessárias não existirem, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será preenchida.")
                return df

            # 🔍 Verificar se as colunas necessárias estão no DataFrame
            colunas_necessarias = ["SG_UF_INTE", "HOSPITAL", "SG_UF_NOT"]
            colunas_existentes = [col for col in colunas_necessarias if col in df.columns]

            if len(colunas_existentes) < 3:
                print(f"⚠️ O DataFrame não contém todas as colunas necessárias: {colunas_necessarias}")
                return df

            # 🚀 Converter a coluna 'HOSPITAL' para inteiro, substituindo NaN por -1 (valor inválido)
            df["HOSPITAL"] = pd.to_numeric(df["HOSPITAL"], errors="coerce").fillna(-1).astype(int)

            # 1️⃣ Se 'HOSPITAL' for 2 ou 9 e 'SG_UF_INTE' estiver vazio, preenche com 'NHI'
            df.loc[df["HOSPITAL"].isin([2, 9]) & df["SG_UF_INTE"].isna(), "SG_UF_INTE"] = "NHI"

            # 2️⃣ Se 'HOSPITAL' for 1 e 'SG_UF_INTE' estiver vazio, preenche com o valor de 'SG_UF_NOT'
            df.loc[(df["HOSPITAL"] == 1) & df["SG_UF_INTE"].isna(), "SG_UF_INTE"] = df["SG_UF_NOT"]

            # 3️⃣ Se 'HOSPITAL' for 9 e 'SG_UF_INTE' NÃO estiver vazio, substitui 'SG_UF_INTE' por 'NHI'
            df.loc[(df["HOSPITAL"] == 9) & df["SG_UF_INTE"].notna(), "SG_UF_INTE"] = "NHI"

            # 🔍 Exibir estatísticas pós-preenchimento
            print("✅ Valores na coluna 'SG_UF_INTE' foram preenchidos conforme as condições.")

            # 4️⃣ Remover registros onde 'HOSPITAL' foi convertido para -1
            df = df.loc[df["HOSPITAL"] != -1]

            # 🔍 Exibir valores nulos restantes na coluna 'SG_UF_INTE'
            valores_nulos = df[df["SG_UF_INTE"].isna()][["HOSPITAL", "SG_UF_INTE", "SG_UF_NOT"]]
            if not valores_nulos.empty:
                print("\n🔍 Valores nulos em 'SG_UF_INTE' APÓS o preenchimento:")
                print(valores_nulos)

            return df

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df

    def preencher_colunas_OUT_MORBI_MORB_DESC(self, df):
        """
        Preenche as colunas 'OUT_MORBI' e 'MORB_DESC' com 9 conforme as condições:
        
        1. Se ambas as colunas estiverem vazias, preenche ambas com 9.
        2. Se 'OUT_MORBI' for 2 e 'MORB_DESC' estiver vazia, preenche 'MORB_DESC' com 9.
        3. Se 'OUT_MORBI' for 1 e 'MORB_DESC' estiver vazia, preenche 'MORB_DESC' com 9.
        4. Se uma das colunas estiver preenchida com 9 e a outra vazia, preenche a vazia com 9.

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo as colunas 'OUT_MORBI' e 'MORB_DESC'.

        Processo:
        1. Verifica se as colunas existem no DataFrame.
        2. Aplica as regras de preenchimento de forma otimizada usando `.loc[]`.
        3. Retorna um DataFrame atualizado e exibe um resumo das alterações.

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com os valores preenchidos.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        - Se as colunas não existirem, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será preenchida.")
                return df

            # 🔍 Verificar se as colunas necessárias estão no DataFrame
            colunas_necessarias = ["OUT_MORBI", "MORB_DESC"]
            colunas_existentes = [col for col in colunas_necessarias if col in df.columns]

            if len(colunas_existentes) < 2:
                print(f"⚠️ O DataFrame não contém todas as colunas necessárias: {colunas_necessarias}")
                return df

            # 🚀 Aplicar regras de preenchimento diretamente usando .loc[] para melhor performance
            
            # 1️⃣ Se ambas as colunas estiverem vazias, preencher ambas com 9
            df.loc[df["OUT_MORBI"].isna() & df["MORB_DESC"].isna(), ["OUT_MORBI", "MORB_DESC"]] = 9

            # 2️⃣ Se 'OUT_MORBI' for 1 ou 2 e 'MORB_DESC' estiver vazia, preencher 'MORB_DESC' com 9
            df.loc[df["OUT_MORBI"].isin([1, 2]) & df["MORB_DESC"].isna(), "MORB_DESC"] = 9

            # 3️⃣ Se 'MORB_DESC' for 9 e 'OUT_MORBI' estiver vazia, preencher 'OUT_MORBI' com 9
            df.loc[df["MORB_DESC"] == 9 & df["OUT_MORBI"].isna(), "OUT_MORBI"] = 9

            # 4️⃣ Se 'OUT_MORBI' for 9 e 'MORB_DESC' estiver vazia, preencher 'MORB_DESC' com 9
            df.loc[df["OUT_MORBI"] == 9 & df["MORB_DESC"].isna(), "MORB_DESC"] = 9

            # 📊 Exibir estatísticas pós-preenchimento
            print("✅ Valores nulos nas colunas 'OUT_MORBI' e 'MORB_DESC' preenchidos conforme as condições.")

            return df

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df

    def tempo_medio_encerramento(self, df):
        """
        Calcula e preenche os valores ausentes na coluna 'DT_ENCERRA' com base na mediana das datas de encerramento.

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo as colunas 'DT_ENCERRA' e 'DT_SIN_PRI'.

        Processo:
        1. Converte as colunas 'DT_ENCERRA' e 'DT_SIN_PRI' para formato de data.
        2. Converte 'DT_ENCERRA' para números inteiros (timestamp) e calcula a mediana.
        3. Converte a mediana de volta para `datetime`.
        4. Preenche valores ausentes em 'DT_ENCERRA' com essa mediana.

        Retorna:
        - pd.DataFrame: DataFrame atualizado com os valores ausentes preenchidos em 'DT_ENCERRA'.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        - Se as colunas necessárias não existirem, exibe um aviso e retorna sem alteração.
        - Se não houver dados suficientes para calcular a mediana, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Criar uma cópia do DataFrame para evitar 'SettingWithCopyWarning'
            df = df.copy()

            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será preenchida.")
                return df

            # 🔍 Verificar se as colunas necessárias existem no DataFrame
            colunas_necessarias = ["DT_ENCERRA", "DT_SIN_PRI"]
            colunas_existentes = [col for col in colunas_necessarias if col in df.columns]

            if len(colunas_existentes) < 2:
                print(f"⚠️ O DataFrame não contém todas as colunas necessárias: {colunas_necessarias}")
                return df

            # 🚀 Converter colunas para datetime, tratando erros
            df["DT_ENCERRA"] = pd.to_datetime(df["DT_ENCERRA"], errors="coerce", dayfirst=True)
            df["DT_SIN_PRI"] = pd.to_datetime(df["DT_SIN_PRI"], errors="coerce", dayfirst=True)

            # 🚀 Converter 'DT_ENCERRA' para timestamp numérico (número de segundos desde 1970)
            df["DT_ENCERRA_INT"] = df["DT_ENCERRA"].astype("int64") // 10**9  # Converter para segundos

            # 🔍 Calcular a mediana em formato numérico
            mediana_data_int = df["DT_ENCERRA_INT"].dropna().median()

            # 🚨 Verificação: Se não há dados suficientes para calcular a mediana
            if pd.isnull(mediana_data_int):
                print("⚠️ Aviso: Não há dados suficientes para calcular a mediana da data de encerramento.")
                df.drop(columns=["DT_ENCERRA_INT"], inplace=True, errors="ignore")
                return df

            # 🚀 Converter de volta para datetime
            mediana_data = pd.to_datetime(mediana_data_int, unit="s")

            # 🚀 Preencher valores ausentes em 'DT_ENCERRA' com a mediana
            df.loc[df["DT_ENCERRA"].isna(), "DT_ENCERRA"] = mediana_data

            # 🔍 Remover a coluna temporária
            df.drop(columns=["DT_ENCERRA_INT"], inplace=True, errors="ignore")

            # 📊 Exibir estatísticas pós-preenchimento
            print(f"✅ Valores ausentes na coluna 'DT_ENCERRA' foram preenchidos com a data da mediana: {mediana_data.strftime('%d/%m/%Y')}")

            return df

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df

    def preencher_dt_interna(self, df):
        """
        Preenche a coluna 'DT_INTERNA' com base nas seguintes condições:
        1️⃣ Se 'HOSPITAL' for 1 e 'DT_INTERNA' estiver vazia, preenche com a mediana de 'DT_SIN_PRI'.
        2️⃣ Se 'HOSPITAL' for 2 ou 9 e 'DT_INTERNA' estiver vazia, preenche com uma data simbólica '01/01/1900'.

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo as colunas 'HOSPITAL', 'DT_INTERNA' e 'DT_SIN_PRI'.

        Processo:
        1. Converte as colunas para formato de data.
        2. Calcula a mediana de 'DT_SIN_PRI'.
        3. Preenche 'DT_INTERNA' com a mediana para 'HOSPITAL' tipo 1.
        4. Preenche 'DT_INTERNA' com '01/01/1900' para 'HOSPITAL' tipo 2 e 9.

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com os valores preenchidos.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        - Se as colunas necessárias não existirem, exibe um aviso e retorna sem alteração.
        - Se não houver dados suficientes para calcular a mediana, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Criar uma cópia do DataFrame para evitar 'SettingWithCopyWarning'
            df = df.copy()

            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será preenchida.")
                return df

            # 🔍 Verificar se as colunas necessárias existem no DataFrame
            colunas_necessarias = ["DT_INTERNA", "DT_SIN_PRI", "HOSPITAL"]
            colunas_existentes = [col for col in colunas_necessarias if col in df.columns]

            if len(colunas_existentes) < 3:
                print(f"⚠️ O DataFrame não contém todas as colunas necessárias: {colunas_necessarias}")
                return df

            # 🚀 Converter colunas para datetime, tratando erros
            df["DT_INTERNA"] = pd.to_datetime(df["DT_INTERNA"], errors="coerce", dayfirst=True)
            df["DT_SIN_PRI"] = pd.to_datetime(df["DT_SIN_PRI"], errors="coerce", dayfirst=True)

            # 🔍 Calcular a mediana de 'DT_SIN_PRI' ignorando valores nulos
            mediana_dt_sin_pri = df["DT_SIN_PRI"].dropna().median()

            # 🚨 Verificação: Se não há dados suficientes para calcular a mediana
            if pd.isnull(mediana_dt_sin_pri):
                print("⚠️ Aviso: Não há dados suficientes para calcular a mediana de 'DT_SIN_PRI'.")
            else:
                # 1️⃣ Preencher valores ausentes em 'DT_INTERNA' para hospitais tipo 1 com a mediana
                df.loc[(df["HOSPITAL"] == 1) & df["DT_INTERNA"].isna(), "DT_INTERNA"] = mediana_dt_sin_pri
                print(f"✅ Valores ausentes em 'DT_INTERNA' para HOSPITAL 1 foram preenchidos com a mediana: {mediana_dt_sin_pri.strftime('%d/%m/%Y')}.")

            # 2️⃣ Preencher valores ausentes em 'DT_INTERNA' para hospitais tipo 2 ou 9 com a data simbólica
            df.loc[(df["HOSPITAL"].isin([2, 9])) & df["DT_INTERNA"].isna(), "DT_INTERNA"] = pd.to_datetime("1900-01-01")

            print(f"✅ Valores ausentes em 'DT_INTERNA' para HOSPITAL 2 e 9 foram preenchidos com a data simbólica: 01/01/1900.")

            return df

        except Exception as e:
            print(f"❌ Erro ao processar datas: {str(e)}")
            return df

    def tratar_nosocomial(self, df):
        """
        Trata os valores ausentes na coluna 'NOSOCOMIAL' com base no 'HOSPITAL':
        1️⃣ Se 'HOSPITAL' for 1, preenche com a moda de 'NOSOCOMIAL'.
        2️⃣ Se 'HOSPITAL' for 2 ou 9, preenche com 9 (Ignorado).
        3️⃣ Caso contrário, mantém os valores originais.

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo as colunas 'HOSPITAL' e 'NOSOCOMIAL'.

        Processo:
        1. Converte a coluna 'HOSPITAL' para inteiro, tratando valores nulos como -1.
        2. Calcula a moda da coluna 'NOSOCOMIAL'.
        3. Preenche valores ausentes em 'NOSOCOMIAL' de acordo com as regras.

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com os valores preenchidos.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        - Se as colunas necessárias não existirem, exibe um aviso e retorna sem alteração.
        - Se não houver moda disponível para 'NOSOCOMIAL', usa o valor 9 como fallback.
        """

        try:
            # 🚨 Criar uma cópia do DataFrame para evitar 'SettingWithCopyWarning'
            df = df.copy()

            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será preenchida.")
                return df

            # 🔍 Verificar se as colunas necessárias existem no DataFrame
            colunas_necessarias = ["NOSOCOMIAL", "HOSPITAL"]
            colunas_existentes = [col for col in colunas_necessarias if col in df.columns]

            if len(colunas_existentes) < 2:
                print(f"⚠️ O DataFrame não contém todas as colunas necessárias: {colunas_necessarias}")
                return df

            # 🚀 Converter a coluna 'HOSPITAL' para inteiro, tratando valores nulos como -1
            df["HOSPITAL"] = pd.to_numeric(df["HOSPITAL"], errors="coerce").fillna(-1).astype(int)

            # 🔍 Calcular a moda da coluna 'NOSOCOMIAL', ignorando valores nulos
            moda_nosocomial = df["NOSOCOMIAL"].mode().dropna()
            moda_nosocomial = moda_nosocomial.iloc[0] if not moda_nosocomial.empty else 9

            # 1️⃣ Preencher valores ausentes em 'NOSOCOMIAL' para hospitais tipo 1 com a moda
            df.loc[(df["HOSPITAL"] == 1) & df["NOSOCOMIAL"].isna(), "NOSOCOMIAL"] = moda_nosocomial

            # 2️⃣ Preencher valores ausentes em 'NOSOCOMIAL' para hospitais tipo 2 ou 9 com 9 (Ignorado)
            df.loc[(df["HOSPITAL"].isin([2, 9])) & df["NOSOCOMIAL"].isna(), "NOSOCOMIAL"] = 9

            # 📊 Exibir estatísticas pós-preenchimento
            print(f"✅ Valores ausentes preenchidos conforme as condições.")
            print(f"   - HOSPITAL 1 -> preenchido com moda ({moda_nosocomial}) de 'NOSOCOMIAL'.")
            print(f"   - HOSPITAL 2 e 9 -> preenchido com 9 (Ignorado).")

            return df

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df

    def preencher_uti_suporte_ven(self,df):
        """
        Preenche as colunas 'UTI' e 'SUPORT_VEN' com base nos sintomas e fatores de risco.

        1️⃣ Se DISPNEIA = 1 ou SATURACAO = 1 -> UTI = 1 (Sim)
        2️⃣ Se ASMA = 1 ou DIABETES = 1 ou OBESIDADE = 1 -> SUPORT_VEN = 1 (Invasivo)
        3️⃣ Se tem fatores de risco mas não sintomas graves -> SUPORT_VEN = 2 (Não invasivo)
        4️⃣ Caso contrário, preenche UTI e SUPORT_VEN com 9 (Ignorado)

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo as colunas necessárias.

        Processo:
        1. Converte todas as colunas relevantes para numérico.
        2. Aplica as regras para preenchimento de 'UTI' e 'SUPORT_VEN'.
        3. Preenche os valores ausentes com 9.

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com as colunas 'UTI' e 'SUPORT_VEN' preenchidas.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        - Se as colunas necessárias não existirem, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Criar uma cópia do DataFrame para evitar 'SettingWithCopyWarning'
            df = df.copy()

            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será preenchida.")
                return df

            # 🔍 Verificar se as colunas necessárias existem no DataFrame
            colunas_necessarias = ["UTI", "SUPORT_VEN", "DISPNEIA", "SATURACAO", "ASMA", "DIABETES", "OBESIDADE"]
            colunas_faltantes = [col for col in colunas_necessarias if col not in df.columns]

            if colunas_faltantes:
                print(f"⚠️ As colunas ausentes no DataFrame: {colunas_faltantes}")
                return df

            # 🚀 Converter todas as colunas relevantes para numérico, tratando erros
            df[colunas_necessarias] = df[colunas_necessarias].apply(pd.to_numeric, errors="coerce")

            # 1️⃣ Criar máscaras para as condições
            mask_uti = (df["DISPNEIA"] == 1) | (df["SATURACAO"] == 1)
            mask_suporte_1 = (df["ASMA"] == 1) | (df["DIABETES"] == 1) | (df["OBESIDADE"] == 1)
            mask_suporte_2 = mask_suporte_1 & ~mask_uti  # Se tiver fatores de risco mas não sintomas graves

            # 🚀 Aplicar preenchimento usando .loc[]
            df.loc[mask_uti, "UTI"] = 1
            df.loc[mask_suporte_1, "SUPORT_VEN"] = 1
            df.loc[mask_suporte_2, "SUPORT_VEN"] = 2

            # 2️⃣ Preencher valores ausentes com 9 usando .loc[]
            df.loc[df["UTI"].isna(), "UTI"] = 9
            df.loc[df["SUPORT_VEN"].isna(), "SUPORT_VEN"] = 9

            # 📊 Exibir estatísticas pós-preenchimento
            print("✅ Valores ausentes em 'UTI' e 'SUPORT_VEN' foram preenchidos conforme as condições.")
            print(f"   - Casos preenchidos em 'UTI' com 1: {mask_uti.sum()}")
            print(f"   - Casos preenchidos em 'SUPORT_VEN' com 1 (Invasivo): {mask_suporte_1.sum()}")
            print(f"   - Casos preenchidos em 'SUPORT_VEN' com 2 (Não invasivo): {mask_suporte_2.sum()}")

            return df

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df

    def tratar_classi_e_evolucao(self, df):
        """
        Trata os valores ausentes nas colunas 'CLASSI_FIN' e 'EVOLUCAO' com base nas regras definidas:

        1️⃣ Se 'CLASSI_FIN' for 5 (Covid-19) e 'EVOLUCAO' estiver vazio, preenche com a moda de 'EVOLUCAO' para Covid-19.
        2️⃣ Se 'EVOLUCAO' for NaN e 'CLASSI_FIN' não indicar Covid, preenche com 9 (Ignorado).
        3️⃣ Se 'CLASSI_FIN' for NaN, preenche com a moda de casos semelhantes (baseado na 'EVOLUCAO').
        4️⃣ Converte valores inválidos para NaN e trata corretamente.

        Parâmetros:
        - df (pd.DataFrame): DataFrame contendo as colunas 'CLASSI_FIN' e 'EVOLUCAO'.

        Processo:
        1. Verifica se as colunas existem.
        2. Converte as colunas para numérico.
        3. Substitui valores inválidos por NaN.
        4. Aplica as regras de preenchimento conforme as condições.

        Retorna:
        - df (pd.DataFrame): DataFrame atualizado com valores preenchidos corretamente.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        - Se as colunas necessárias não existirem, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Criar uma cópia do DataFrame para evitar 'SettingWithCopyWarning'
            df = df.copy()

            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma coluna será preenchida.")
                return df

            # 🔍 Verificar se as colunas necessárias existem no DataFrame
            colunas_necessarias = ["CLASSI_FIN", "EVOLUCAO"]
            colunas_faltantes = [col for col in colunas_necessarias if col not in df.columns]

            if colunas_faltantes:
                print(f"⚠️ As colunas ausentes no DataFrame: {colunas_faltantes}")
                return df

            # 🚀 Converter as colunas para numérico, tratando erros
            df[colunas_necessarias] = df[colunas_necessarias].apply(pd.to_numeric, errors="coerce")

            # 🚀 Garantir que CLASSI_FIN tenha apenas valores válidos (1 a 5), substituindo valores inválidos por NaN
            df.loc[~df["CLASSI_FIN"].isin([1, 2, 3, 4, 5]), "CLASSI_FIN"] = pd.NA

            # 🚀 Garantir que EVOLUCAO tenha apenas valores válidos (1, 2, 3, 9), substituindo valores inválidos por NaN
            df.loc[~df["EVOLUCAO"].isin([1, 2, 3, 9]), "EVOLUCAO"] = pd.NA

            # 🔍 Calcular a moda da coluna EVOLUCAO para CLASSI_FIN = 5 (Covid-19)
            moda_covid = df.loc[df["CLASSI_FIN"] == 5, "EVOLUCAO"].mode().dropna()
            moda_covid = moda_covid.iloc[0] if not moda_covid.empty else 9

            # 🚀 Preencher valores ausentes em EVOLUCAO para CLASSI_FIN = 5 com a moda
            df.loc[(df["CLASSI_FIN"] == 5) & df["EVOLUCAO"].isna(), "EVOLUCAO"] = moda_covid

            # 🚀 Preencher valores ausentes em EVOLUCAO com 9 (Ignorado) se CLASSI_FIN não for Covid-19
            df.loc[df["EVOLUCAO"].isna(), "EVOLUCAO"] = 9

            # 🔍 Calcular a moda da coluna CLASSI_FIN
            moda_classi = df["CLASSI_FIN"].mode().dropna()
            moda_classi = moda_classi.iloc[0] if not moda_classi.empty else 4  # Se não houver moda, usar 4 (SRAG não especificado)

            # 🚀 Preencher valores ausentes em CLASSI_FIN com a moda dos casos semelhantes
            df.loc[df["CLASSI_FIN"].isna(), "CLASSI_FIN"] = moda_classi

            # 📊 Exibir estatísticas pós-preenchimento
            print("✅ Valores ausentes em 'CLASSI_FIN' e 'EVOLUCAO' foram preenchidos conforme as condições.")
            print(f"   - Moda de 'EVOLUCAO' para Covid-19: {moda_covid}")
            print(f"   - Valores ausentes em 'EVOLUCAO' preenchidos com 9 (Ignorado)")
            print(f"   - Moda de 'CLASSI_FIN' usada para preenchimento: {moda_classi}")

            return df

        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            return df

    def dados_faltantes(self, df):
        """
        Analisa e exibe informações sobre valores ausentes no DataFrame.

        Funcionalidades:
        - Conta valores ausentes por coluna.
        - Calcula o percentual de valores ausentes.
        - Ordena do maior para o menor número de valores ausentes.
        - Exibe um resumo formatado das colunas com dados faltantes.

        Parâmetros:
        - df (pd.DataFrame): O DataFrame a ser analisado.

        Retorna:
        - df (pd.DataFrame): O mesmo DataFrame original, sem modificações.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um aviso e retorna sem alteração.
        """

        try:
            # 🚨 Verificação: DataFrame vazio
            if df.empty:
                print("⚠️ O DataFrame está vazio. Nenhuma análise foi realizada.")
                return df

            # 🚀 Contar valores ausentes
            missing_count = df.isnull().sum()

            # 🚀 Calcular percentual de valores ausentes
            missing_percentage = (missing_count / len(df)) * 100

            # 🚀 Criar um DataFrame com os resultados
            missing_data = pd.DataFrame({
                "Total Faltantes": missing_count,
                "Percentual (%)": missing_percentage
            })

            # 🚀 Filtrar apenas colunas com valores ausentes e ordenar do maior para o menor
            missing_data = missing_data[missing_data["Total Faltantes"] > 0].sort_values(by="Total Faltantes", ascending=False)

            # 🚀 Contar o número total de colunas com valores ausentes
            total_missing_columns = missing_data.shape[0]

            # 📊 Exibir os resultados formatados
            print("\n🔍 **Relatório de Valores Faltantes**")
            print("-" * 50)

            if total_missing_columns == 0:
                print("✅ Nenhuma coluna contém valores faltantes.")
            else:
                print(f"⚠️ Total de colunas com dados faltantes: {total_missing_columns}\n")
                print(missing_data.to_string(index=True))  # Exibir DataFrame sem usar display()

            print("\n✅ Análise concluída!\n")
            return df

        except Exception as e:
            print(f"❌ Erro ao analisar valores faltantes: {str(e)}")
            return df

    def executar_tratamento_de_dados_faltantes(self):
        """
        Executa o pipeline de tratamento de dados faltantes no DataFrame `self.df`.

        Etapas do Processo:
        1️⃣ Identifica e exibe colunas com dados faltantes.
        2️⃣ Exclui linhas com alto percentual de valores nulos.
        3️⃣ Remove colunas com valores nulos acima do limite definido.
        4️⃣ Remove colunas relacionadas a município e regiões.
        5️⃣ Exclui colunas desnecessárias automaticamente.
        6️⃣ Preenche valores nulos com 9 para colunas categóricas.
        7️⃣ Preenche dados faltantes de país e estados com valores padrão.
        8️⃣ Preenche valores numéricos ausentes com mediana.
        9️⃣ Preenche valores categóricos ausentes com a moda.
        🔟 Ajusta valores nulos em colunas relacionadas a internação e tratamento hospitalar.
        11️⃣ Calcula e preenche datas de encerramento e internação com base na mediana.
        12️⃣ Ajusta valores nulos para colunas de internação e suporte hospitalar.
        13️⃣ Normaliza dados relacionados a fatores de risco e evolução do caso.
        14️⃣ Exibe novamente os dados faltantes após o tratamento.

        Tratamento de Erros:
        - Se uma etapa falhar, um aviso será exibido e o processo continuará.
        - Logs detalhados ajudam na depuração.

        Retorna:
        - None. O DataFrame `self.df` é atualizado diretamente.
        """

        print("🔄 Iniciando o tratamento de dados faltantes...")

        try:
            print("📊 Passo 1: Identificando dados faltantes...")
            passo1 = self.dados_faltantes(self.df)

            print("🗑️ Passo 2: Excluindo linhas vazias...")
            self.df = self.excluir_linhas_vazias(self.df)

            print("📉 Passo 3: Removendo colunas com muitos valores faltantes...")
            self.df = self.remover_colunas_faltantes(self.df)

            print("🌍 Passo 4: Removendo colunas de municípios e regiões...")
            self.df = self.remover_colunas_municipio_regional(self.df)

            print("⚙️ Passo 5: Removendo colunas desnecessárias automaticamente...")
            self.df = self.remover_colunas_automaticamente(self.df)

            print("9️⃣ Passo 6: Preenchendo valores nulos com 9 para colunas categóricas...")
            self.df = self.preencher_com_9(self.df)

            print("🏛️ Passo 7: Tratando dados faltantes de país e estados...")
            self.df = self.tratar_dados_faltantes_pais(self.df)

            print("📊 Passo 8: Preenchendo valores numéricos ausentes com mediana...")
            self.df = self.preencher_com_mediana(self.df)

            print("📈 Passo 9: Preenchendo valores categóricos ausentes com moda...")
            self.df = self.preencher_com_moda(self.df)

            print("🗺️ Passo 10: Preenchendo SG_UF_INTE...")
            self.df = self.preencher_sg_uf_inte(self.df)

            print("⚕️ Passo 11: Tratando colunas OUT_MORBI e MORB_DESC...")
            self.df = self.preencher_colunas_OUT_MORBI_MORB_DESC(self.df)

            print("📅 Passo 12: Calculando e preenchendo datas de encerramento...")
            self.df = self.tempo_medio_encerramento(self.df)

            print("🏥 Passo 13: Preenchendo datas de internação...")
            self.df = self.preencher_dt_interna(self.df)

            print("🦠 Passo 14: Tratando dados de infecção nosocomial...")
            self.df = self.tratar_nosocomial(self.df)

            print("💨 Passo 15: Preenchendo colunas UTI e Suporte Ventilatório...")
            self.df = self.preencher_uti_suporte_ven(self.df)

            print("🔬 Passo 16: Tratando dados de classificação e evolução do caso...")
            self.df = self.tratar_classi_e_evolucao(self.df)

            print("📊 Passo 17: Exibindo resumo dos dados faltantes após o tratamento...")
            passo17 = self.dados_faltantes(self.df)

            print("✅ Tratamento de dados faltantes concluído com sucesso!")
            
            return self.df


        except Exception as e:
            print(f"❌ Erro durante o tratamento de dados: {str(e)}")






