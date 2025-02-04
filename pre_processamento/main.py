from pre_processamento import PreprocessDataset
from data_lake import Data_Lake
from dados_faltantes import Dados_Faltantes
from gcp_dataset import GCP_Dataset
from pre_processamento import PreprocessDataset
from outliers import Outliers
from normalizacao import Normalizacao
from sqlalchemy import create_engine

class Oficial:
    """
    Classe principal para gerenciar o processo de ETL (Extração, Tratamento e Carregamento de dados).
    
    Etapas:
    1️⃣ Lê os dados do GCP através da classe `GCP_Dataset`.
    2️⃣ Trata os dados faltantes usando a classe `Dados_Faltantes`.
    """

    def __init__(self):
        self.df = None
        self.bot_passo_1 = Data_Lake("/home/davicortes_oliveira1/dataset_SRAG/pre_processamento/srag.csv","srag_datalake")  # Inicializa sem valor 
    
    def data_lake(self):
        self.bot_passo_1.executar_datalake()

    def ler_dataset(self):
        """
        Lê os dados do banco GCP e armazena no atributo `self.df`.
        Após a leitura, inicializa `bot_passo_2` com `Dados_Faltantes(self.df)`.
        """
        print("📥 Lendo o dataset do GCP...")
        gcp = GCP_Dataset()
        self.df = gcp.ler_gcp_DB()

        if self.df is None or self.df.empty:
            print("❌ Erro: O dataset não foi carregado corretamente!")
        else:
            print("✅ Dataset carregado com sucesso!")
            bot = Normalizacao(self.df)
            dados_faltantes = Dados_Faltantes(self.df)  # Agora pode ser inicializado corretamente
            self.df = self.tratar_dados_faltantes(dados_faltantes)
            self.df = bot.normalizar_sexo_sinto(self.df)
            self.df = self.pre_processamento()

    def tratar_dados_faltantes(self, dados):
        """
        Executa o tratamento de dados faltantes.
        Verifica se o dataset foi carregado antes de iniciar o processamento.
        """
        if self.df is None or self.df.empty:
            print("⚠️ O dataset ainda não foi carregado. Execute `ler_dataset()` primeiro.")
            return
        
        print("🔄 Iniciando o tratamento de dados faltantes...")
        self.df = dados.executar_tratamento_de_dados_faltantes()
        print("✅ Tratamento de dados concluído!")

        return self.df
    
    def pre_processamento(self):
        pre = PreprocessDataset(self.df)
        self.df = pre.converter_tipos_colunas()
        return self.df
    
    def outliers(self):
        oute = Outliers(self.df)
        self.df = oute.executar_outliers()
        self.df = self.pre_processamento()
    
    def normalizacao(self):
        bot = Normalizacao(self.df)
        self.df = bot.executar_normalizacao()
        self.df = self.pre_processamento()

    def enviar_para_gcp(self, df, if_exists="replace", batch_size=10000):
        """
        Envia um DataFrame Pandas para uma tabela MySQL no Google Cloud (GCP) usando chunks para evitar consumo excessivo de memória.

        Parâmetros:
        - df (pd.DataFrame): DataFrame a ser enviado para o banco de dados.
        - if_exists (str): Opção de escrita na tabela ('fail', 'replace', 'append'). Padrão: 'replace'.
        - batch_size (int): Tamanho do chunk para inserção dos dados (padrão: 10.000 linhas por batch).

        Retorna:
        - None
        """

        try:
            # Criar a conexão com o banco de dados
            print("🔗 Conectando ao banco de dados GCP MySQL...")
            engine = create_engine("mysql+pymysql://devdavi:12345678@34.170.252.6/srag_warehouse")

            total_linhas = len(df)
            print(f"📤 Enviando {total_linhas} registros para a tabela 'srag_warehouse' em chunks de {batch_size} linhas...")

            # Dividir a inserção em chunks para evitar sobrecarga de memória
            for i, chunk in enumerate(range(0, total_linhas, batch_size)):
                df_chunk = df.iloc[chunk : chunk + batch_size]  # Pegando um pedaço do DataFrame

                # Enviando para o banco
                df_chunk.to_sql("srag_warehouse", con=engine, if_exists=if_exists, index=False, method="multi")

                print(f"✅ Chunk {i+1}: Inseriu {len(df_chunk)} registros (de {chunk} até {chunk+len(df_chunk)-1}).")

            print(f"\n🎉 Upload concluído com sucesso! Total de {total_linhas} registros inseridos.")

        except Exception as e:
            print(f"❌ Erro ao enviar dados para o banco GCP: {str(e)}")

    def executar_classe(self):
        #self.data_lake()
        self.ler_dataset()
        self.outliers()
        self.normalizacao()
        self.enviar_para_gcp(self.df)
    


    
    

# Criando uma instância da classe Oficial e executando o pipeline corretamente
bot = Oficial()
bot.executar_classe()




