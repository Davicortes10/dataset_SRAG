from pre_processamento import PreprocessDataset
from data_lake import Data_Lake
from dados_faltantes import Dados_Faltantes
from gcp_dataset import GCP_Dataset

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
        self.bot_passo_2 = GCP_Dataset()

    def ler_dataset(self):
        """
        Lê os dados do banco GCP e armazena no atributo `self.df`.
        Após a leitura, inicializa `bot_passo_2` com `Dados_Faltantes(self.df)`.
        """
        print("📥 Lendo o dataset do GCP...")
        self.df = self.bot_passo_1.ler_gcp_DB()

        if self.df is None or self.df.empty:
            print("❌ Erro: O dataset não foi carregado corretamente!")
        else:
            print("✅ Dataset carregado com sucesso!")
            self.bot_passo_2 = Dados_Faltantes(self.df)  # Agora pode ser inicializado corretamente

    def tratar_dados_faltantes(self):
        """
        Executa o tratamento de dados faltantes.
        Verifica se o dataset foi carregado antes de iniciar o processamento.
        """
        if self.df is None or self.df.empty:
            print("⚠️ O dataset ainda não foi carregado. Execute `ler_dataset()` primeiro.")
            return
        
        print("🔄 Iniciando o tratamento de dados faltantes...")
        self.bot_passo_2.executar_tratamento_de_dados_faltantes()
        print("✅ Tratamento de dados concluído!")
    
    def data_lake(self):
        self.bot_passo_2.executar_datalake()

# Criando uma instância da classe Oficial e executando o pipeline corretamente
bot = Oficial()
bot.data_lake()
#bot.ler_dataset()  # Primeiro, carrega os dados do banco
#bot.tratar_dados_faltantes()  # Depois, trata os dados faltantes

