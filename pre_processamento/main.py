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

    def enviar_para_gcp(self, df, if_exists="replace"):
        """
        Envia um DataFrame Pandas para uma tabela MySQL no Google Cloud (GCP) sem dividir em chunks.

        Parâmetros:
        - df (pd.DataFrame): DataFrame a ser enviado para o banco de dados.
        - if_exists (str): Opção de escrita na tabela ('fail', 'replace', 'append'). Padrão: 'replace'.

        Retorna:
        - None
        """

        print("\n🔍 Iniciando envio do DataFrame para o GCP...\n")

        # 📌 Verificar se o DataFrame está vazio antes de enviar
        if df.empty:
            print("⚠️ Aviso: O DataFrame está vazio. Nenhum dado será enviado para o banco de dados.")
            return

        if len(df.columns) == 0:
            print("⚠️ Aviso: O DataFrame não possui colunas. Verifique os dados antes do envio.")
            return

        # 📌 Exibir os 5 primeiros registros para depuração
        print("\n📊 Primeiros registros do DataFrame que será enviado:")
        print(df.head())

        try:
            # Criar a conexão com o banco de dados
            print("\n🔗 Conectando ao banco de dados GCP MySQL...")
            engine = create_engine("mysql+pymysql://devdavi:12345678@34.170.252.6/srag_warehouse")

            # 📌 Enviar os dados para o banco
            print(f"\n📤 Enviando {len(df)} registros para a tabela 'srag_warehouse'...")

            df.to_sql("srag_warehouse", con=engine, if_exists=if_exists, index=False, method="multi")

            # 📌 Confirmar se os dados foram enviados corretamente
            print(f"\n✅ Upload concluído com sucesso na tabela 'srag_warehouse'!")
            print(f"🔹 Total de registros enviados: {len(df)}")

        except Exception as e:
            print(f"\n❌ Erro ao enviar dados para o banco GCP: {str(e)}")

        print("\n🚀 Processo de envio finalizado.\n")
    
    def executar_classe(self):
        self.data_lake()
        self.ler_dataset()
        self.outliers()
        self.normalizacao()
        self.enviar_para_gcp(self.df)
    


    
    

# Criando uma instância da classe Oficial e executando o pipeline corretamente
bot = Oficial()
bot.executar_classe()




