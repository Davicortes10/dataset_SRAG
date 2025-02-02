from sqlalchemy import create_engine
import pandas as pd
import pymysql
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from playwright.sync_api import sync_playwright


class Data_Lake:
    def __init__(self,caminho, db):
        self.file_path = "srag.csv"
        self.caminho = caminho
        self.db = db
        self.db_connection = f"jdbc:mysql://34.170.252.6:3306/{self.db}"
        self.spark = SparkSession.builder \
            .appName("Atualizar Data Lake") \
            .config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.33.jar") \
        .getOrCreate()

    def automate_download(self):
        """
        Automatiza o download do arquivo SRAG do OpenDataSUS utilizando Playwright.

        Etapas do processo:
        1. Abre um navegador headless e acessa a URL do dataset SRAG-2020.
        2. Localiza e clica no primeiro link da página.
        3. Aguarda a nova página carregar completamente.
        4. Localiza e clica no link de download do arquivo.
        5. Salva o arquivo baixado no diretório local.

        Tratamento de Erros:
        - Se houver erro ao acessar a página ou localizar elementos, o navegador é fechado corretamente.
        - Se o download falhar, exibe uma mensagem de erro.

        Retorna:
        - None. Apenas executa o download e salva o arquivo.

        Dependências:
        - Instale o Playwright antes de executar: `pip install playwright`
        - Rode `playwright install` para instalar os navegadores.
        """

        print("🔄 Iniciando a automação do download...")

        with sync_playwright() as p:
            try:
                print("🌍 Inicializando navegador headless...")
                browser = p.chromium.launch(headless=True)  # Headless=True para rodar em segundo plano
                context = browser.new_context()
                page = context.new_page()

                # 📌 Acessar a página inicial
                url = "https://opendatasus.saude.gov.br/dataset/srag-2020"
                print(f"🔍 Acessando URL: {url}")
                page.goto(url, timeout=10000)  # Tempo limite de 10s para carregar a página
                page.wait_for_load_state("load")  # Aguarde a página carregar completamente
                print("✅ Página carregada com sucesso!")

                # 📌 Localizar e clicar no primeiro link (acessar página de downloads)
                first_link_xpath = '//*[@id="dataset-resources"]/ul/li[3]/a'
                print(f"🔍 Aguardando elemento: {first_link_xpath}")
                page.wait_for_selector(first_link_xpath, timeout=5000)
                print("✅ Elemento encontrado. Clicando...")
                page.locator(first_link_xpath).click()

                # 📌 Aguarde a nova página carregar
                page.wait_for_load_state("load")
                print("✅ Nova página carregada!")

                # 📌 Localizar e clicar no link de download
                download_link_xpath = '//*[@id="content"]/div[3]/section/div/div[1]/ul/li/div/a'
                print(f"🔍 Aguardando elemento de download: {download_link_xpath}")
                page.wait_for_selector(download_link_xpath, timeout=5000)
                print("✅ Elemento de download encontrado. Iniciando o download...")

                # 📌 Iniciar o download e aguardar sua conclusão
                with page.expect_download() as download_info:
                    page.locator(download_link_xpath).click()
                download = download_info.value

                # 📌 Salvar o arquivo baixado
                file_path = "srag.csv"
                download.save_as(file_path)
                print(f"📁 Download concluído! Arquivo salvo em: {file_path}")

            except Exception as e:
                print(f"❌ Erro durante a automação: {e}")

            finally:
                # 🚀 Garante que o navegador sempre seja fechado
                print("🛑 Fechando o navegador...")
                browser.close()

    def atualizar_Data_Lake(self):
        """
        Atualiza o Data Lake carregando um arquivo CSV no PySpark e enviando os dados para o banco.

        Etapas do Processo:
        1️⃣ Verifica se o arquivo existe antes de tentar carregar.
        2️⃣ Lê o arquivo CSV usando PySpark.
        3️⃣ Exibe estatísticas do DataFrame carregado.
        4️⃣ Conecta ao banco de dados e insere os dados.

        Tratamento de Erros:
        - Se o arquivo CSV não for encontrado, exibe um erro e interrompe o processo.
        - Se houver falha na leitura do CSV, exibe um erro detalhado.
        - Se o PySpark não estiver configurado corretamente, exibe uma mensagem de erro.

        Parâmetros:
        - self.file_path (str): Caminho do arquivo CSV a ser carregado.
        - self.spark (SparkSession): Sessão ativa do Spark.

        Retorna:
        - None. Apenas carrega os dados e atualiza o Data Lake.
        """

        print("🔄 Iniciando o processo de atualização do Data Lake com PySpark...")

        # 🚀 Verificar se o arquivo CSV existe antes de tentar carregar
        if not os.path.exists(self.caminho):
            print(f"❌ Erro: O arquivo '{self.caminho}' não foi encontrado.")
            return

        try:
            # 📌 Ler o arquivo CSV usando PySpark
            print(f"📂 Lendo o arquivo CSV: {self.caminho}")
            df = self.spark.read.option("delimiter", ";") \
                .option("header", "true") \
                .csv(self.caminho)

            # 🚀 Verificar se o DataFrame foi carregado corretamente
            num_linhas = df.count()
            num_colunas = len(df.columns)

            if num_linhas == 0 or num_colunas == 0:
                print("⚠️ Aviso: O arquivo CSV foi carregado, mas está vazio!")
                return

            # 📊 Exibir informações sobre os dados carregados
            print(f"✅ Arquivo CSV carregado com sucesso!")
            print(f"🔢 Total de Linhas: {num_linhas}")
            print(f"📝 Total de Colunas: {num_colunas}")
            print(f"📋 Colunas detectadas: {df.columns}")

            # 📌 Chamar a função para conectar ao banco de dados e inserir os dados
            self.conectar_DB(df)

        except Exception as e:
            print(f"❌ Erro ao ler o arquivo CSV: {e}")
            return
    
    def conectar_DB(self, df):
        """
        Conecta ao banco de dados e realiza o upload dos dados do DataFrame para a tabela 'srag_datalake'.

        Etapas do Processo:
        1️⃣ Verifica se o DataFrame contém dados antes de tentar salvar.
        2️⃣ Configura a conexão JDBC para o MySQL.
        3️⃣ Insere os dados no banco de dados no modo 'overwrite' (substituir existentes).
        4️⃣ Exibe logs detalhados sobre o processo.

        Segurança:
        - Usuário e senha são recuperados de variáveis de ambiente para maior segurança.

        Tratamento de Erros:
        - Se o DataFrame estiver vazio, exibe um alerta e interrompe o processo.
        - Se houver erro na conexão ou inserção, exibe uma mensagem detalhada.

        Parâmetros:
        - self.db_connection (str): URL de conexão JDBC do banco de dados.
        - self.db (str): Nome da tabela no banco.
        - df (pyspark.sql.DataFrame): DataFrame PySpark contendo os dados a serem inseridos.

        Retorna:
        - None. Apenas faz o upload dos dados.
        """

        print("🔄 Iniciando o upload para o banco de dados...")

        # 🚀 Verificação: O DataFrame contém dados?
        if df.count() == 0:
            print("⚠️ O DataFrame está vazio! Nenhum dado foi enviado ao banco.")
            return

        try:
            # 📌 Credenciais armazenadas de forma segura
            db_user = os.getenv("DB_USER", "devdavi")  # Usa variável de ambiente ou valor padrão
            db_password = os.getenv("DB_PASSWORD", "12345678")  # Usa variável de ambiente ou valor padrão

            print(f"📡 Conectando ao banco de dados...")
            print(f"🔗 URL: {self.db_connection}")
            print(f"📂 Tabela de destino: {self.db}")

            # 📌 Escrever os dados no banco de dados
            df.write \
                .format("jdbc") \
                .option("url", self.db_connection) \
                .option("dbtable", f"{self.db}") \
                .option("user", db_user) \
                .option("password", db_password) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("overwrite") \
                .save()

            print("✅ Upload concluído com sucesso!")

        except Exception as e:
            print(f"❌ Erro ao fazer o upload para o banco de dados: {str(e)}")
            return

        print("✅ Processo de atualização do Data Lake finalizado com sucesso!")
    
    def executar_datalake(self):
        """
        Executa todo o pipeline do Data Lake, incluindo:
        
        1️⃣ **Download Automático dos Dados**
        2️⃣ **Atualização do Data Lake**
        
        Etapas do Processo:
        - Chama `automate_download()` para baixar os dados do OpenDataSUS.
        - Chama `atualizar_Data_Lake()` para carregar os dados no sistema.
        
        Tratamento de Erros:
        - Se o download falhar, a atualização do Data Lake não será executada.
        - Logs detalhados são exibidos para facilitar a depuração.
        
        Retorna:
        - None. Apenas executa as funções na sequência correta.
        """

        print("🔄 Iniciando o processo de execução do Data Lake...")

        try:
            print("📥 Passo 1: Baixando os dados...")
            self.automate_download()
            print("✅ Download concluído com sucesso!\n")

        except Exception as e:
            print(f"❌ Erro ao baixar os dados: {str(e)}")
            print("🚫 Processo interrompido!")
            return  # Interrompe o fluxo se o download falhar

        try:
            print("📊 Passo 2: Atualizando o Data Lake...")
            self.atualizar_Data_Lake()
            print("✅ Atualização do Data Lake concluída com sucesso!")

        except Exception as e:
            print(f"❌ Erro ao atualizar o Data Lake: {str(e)}")
            print("🚫 Processo interrompido!")
            return

        print("🏁 ✅ Processo completo! O Data Lake foi atualizado com sucesso! 🚀")

