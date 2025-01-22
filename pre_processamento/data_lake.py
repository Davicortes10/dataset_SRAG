from sqlalchemy import create_engine
import pandas as pd
import pymysql
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from playwright.sync_api import sync_playwright


class Data_Lake:
    def __init__(self,file_path, db):
        self.file_path = file_path
        self.db = db
        self.db_connection = f"jdbc:mysql://34.170.252.6:3306/{self.db}"
        self.spark = SparkSession.builder \
            .appName("Atualizar Data Lake") \
            .config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.33.jar") \
        .getOrCreate()
    
    def atualizar_Data_Lake(self):
        print("Iniciando o processo de atualização do Data Lake com PySpark...")

        # Leitura do arquivo CSV
        try:
            print(f"Lendo o arquivo CSV em: {self.file_path}")
            df = self.spark.read.option("delimiter", ";") \
                .option("header", "true") \
                .csv(self.file_path)
            print(f"Arquivo CSV lido com sucesso! Linhas: {df.count()}, Colunas: {len(df.columns)}")
            print(f"Colunas detectadas: {df.columns}")
            self.conectar_DB(df)
        except Exception as e:
            print(f"Erro ao ler o arquivo CSV: {e}")
            return
    
    def conectar_DB(self, df):
        # Upload para o banco de dados
        try:
            print(f"Iniciando o upload para a tabela 'srag_datalake' no banco de dados...")
            df.write \
                .format("jdbc") \
                .option("url", self.db_connection) \
                .option("dbtable", f"{self.db}") \
                .option("user", "devdavi") \
                .option("password", "12345678") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("overwrite") \
                .save()
            print("Upload concluído com sucesso!")
        except Exception as e:
            print(f"Erro ao fazer o upload para o banco de dados: {e}")
            return

        print("Processo de atualização do Data Lake finalizado com sucesso!")
    
    def automate_download(self):
        print("Iniciando a automação...")

        with sync_playwright() as p:
            print("Iniciando o navegador...")
            browser = p.chromium.launch(headless=True)  # Defina headless=True para rodar sem interface gráfica
            context = browser.new_context()

            # Acessar a página inicial
            page = context.new_page()
            try:
                url = "https://opendatasus.saude.gov.br/dataset/srag-2020"
                print(f"Acessando a página inicial: {url}")
                page.goto(url)
                time.sleep(4)  # Espera de 4 segundos
            except Exception as e:
                print(f"Erro ao acessar a página inicial: {e}")
                browser.close()
                return

            # Clicar no primeiro link usando XPath
            first_link_xpath = '//*[@id="dataset-resources"]/ul/li[3]/a'
            try:
                print(f"Aguardando o primeiro link no XPath: {first_link_xpath}")
                page.wait_for_selector(first_link_xpath, timeout=5000)  # Aguarde o elemento estar disponível
                print("Elemento encontrado. Clicando no primeiro link...")
                page.locator(first_link_xpath).click()
                time.sleep(4)  # Espera de 4 segundos
            except Exception as e:
                print(f"Erro ao clicar no primeiro link: {e}")
                browser.close()
                return

            # Aguarde a nova página carregar (se necessário)
            try:
                print("Aguardando o carregamento da nova página...")
                page.wait_for_load_state("load")
                time.sleep(4)  # Espera de 4 segundos
            except Exception as e:
                print(f"Erro ao aguardar o carregamento da página: {e}")
                browser.close()
                return

            # Clicar no link de download usando XPath
            download_link_xpath = '//*[@id="content"]/div[3]/section/div/div[1]/ul/li/div/a'
            try:
                print(f"Aguardando o link de download no XPath: {download_link_xpath}")
                page.wait_for_selector(download_link_xpath, timeout=5000)  # Aguarde o elemento estar disponível
                print("Elemento de download encontrado. Iniciando o download...")
                
                # Iniciar o download
                with page.expect_download() as download_info:
                    page.locator(download_link_xpath).click()
                download = download_info.value
                time.sleep(4)  # Espera de 4 segundos para garantir que o download seja iniciado

                # Salvar o arquivo baixado
                file_path = "srag.csv"
                print(f"Salvando o arquivo baixado em: {file_path}")
                download.save_as(file_path)

            except Exception as e:
                print(f"Erro ao clicar no link de download ou ao salvar o arquivo: {e}")
                browser.close()
                return

            # Fechar o navegador
            print("Fechando o navegador...")
            browser.close()
            print("Arquivo baixado com sucesso!")