import time
from playwright.sync_api import sync_playwright

def automate_download():
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

# Executar a automação
if __name__ == "__main__":
    automate_download()
