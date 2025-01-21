from playwright.sync_api import sync_playwright

def automate_download():
    with sync_playwright() as p:
        # Iniciar o navegador
        browser = p.chromium.launch(headless=False)  # Defina headless=True para rodar sem interface gráfica
        context = browser.new_context()

        # Acessar a página inicial
        page = context.new_page()
        page.goto("https://opendatasus.saude.gov.br/dataset/srag-2020")  # Substitua pela URL da página inicial

        # Clicar no primeiro link usando XPath
        first_link_xpath = '//*[@id="dataset-resources"]/ul/li[3]/a'  # Substitua pelo XPath do link
        page.wait_for_selector(first_link_xpath, timeout=5000)  # Aguarde o elemento estar disponível
        page.locator(first_link_xpath).click()

        # Aguarde a nova página carregar (se necessário)
        page.wait_for_load_state("load")

        # Clicar no link de download usando XPath
        download_link_xpath = '//*[@id="content"]/div[3]/section/div/div[1]/ul/li/div/a'  # Substitua pelo XPath do botão
        page.wait_for_selector(download_link_xpath, timeout=5000)  # Aguarde o elemento estar disponível

        # Iniciar o download
        with page.expect_download() as download_info:
            page.locator(download_link_xpath).click()
        download = download_info.value

        # Salvar o arquivo baixado
        download.save_as("srag.csv")  # Substitua pelo caminho e nome do arquivo desejado

        # Fechar o navegador
        browser.close()
        print("Arquivo baixado com sucesso!")

# Executar a automação
automate_download()
