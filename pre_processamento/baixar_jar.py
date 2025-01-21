import time
from playwright.sync_api import sync_playwright

def download_mysql_connector():
    with sync_playwright() as p:
        # Iniciar o navegador
        print("Iniciando o navegador...")
        browser = p.chromium.launch(headless=False)  # Defina headless=True para rodar sem interface gráfica
        context = browser.new_context()
        page = context.new_page()

        # Acessar o site
        url = "https://downloads.mysql.com/archives/c-j/"
        print(f"Acessando o site: {url}")
        page.goto(url)
        time.sleep(3)  # Aguarde 3 segundos para garantir que a página seja carregada

        # Selecionar o primeiro item da lista suspensa (por exemplo, versão do conector)
        first_dropdown_xpath = '//*[@id="version"]'  # Substitua pelo XPath real do dropdown
        print("Aguardando o primeiro dropdown aparecer...")
        page.wait_for_selector(first_dropdown_xpath, timeout=10000)  # Aguarde até 10 segundos para o elemento estar disponível
        print("Selecionando o primeiro item na lista suspensa...")
        page.locator(first_dropdown_xpath).select_option("8.0.33")  # Substitua pelo valor desejado
        time.sleep(2)  # Aguarde 2 segundos após a seleção

        # Selecionar o segundo item da lista suspensa (por exemplo, sistema operacional)
        second_dropdown_xpath = '//*[@id="os"]'  # Substitua pelo XPath real do segundo dropdown
        print("Aguardando o segundo dropdown aparecer...")
        page.wait_for_selector(second_dropdown_xpath, timeout=10000)  # Aguarde até 10 segundos para o elemento estar disponível
        print("Selecionando o segundo item na lista suspensa...")
        page.locator(second_dropdown_xpath).select_option("Ubuntu Linux")  # Substitua pelo valor desejado
        time.sleep(2)  # Aguarde 2 segundos após a seleção

        # Clicar no botão para confirmar ou iniciar o download
        button_xpath = '//*[@id="files"]/div/table/tbody/tr[7]/td[4]/div/a'  # Substitua pelo XPath real do botão
        print("Aguardando o botão de download aparecer...")
        page.wait_for_selector(button_xpath, timeout=10000)  # Aguarde até 10 segundos para o botão estar disponível
        print("Clicando no botão de download...")
        page.locator(button_xpath).click()
        time.sleep(5)  # Aguarde 5 segundos para o download iniciar

        print("Operação concluída com sucesso!")

        # Fechar o navegador
        print("Fechando o navegador...")
        browser.close()

# Executar a função
if __name__ == "__main__":
    download_mysql_connector()
