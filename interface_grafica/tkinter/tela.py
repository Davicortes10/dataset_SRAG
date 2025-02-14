

import tkinter as tk
from tkinter import messagebox
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


# Função para buscar dados da API
def obter_dados_api():
    url = "https://7c96-186-216-47-142.ngrok-free.app/api/obter_dados/"  # Atualize com sua URL
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()  # Retorna os dados como dicionário
        else:
            messagebox.showerror("Erro", f"Erro ao obter dados: {response.status_code}")
            return None
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao se conectar à API: {e}")
        return None


# Função para exibir o gráfico
def exibir_grafico():
    dados = obter_dados_api()  # Obtém os dados da API
    if not dados:
        return  # Se não houver dados, sai da função

    # Supondo que os dados sejam uma matriz de confusão
    matriz_confusao = np.array(dados["matriz_confusao"])  # Dados esperados no formato de matriz
    labels = dados["labels"]  # Rótulos das classes

    # Configurando o gráfico
    plt.figure(figsize=(8, 6))
    sns.heatmap(
        matriz_confusao, annot=True, fmt="d", cmap="Blues",
        xticklabels=labels, yticklabels=labels
    )
    plt.xlabel("Predito")
    plt.ylabel("Real")
    plt.title("Matriz de Confusão - Evolução do Paciente")
    plt.show()


# Configuração da interface Tkinter
root = tk.Tk()
root.title("Visualização de Gráficos")
root.geometry("300x150")

# Botão para carregar os dados e gerar o gráfico
botao_grafico = tk.Button(root, text="Exibir Gráfico", command=exibir_grafico)
botao_grafico.pack(pady=20)

# Loop principal do Tkinter
root.mainloop()