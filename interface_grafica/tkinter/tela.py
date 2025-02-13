import tkinter as tk
from tkinter import ttk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import requests

url = "https://c02e-186-216-47-142.ngrok-free.app/api/obter_dados/"  # URL para GET dos dados
response = requests.get(url)

print(response.json())



# Função para obter dados da URL


# Função para exibir os dados na tela
def exibir_dados():
    dados = response.json()

    # Exibe os dados na tela
    texto = "\n".join([f"{key}: {value}" for key, value in dados.items()])
    label.config(text=texto)

# Cria a janela principal
root = tk.Tk()
root.title("Tela de Dados")
root.geometry("400x300")

# Cria o botão que irá chamar a função de exibir dados
botao = tk.Button(root, text="Obter Dados", command=exibir_dados)
botao.pack(pady=20)

# Cria o label que vai exibir os dados
label = tk.Label(root, text="", justify="left")
label.pack(pady=20)

# Inicia o loop da interface gráfica
root.mainloop()