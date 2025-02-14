import tkinter as tk
from tkinter import ttk
import requests
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
import numpy as np

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Visualização de Resultados")
        self.root.geometry("800x600")

        # Criando os botões
        ttk.Button(root, text="Carregar Métricas", command=self.carregar_metricas).pack(pady=10)
        ttk.Button(root, text="Carregar Gráficos", command=self.carregar_graficos).pack(pady=10)

        # Frame para exibir os gráficos
        self.frame_graficos = ttk.Frame(root)
        self.frame_graficos.pack(fill=tk.BOTH, expand=True)

    def carregar_metricas(self):
        """Faz uma requisição GET à API e exibe as métricas no console."""
        url = "http://127.0.0.1:8000/api/fornecer_dados/"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                dados = response.json()
                print("Métricas carregadas com sucesso!")
                print("Acurácia:", dados.get("acuracia"))
                print("Relatório de Classificação:", dados.get("relatorio_classificacao"))
                print("Matriz de Confusão:", dados.get("matriz_confusao"))
            else:
                print(f"Erro ao carregar métricas: {response.status_code}")
        except Exception as e:
            print("Erro na requisição:", e)

    def carregar_graficos(self):
        """Faz uma requisição GET à API e gera os gráficos."""
        url = "https://13a9-186-216-47-142.ngrok-free.app/fornecer_graficos/"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                dados = response.json()
                self.gerar_graficos(dados)
            else:
                print(f"Erro ao carregar gráficos: {response.status_code}")
        except Exception as e:
            print("Erro na requisição:", e)

    def gerar_graficos(self, dados):
        """Gera os gráficos com base nos dados recebidos."""
        # Limpando o frame de gráficos
        for widget in self.frame_graficos.winfo_children():
            widget.destroy()

        # Gráfico de Perda
        fig1, ax1 = plt.subplots(figsize=(4, 3))
        ax1.plot(dados["perda_treino"], label="Perda Treino")
        ax1.plot(dados["perda_validacao"], label="Perda Validação")
        ax1.set_title("Evolução da Perda")
        ax1.set_xlabel("Épocas")
        ax1.set_ylabel("Perda")
        ax1.legend()
        canvas1 = FigureCanvasTkAgg(fig1, self.frame_graficos)
        canvas1.get_tk_widget().pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Gráfico de Acurácia
        fig2, ax2 = plt.subplots(figsize=(4, 3))
        ax2.plot(dados["acuracia_treino"], label="Acurácia Treino")
        ax2.plot(dados["acuracia_validacao"], label="Acurácia Validação")
        ax2.set_title("Evolução da Acurácia")
        ax2.set_xlabel("Épocas")
        ax2.set_ylabel("Acurácia")
        ax2.legend()
        canvas2 = FigureCanvasTkAgg(fig2, self.frame_graficos)
        canvas2.get_tk_widget().pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Gráfico de Matriz de Confusão (Heatmap)
        fig3, ax3 = plt.subplots(figsize=(4, 3))
        matriz_confusao = np.array(dados["matriz_confusao"])
        cax = ax3.matshow(matriz_confusao, cmap="Blues")
        fig3.colorbar(cax)
        for (i, j), valor in np.ndenumerate(matriz_confusao):
            ax3.text(j, i, str(valor), ha="center", va="center", color="red")
        ax3.set_title("Matriz de Confusão")
        ax3.set_xlabel("Predições")
        ax3.set_ylabel("Valores Reais")
        canvas3 = FigureCanvasTkAgg(fig3, self.frame_graficos)
        canvas3.get_tk_widget().pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

# Inicializando o app
root = tk.Tk()
app = App(root)
root.mainloop()
