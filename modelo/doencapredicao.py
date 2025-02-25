import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from gcp_dataset_warehouse import GCP_Dataset


class DoencaPredictor:
    """
    Classe para prever SRAG por Influenza ou COVID-19 com base nos sintomas e comorbidades usando uma Rede Neural.

    Métodos:
        - carregar_dados(): Carrega e prepara os dados para treinamento.
        - preprocessar_dados(): Realiza a codificação e normalização dos dados.
        - construir_modelo(): Define e compila a rede neural.
        - treinar_modelo(): Treina o modelo com os dados fornecidos.
        - avaliar_modelo(): Avalia o modelo e gera métricas de desempenho.
        - prever_doenca(): Faz previsões com base nos sintomas do paciente.
        - visualizar_resultados(): Plota gráficos de desempenho do modelo.
    """

    def __init__(self, df):
        """Inicializa os atributos da classe."""
        self.df = df
        self.X = None
        self.y = None
        self.label_encoder = LabelEncoder()
        self.scaler = StandardScaler()
        self.model = None
        self.history = None

    def carregar_dados(self):
        """
        Processa um dataset real contendo sintomas, comorbidades e classificação final da SRAG.

        Etapas:
            - Mantém apenas os casos de Influenza (1) e COVID-19 (5) na coluna CLASSI_FIN.
            - Remove valores nulos e inconsistências.
            - Renomeia CLASSI_FIN para "Doenca".
        """

        # Seleciona apenas as colunas relevantes
        colunas_necessarias = [
            "FEBRE", "TOSSE", "FADIGA", "DOR_ABD", "DISPNEIA", "SATURACAO", "VOMITO", "DIARREIA", 
            "PERD_PALA", "PERD_OLFT", "DESC_RESP", "GARGANTA",
            "DIABETES", "OBESIDADE", "CARDIOPATI", "RENAL", "HEPATICA", "IMUNODEPRE",
            "PUERPERA", "HEMATOLOGI", "SIND_DOWN", "ASMA", "NEUROLOGIC", "PNEUMOPATI",
            "CLASSI_FIN"
        ]
        
        self.df = self.df[colunas_necessarias]

        # Remover valores nulos na coluna CLASSI_FIN
        self.df.dropna(subset=["CLASSI_FIN"], inplace=True)

        # Filtrar apenas Influenza (1) e COVID-19 (5)
        self.df = self.df[self.df["CLASSI_FIN"].isin([1,2,3, 5])]

        # Renomear CLASSI_FIN para "Doenca" e substituir valores numéricos por texto
        self.df["Doenca"] = self.df["CLASSI_FIN"].replace({1: "Influenza", 2: "outro vírus", 3: "outro agente etiológico", 5: "COVID-19"})
        self.df.drop(columns=["CLASSI_FIN"], inplace=True)  # Remove a coluna original após a conversão

        print("✅ Dados processados com sucesso!")

    def preprocessar_dados(self):
        """
        Realiza o pré-processamento dos dados:
            - Transforma a variável categórica "Doenca" em valores numéricos.
            - Trata valores ausentes nas variáveis de entrada.
            - Normaliza as variáveis.
            - Divide os dados em conjunto de treino (80%) e teste (20%).
        """
        # Preenchimento de valores nulos com 0 (ausência da condição)
        self.df.fillna(0, inplace=True)

        # Separar variáveis de entrada (X) e saída (y)
        self.X = self.df.drop(columns=["Doenca"])
        self.y = self.label_encoder.fit_transform(self.df["Doenca"])

        # Normalizar os dados
        self.X = self.scaler.fit_transform(self.X)

        # Dividir dados em treino (80%) e teste (20%)
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            self.X, self.y, test_size=0.2, random_state=42, stratify=self.y
        )

        print("✅ Dados preprocessados com sucesso!")

    def construir_modelo(self):
        """
        Constrói e compila um modelo de Rede Neural usando Keras.
        """
        self.model = Sequential([
            Dense(128, activation="relu", input_shape=(self.X_train.shape[1],)),
            Dropout(0.3),
            Dense(64, activation="relu"),
            Dropout(0.2),
            Dense(len(self.label_encoder.classes_), activation="softmax")
        ])

        self.model.compile(optimizer="adam", loss="sparse_categorical_crossentropy", metrics=["accuracy"])
        print("✅ Modelo construído com sucesso!")

    def treinar_modelo(self, epochs=50, batch_size=16):
        """
        Treina o modelo com os dados de treino.

        Parâmetros:
            - epochs (int): Número de épocas do treinamento.
            - batch_size (int): Tamanho do batch durante o treinamento.
        """
        self.history = self.model.fit(
            self.X_train, self.y_train,
            epochs=epochs, batch_size=batch_size,
            validation_data=(self.X_test, self.y_test),
            verbose=1
        )
        print("✅ Modelo treinado com sucesso!")

    def avaliar_modelo(self):
        """
        Avalia o desempenho do modelo e exibe métricas de classificação.
        """
        # Gerar previsões
        y_pred = np.argmax(self.model.predict(self.X_test), axis=1)
        y_test_classes = self.y_test  # Mantém os rótulos verdadeiros

        # Exibir métricas
        print("Acurácia:", accuracy_score(y_test_classes, y_pred))
        print("\n🔍 Relatório de Classificação:\n", classification_report(
            y_test_classes, y_pred,
            target_names=["Influenza", "Outro vírus", "Outro agente etiológico", "COVID-19"]
        ))

        # Gerar matriz de confusão
        cm = confusion_matrix(y_test_classes, y_pred)

        # Exibir matriz de confusão
        plt.figure(figsize=(6, 4))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues",
                    xticklabels=["Influenza", "Outro vírus", "Outro agente etiológico", "COVID-19"],
                    yticklabels=["Influenza", "Outro vírus", "Outro agente etiológico", "COVID-19"])
        plt.xlabel('Previsto')
        plt.ylabel('Real')
        plt.title('Matriz de Confusão')
        plt.show()

    def visualizar_resultados(self):
        """
        Gera gráficos para visualizar a evolução do treinamento.
        """
        if self.history:
            # Gráfico de perda
            plt.figure(figsize=(6, 4))
            plt.plot(self.history.history['loss'], label='Perda Treino')
            plt.plot(self.history.history['val_loss'], label='Perda Validação')
            plt.xlabel('Épocas')
            plt.ylabel('Perda')
            plt.legend()
            plt.title('Evolução da Perda')
            plt.show()

            # Gráfico de acurácia
            plt.figure(figsize=(6, 4))
            plt.plot(self.history.history['accuracy'], label='Acurácia Treino')
            plt.plot(self.history.history['val_accuracy'], label='Acurácia Validação')
            plt.xlabel('Épocas')
            plt.ylabel('Acurácia')
            plt.legend()
            plt.title('Evolução da Acurácia')
            plt.show()



gcp = GCP_Dataset()
dataset = gcp.ler_gcp_DB()
modelo = DoencaPredictor(dataset)
modelo.carregar_dados()
modelo.preprocessar_dados()
modelo.construir_modelo()
modelo.treinar_modelo()
modelo.avaliar_modelo()
#modelo.visualizar_resultados()
