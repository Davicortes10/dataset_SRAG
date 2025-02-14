import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout

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

    def __init__(self, dataset_path):
        """Inicializa os atributos da classe."""
        self.dataset_path = dataset_path
        self.df = None
        self.X = None
        self.y = None
        self.label_encoder = LabelEncoder()
        self.scaler = StandardScaler()
        self.model = None
        self.history = None

    def carregar_dados(self):
        """
        Carrega um dataset real contendo sintomas, comorbidades e classificação final da SRAG.

        - Mantém apenas os casos de Influenza (1) e COVID-19 (5) na coluna CLASSI_FIN.
        - Remove valores nulos e inconsistências.
        """
        # Carregar dataset com sintomas e comorbidades
        self.df = pd.read_csv(self.dataset_path, usecols=[
            "FEBRE", "TOSSE", "FADIGA", "DOR_ABD", "DISPNEIA", "SATURACAO", "VOMITO", "DIARREIA", "PERD_PALA","PERD_OLFT", "DESC_RESP", "GARGANTA"
            "DIABETES", "OBESIDADE", "CARDIOPATI", "RENAL", "HEPATICA", "IMUNODEPRE",
            "CLASSI_FIN", "PUERPERA", "HEMATOLOGI", "SIND_DOWN", "ASMA", "NEUROLOGIC", "PNEUMOPATI"
        ])

        # Remover valores nulos na coluna CLASSI_FIN
        self.df.dropna(subset=["CLASSI_FIN"], inplace=True)

        # Filtrar apenas Influenza (1) e COVID-19 (5)
        self.df = self.df[self.df["CLASSI_FIN"].isin([1, 5])]

        # Renomear CLASSI_FIN para "Doenca" e substituir valores
        self.df["CLASSI_FIN"] = self.df["CLASSI_FIN"].replace({1: "Influenza", 5: "COVID-19"})
        self.df.rename(columns={"CLASSI_FIN": "Doenca"}, inplace=True)

        print("✅ Dados carregados com sucesso!")

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
        y_pred = np.argmax(self.model.predict(self.X_test), axis=1)

        print("\n🔍 Relatório de Classificação:\n", classification_report(
            self.y_test, y_pred, target_names=self.label_encoder.classes_
        ))

        cm = confusion_matrix(self.y_test, y_pred)

        plt.figure(figsize=(6, 4))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues",
                    xticklabels=self.label_encoder.classes_,
                    yticklabels=self.label_encoder.classes_)
        plt.xlabel('Previsto')
        plt.ylabel('Real')
        plt.title('Matriz de Confusão')
        plt.show()

    def prever_doenca(self, sintomas):
        """
        Realiza uma previsão com base nos sintomas e comorbidades do paciente.

        Parâmetros:
            - sintomas (list): Lista binária com os sintomas (1 = presente, 0 = ausente).

        Retorna:
            - Nome da doença prevista e probabilidade.
        """
        sintomas_array = np.array([sintomas])  # Converter para numpy array
        sintomas_scaled = self.scaler.transform(sintomas_array)  # Normalizar entrada

        predicao = self.model.predict(sintomas_scaled)
        doenca_prevista = self.label_encoder.inverse_transform([np.argmax(predicao)])

        print(f"\n🔮 Previsão: {doenca_prevista[0]} (Confiança: {100*np.max(predicao):.2f}%)")

    def visualizar_resultados(self):
        """
        Gera gráficos para visualizar a evolução do treinamento e a matriz de confusão.
        """
        # Evolução da Acurácia
        plt.plot(self.history.history['accuracy'], label='Acurácia Treino')
        plt.plot(self.history.history['val_accuracy'], label='Acurácia Validação')
        plt.xlabel('Épocas')
        plt.ylabel('Acurácia')
        plt.legend()
        plt.title('Evolução da Acurácia')
        plt.show()

# Exemplo de uso
modelo = DoencaPredictor("seu_dataset.csv")  # Substitua pelo caminho correto do arquivo
modelo.carregar_dados()
modelo.preprocessar_dados()
modelo.construir_modelo()
modelo.treinar_modelo()
modelo.avaliar_modelo()
