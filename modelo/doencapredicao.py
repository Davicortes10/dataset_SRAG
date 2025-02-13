import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

class DoencaPredictor:
    """
    Classe para prever doenças com base nos sintomas usando uma Rede Neural.

    Métodos:
        - carregar_dados(): Carrega e prepara os dados para treinamento.
        - preprocessar_dados(): Realiza a codificação e normalização dos dados.
        - construir_modelo(): Define e compila a rede neural.
        - treinar_modelo(): Treina o modelo com os dados fornecidos.
        - avaliar_modelo(): Avalia o modelo e gera métricas de desempenho.
        - prever_doenca(): Faz previsões com base nos sintomas do paciente.
        - visualizar_resultados(): Plota gráficos de desempenho do modelo.
    """
    
    def __init__(self):
        """Inicializa os atributos da classe."""
        self.df = None
        self.X = None
        self.y = None
        self.label_encoder = LabelEncoder()
        self.scaler = StandardScaler()
        self.model = None
        self.history = None

    def carregar_dados(self):
        """
        Carrega um dataset fictício com sintomas e doenças.

        O dataset é estruturado com colunas binárias para sintomas (1 = presente, 0 = ausente),
        e uma coluna categórica para a doença diagnosticada.
        """
        data = {
            "Febre": [1, 1, 0, 1, 0, 1, 1, 0, 1, 0],
            "Tosse": [1, 0, 1, 1, 0, 1, 1, 0, 1, 0],
            "Fadiga": [0, 1, 0, 1, 0, 1, 1, 1, 0, 0],
            "Dor_Cabeca": [1, 0, 1, 1, 0, 0, 1, 0, 1, 1],
            "Falta_Ar": [0, 1, 1, 1, 0, 1, 0, 1, 1, 0],
            "Diarreia": [0, 0, 0, 1, 1, 1, 0, 0, 1, 1],
            "Doenca": ["Gripe", "COVID-19", "Pneumonia", "COVID-19", "Dengue", "COVID-19", 
                       "Gripe", "Dengue", "Pneumonia", "Gripe"]
        }
        self.df = pd.DataFrame(data)
        print("✅ Dados carregados com sucesso!")

    def preprocessar_dados(self):
        """
        Realiza o pré-processamento dos dados:
            - Transforma a variável categórica "Doenca" em valores numéricos.
            - Normaliza as variáveis de entrada.
            - Divide os dados em conjunto de treino (80%) e teste (20%).
        """
        self.X = self.df.drop(columns=["Doenca"])
        self.y = self.label_encoder.fit_transform(self.df["Doenca"])

        # Normalizar os dados
        self.X = self.scaler.fit_transform(self.X)

        # Dividir dados em treino e teste
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            self.X, self.y, test_size=0.2, random_state=42, stratify=self.y
        )

        print("✅ Dados preprocessados com sucesso!")

    def construir_modelo(self):
        """
        Constrói e compila um modelo de Rede Neural usando Keras.
        """
        self.model = Sequential([
            Dense(64, activation="relu", input_shape=(self.X_train.shape[1],)),
            Dropout(0.3),
            Dense(32, activation="relu"),
            Dropout(0.2),
            Dense(len(self.label_encoder.classes_), activation="softmax")
        ])

        self.model.compile(optimizer="adam", loss="sparse_categorical_crossentropy", metrics=["accuracy"])
        print("✅ Modelo construído com sucesso!")

    def treinar_modelo(self, epochs=100, batch_size=8):
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

    def prever_doenca(self, sintomas):
        """
        Realiza uma previsão com base nos sintomas do paciente.

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

        # Matriz de Confusão
        y_pred = np.argmax(self.model.predict(self.X_test), axis=1)
        cm = confusion_matrix(self.y_test, y_pred)

        plt.figure(figsize=(6, 4))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", 
                    xticklabels=self.label_encoder.classes_, 
                    yticklabels=self.label_encoder.classes_)
        plt.xlabel('Previsto')
        plt.ylabel('Real')
        plt.title('Matriz de Confusão')
        plt.show()

# Criando e executando o modelo
if __name__ == "__main__":
    # Criar instância da classe
    modelo = DoencaPredictor()

    # Executar as etapas
    modelo.carregar_dados()
    modelo.preprocessar_dados()
    modelo.construir_modelo()
    modelo.treinar_modelo(epochs=50, batch_size=8)
    modelo.avaliar_modelo()
    modelo.visualizar_resultados()

    # Teste de previsão para um novo paciente
    novo_paciente = [1, 1, 0, 1, 0, 1]  # Exemplo de sintomas
    modelo.prever_doenca(novo_paciente)
