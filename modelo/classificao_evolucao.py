import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.utils import to_categorical
from gcp_dataset_warehouse import GCP_Dataset
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import matplotlib.pyplot as plt
import seaborn as sns



class ClassificacaoEvolucao:
    """
    Classe para prever a evolução do paciente com base nos fatores de risco e sintomas.

    Métodos:
        - filtrar_dados(): Remove registros inválidos e mantém apenas "Melhora" e "Óbito".
        - preprocessamento(): Normaliza os dados e transforma a variável alvo.
        - dividir_dados(): Separa os dados em treino e teste.
        - criar_modelo(): Define e compila a rede neural.
        - treinar_modelo(): Treina a rede neural e armazena o histórico.
        - avaliar_modelo(): Gera métricas de desempenho do modelo.
        - visualizar_resultados(): Plota gráficos de perda e acurácia.
        - executar_classificacao(): Executa todo o pipeline de classificação.
    """

    def __init__(self, dataset):
        """Inicializa a classe com o dataset e normalizador."""
        self.dataset = dataset
        self.scaler = StandardScaler()
        self.history = None
        self.dados = ()

    def filtrar_dados(self):
        """Remove registros inválidos e mantém apenas os casos de MELHORA e ÓBITO."""
        df = self.dataset

        # Remove registros com EVOLUCAO = 9 (Ignorado)
        df = df[df['EVOLUCAO'] != 9]

        # Converte EVOLUCAO para classes binárias
        df['EVOLUCAO'] = df['EVOLUCAO'].replace({1: 0, 2: 1, 3: 1})  # 0 = Melhora, 1 = Óbito

        return df

    def preprocessamento(self, df):
        """Normaliza as variáveis de entrada e transforma a variável alvo."""
        X = self.scaler.fit_transform(df[['NU_IDADE_N', 'QTD_FATOR_RISC', 'QTD_SINT']])
        y = to_categorical(df['EVOLUCAO'])  # Conversão para one-hot encoding
        return X, y

    def dividir_dados(self, X, y):
        """Divide os dados em treino e teste."""
        return train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    def criar_modelo(self, input_dim):
        """Cria e retorna o modelo de rede neural."""
        model = Sequential([
            Dense(64, activation='relu', input_shape=(input_dim,)),
            Dropout(0.3),
            Dense(32, activation='relu'),
            Dropout(0.2),
            Dense(2, activation='softmax')  # Camada de saída (2 classes: Melhora e Óbito)
        ])
        model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
        return model

    def treinar_modelo(self, model, X_train, y_train):
        """Treina a rede neural e armazena o histórico do treinamento."""
        early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
        self.history = model.fit(
            X_train, y_train,
            epochs=100, batch_size=32, validation_split=0.2,
            callbacks=[early_stopping], verbose=1
        )
        return model
    
    def avaliar_modelo(self, model, X_test, y_test):
        """Avalia o modelo e gera métricas de desempenho."""
        y_pred = model.predict(X_test)
        y_pred_classes = np.argmax(y_pred, axis=1)
        y_test_classes = np.argmax(y_test, axis=1)

        # Exibir métricas
        print("Acurácia:", accuracy_score(y_test_classes, y_pred_classes))
        print("\n🔍 Relatório de Classificação:\n", classification_report(
            y_test_classes, y_pred_classes,
            target_names=['MELHORA DE QUADRO', 'ÓBITO']
        ))
        # Calcular métricas
        acuracia = accuracy_score(y_test_classes, y_pred_classes)
        relatorio_classificacao = classification_report(
            y_test_classes,
            y_pred_classes,
            target_names=['MELHORA DE QUADRO', 'ÓBITO'],
            output_dict=True  # Para obter o relatório como um dicionário
        )
        matriz_confusao = confusion_matrix(y_test_classes, y_pred_classes).tolist()
        '''self.enviar_dados({
            "acuracia": acuracia,
            "relatorio_classificacao": relatorio_classificacao,
            "matriz_confusao": matriz_confusao
        })'''

    def enviar_dados(self,dados):
        """Envia as métricas para o endpoint Django."""
        url = "https://7c96-186-216-47-142.ngrok-free.app/api/armazenar_dados/"  # Substitua pela URL do seu endpoint
        try:
            response = requests.post(url, json=dados)
            if response.status_code == 200:
                print("Dados enviados com sucesso!")
                print("Resposta do servidor:", response.json())
            else:
                print(f"Erro ao enviar dados. Status code: {response.status_code}")
                print("Resposta:", response.text)
        except Exception as e:
            print("Erro ao enviar os dados:", e)

    def visualizar_resultados(self):
        """Plota gráficos de perda e acurácia ao longo das épocas."""
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

    def executar_classificacao(self):
        """Executa todo o pipeline de classificação da evolução do paciente."""
        df = self.filtrar_dados()
        X, y = self.preprocessamento(df)
        X_train, X_test, y_train, y_test = self.dividir_dados(X, y)

        model = self.criar_modelo(X_train.shape[1])
        model = self.treinar_modelo(model, X_train, y_train)
        self.avaliar_modelo(model, X_test, y_test)
        #self.visualizar_resultados()


gcp = GCP_Dataset()
df = gcp.ler_gcp_DB()
bot = ClassificacaoEvolucao(df)



