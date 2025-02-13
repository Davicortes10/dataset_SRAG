import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.utils import to_categorical
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import matplotlib.pyplot as plt
import seaborn as sns


class ClassificacaoEvolucao:
    def __init__(self, dataset):
        self.dataset = dataset
        self.scaler = StandardScaler()

    def filtrar_dados(self):
        """Remove registros com EVOLUCAO = 9 (Ignorado)."""
        return self.dataset[self.dataset['EVOLUCAO'] != 9]

    def preprocessamento(self, df):
        """Realiza normalização e codificação da variável alvo."""
        X = self.scaler.fit_transform(df[['NU_IDADE_N', 'QTD_FATOR_RISC', 'QTD_SINT']])
        y = to_categorical(df['EVOLUCAO'] - 1)  # Ajustando índices para one-hot encoding
        return X, y

    def dividir_dados(self, X, y):
        """Divide os dados em treino e teste."""
        return train_test_split(X, y, test_size=0.2, random_state=42)

    def criar_modelo(self, input_dim):
        """Cria e retorna o modelo de rede neural."""
        model = Sequential([
            Dense(64, activation='relu', input_shape=(input_dim,)),  # Camada oculta 1
            Dense(32, activation='relu'),  # Camada oculta 2
            Dense(3, activation='softmax')  # Camada de saída (3 classes)
        ])
        model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
        return model

    def treinar_modelo(self, model, X_train, y_train):
        """Treina a rede neural."""
        early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
        history = model.fit(
            X_train, y_train,
            epochs=100, batch_size=32, validation_split=0.2,
            callbacks=[early_stopping], verbose=1
        )
        return history

    def avaliar_modelo(self, model, X_test, y_test):
        """Avalia o modelo e gera métricas de desempenho."""
        y_pred = model.predict(X_test)
        y_pred_classes = np.argmax(y_pred, axis=1)
        y_test_classes = np.argmax(y_test, axis=1)

        # Exibir métricas
        print("Acurácia:", accuracy_score(y_test_classes, y_pred_classes))
        print("\n🔍 Relatório de Classificação:\n", classification_report(
            y_test_classes, y_pred_classes,
            target_names=['MELHORA DE QUADRO', 'ÓBITO', 'ÓBITO POR OUTRAS CAUSAS']
        ))

        # Exibir matriz de confusão
        plt.figure(figsize=(8, 6))
        sns.heatmap(
            confusion_matrix(y_test_classes, y_pred_classes),
            annot=True, fmt="d", cmap="Blues",
            xticklabels=['MELHORA DE QUADRO', 'ÓBITO', 'ÓBITO POR OUTRAS CAUSAS'],
            yticklabels=['MELHORA DE QUADRO', 'ÓBITO', 'ÓBITO POR OUTRAS CAUSAS']
        )
        plt.xlabel("Predito")
        plt.ylabel("Real")
        plt.title("Matriz de Confusão - Evolução do Paciente")
        plt.show()

    def executar_classificacao(self):
        """Executa todo o pipeline de classificação da evolução do paciente."""
        df = self.filtrar_dados()
        X, y = self.preprocessamento(df)
        X_train, X_test, y_train, y_test = self.dividir_dados(X, y)

        model = self.criar_modelo(X_train.shape[1])
        self.treinar_modelo(model, X_train, y_train)
        self.avaliar_modelo(model, X_test, y_test)

