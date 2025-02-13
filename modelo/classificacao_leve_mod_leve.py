import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.preprocessing import StandardScaler, LabelEncoder
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns


class Classificacao_Leve_Mode_Grave:
    def __init__(self, dataset):
        self.dataset = dataset
        self.label_encoder = LabelEncoder()
        self.scaler = StandardScaler()

    def selecionar_colunas(self):
        """Seleciona as colunas relevantes para análise."""
        colunas_relevantes = [
            "NU_IDADE_N", "FEBRE", "TOSSE", "DISPNEIA", "SATURACAO",
            "FADIGA", "DOR_ABD", "DESC_RESP", "VOMITO", "DIARREIA",
            "PERD_OLFT", "PERD_PALA", "DIABETES", "CARDIOPATI",
            "OBESIDADE", "RENAL", "HEPATICA", "NEUROLOGIC",
            "IMUNODEPRE", "ASMA", "PNEUMOPATI", "CLASSIFICACAO"
        ]
        return self.dataset[colunas_relevantes]

    def preprocessamento(self, df):
        """Realiza a normalização e codificação das variáveis."""
        df["CLASSIFICACAO"] = self.label_encoder.fit_transform(df["CLASSIFICACAO"])
        X = df.drop(columns=["CLASSIFICACAO"])
        y = df["CLASSIFICACAO"]
        X_scaled = self.scaler.fit_transform(X)
        return X_scaled, y

    def dividir_dados(self, X, y):
        """Divide os dados em conjuntos de treino e teste."""
        return train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    def criar_modelo(self, input_dim):
        """Cria e retorna a estrutura da rede neural."""
        model = Sequential([
            Dense(64, activation="relu", input_shape=(input_dim,)),
            Dropout(0.3),
            Dense(32, activation="relu"),
            Dropout(0.2),
            Dense(16, activation="relu"),
            Dense(3, activation="softmax")
        ])
        model.compile(optimizer="adam", loss="sparse_categorical_crossentropy", metrics=["accuracy"])
        return model

    def treinar_modelo(self, model, X_train, y_train, X_test, y_test):
        """Treina a rede neural."""
        early_stopping = EarlyStopping(monitor="val_loss", patience=10, restore_best_weights=True)
        history = model.fit(X_train, y_train, epochs=100, batch_size=32, validation_data=(X_test, y_test),
                            callbacks=[early_stopping], verbose=1)
        return history

    def avaliar_modelo(self, model, X_test, y_test):
        """Avalia o modelo e gera a matriz de confusão."""
        y_pred = np.argmax(model.predict(X_test), axis=1)
        print("\n🔍 Relatório de Classificação:\n", classification_report(y_test, y_pred, target_names=self.label_encoder.classes_))

        # Matriz de Confusão
        plt.figure(figsize=(8, 6))
        sns.heatmap(
            confusion_matrix(y_test, y_pred),
            annot=True,
            fmt="d",
            cmap="Blues",
            xticklabels=self.label_encoder.classes_,
            yticklabels=self.label_encoder.classes_
        )
        plt.xlabel("Predito")
        plt.ylabel("Real")
        plt.title("Matriz de Confusão")
        plt.show()

    def validacao_cruzada(self, X, y):
        """Realiza validação cruzada para avaliar a robustez do modelo."""
        kfold = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        cv_scores = []

        for train_idx, val_idx in kfold.split(X, y):
            X_train_cv, X_val_cv = X[train_idx], X[val_idx]
            y_train_cv, y_val_cv = y[train_idx], y[val_idx]

            model_cv = self.criar_modelo(X.shape[1])
            model_cv.fit(X_train_cv, y_train_cv, epochs=50, batch_size=32, verbose=0)

            val_loss, val_accuracy = model_cv.evaluate(X_val_cv, y_val_cv, verbose=0)
            cv_scores.append(val_accuracy)

        print("\n🔍 Acurácia Média na Validação Cruzada:", np.mean(cv_scores))

    def executar_classificacao(self):
        """Executa todo o pipeline de classificação."""
        df = self.selecionar_colunas()
        X, y = self.preprocessamento(df)
        X_train, X_test, y_train, y_test = self.dividir_dados(X, y)

        model = self.criar_modelo(X_train.shape[1])
        self.treinar_modelo(model, X_train, y_train, X_test, y_test)
        self.avaliar_modelo(model, X_test, y_test)
        self.validacao_cruzada(X, y)


 
