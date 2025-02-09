import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns


class Classficacao_Grave_Mod_Leve:
    def __init__(self, df):
        self.dataset = df
    
    def classicacao(self):
        # Selecionando apenas as colunas relevantes
        colunas_relevantes = [
            # 📌 Informações básicas
            "IDADE", 
            
            # 📌 Sintomas principais
            "FEBRE", "TOSSE", "DISPNEIA", "SATURACAO", 
            
            # 📌 Sintomas adicionais
            "FADIGA", "DOR_ABD", "DESC_RESP", "VOMITO", "DIARREIA", 
            "PERD_OLFT", "PERD_PALA", 
            
            # 📌 Doenças pré-existentes
            "DIABETES", "CARDIOPATI", "OBESIDADE", 
            "RENAL", "HEPATICA", "NEUROLOGIC", 
            "IMUNODEPRE", "ASMA", "PNEUMOPATI", 
            
            # 📌 Classificação (variável alvo)
            "CLASSIFICACAO"  # Leve, Moderado ou Grave
        ]
        df = df[colunas_relevantes]

        # Converter variáveis categóricas para numéricas
        label_encoder = LabelEncoder()
        df["CLASSIFICACAO"] = label_encoder.fit_transform(df["CLASSIFICACAO"])  # 0=leve, 1=moderado, 2=grave

        # Separar variáveis preditoras (X) e alvo (y)
        X = df.drop(columns=["CLASSIFICACAO"])
        y = df["CLASSIFICACAO"]

        # Normalizar os dados numéricos
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Dividir os dados em treino (80%) e teste (20%)
        X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42, stratify=y)

        # 🚀 Passo 3: Construção da Rede Neural
        model = Sequential([
            Dense(64, activation="relu", input_shape=(X_train.shape[1],)),
            Dropout(0.3),  # Evita overfitting
            Dense(32, activation="relu"),
            Dropout(0.2),
            Dense(16, activation="relu"),
            Dense(3, activation="softmax")  # 3 classes de saída (leve, moderado, grave)
        ])

        # 🚀 Compilar o modelo
        model.compile(optimizer="adam", loss="sparse_categorical_crossentropy", metrics=["accuracy"])

        # 🚀 Passo 4: Treinar a Rede Neural
        history = model.fit(X_train, y_train, epochs=50, batch_size=32, validation_data=(X_test, y_test))

        # 🚀 Avaliação do Modelo
        y_pred = np.argmax(model.predict(X_test), axis=1)
        print("\n🔍 Relatório de Classificação:\n", classification_report(y_test, y_pred, target_names=label_encoder.classes_))

        # 🚀 Matriz de Confusão
        plt.figure(figsize=(6,5))
        sns.heatmap(confusion_matrix(y_test, y_pred), annot=True, fmt="d", cmap="Blues", xticklabels=label_encoder.classes_, yticklabels=label_encoder.classes_)
        plt.xlabel("Predito")
        plt.ylabel("Real")
        plt.title("Matriz de Confusão")
        plt.show()


'''
3. Mortalidade de uma Doença com Base na Localidade
Passo a Passo
Objetivo: Criar um modelo que analisa a taxa de mortalidade de uma doença considerando diferentes regiões geográficas.

Passo 1: Coleta de Dados
Variáveis principais:
Região (estado, cidade, bairro)
Número total de casos
Número de óbitos
População total
Recursos hospitalares disponíveis (leitos, UTI, médicos por habitante)
Fatores socioeconômicos (renda média, saneamento básico, vacinação)
Passo 2: Cálculo da Mortalidade
Taxa de Mortalidade: 
Mortalidade
=
O
ˊ
bitos
Casos Confirmados
×
100
Mortalidade= 
Casos Confirmados
O
ˊ
 bitos
​
 ×100
Criar mapas de calor com mortalidade por região.
Passo 3: Modelagem
Regressão Linear ou Regressão Logística para identificar fatores que impactam a mortalidade.
Árvores de Decisão ou Random Forest para entender os fatores que mais influenciam a taxa de óbitos.
Passo 4: Predição
O modelo pode prever se determinada região terá um aumento ou redução na taxa de mortalidade nos próximos meses.
4. Previsão de Doenças com Base nos Sintomas
Passo a Passo
Objetivo: Criar um sistema que, com base nos sintomas apresentados por um paciente, prevê qual doença ele pode ter.

Passo 1: Coleta de Dados
Variáveis principais:
Sintomas (febre, tosse, fadiga, dor de cabeça, falta de ar, diarreia, vômito, dor no peito, etc.)
Doença diagnosticada (gripe, COVID-19, pneumonia, dengue, etc.)
Passo 2: Criação de um Dataset
Cada linha representa um paciente com os sintomas apresentados e o diagnóstico final.
Transformação dos sintomas em variáveis binárias (1 = presente, 0 = ausente).
Passo 3: Modelagem
Modelos possíveis:
Árvore de Decisão (interpretação fácil).
Random Forest (bom para evitar overfitting).
Redes Neurais (se houver uma base de dados muito grande).
Passo 4: Treinamento e Avaliação
Divisão dos dados (80% treino, 20% teste).
Treinar o modelo para aprender a relação entre sintomas e doenças.
Avaliar com métricas de classificação (precisão, recall, F1-score).
Passo 5: Predição
Entrar com os sintomas do paciente e prever qual doença ele pode ter.
Exemplo:
Sintomas: Febre, tosse, dor de cabeça → Predição: Gripe (90% de chance)
Sintomas: Febre, tosse, falta de ar → Predição: COVID-19 (85% de chance)
'''