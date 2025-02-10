import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from tensorflow.keras.utils import to_categorical
import matplotlib.pyplot as plt
import seaborn as sns





class Classficacao_Modelos:
    def __init__(self, df):
        self.dataset = df
    
    def classicacao_leve_mod_grave(self):
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

    def classificacao_evolucao(self):
        # Passo 1: Tratamento dos Dados
        # Remover registros com EVOLUCAO = 9 (Ignorado)
        dataset_filtrado = self.dataset[self.dataset['EVOLUCAO'] != 9]

        # Passo 2: Pré-processamento dos Dados
        # Normalização das variáveis numéricas
        scaler = StandardScaler()
        X = scaler.fit_transform(dataset_filtrado[['NU_IDADE_N', 'QTD_FATOR_RISC', 'QTD_SINT']])

        # Codificação one-hot da variável target
        y = to_categorical(dataset_filtrado['EVOLUCAO'] - 1)  # Subtrair 1 para ajustar os índices (1 -> 0, 2 -> 1, 3 -> 2)

        # Passo 3: Divisão dos Dados
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Passo 4: Construção da Rede Neural
        model = Sequential([
            Dense(64, activation='relu', input_shape=(X_train.shape[1],)),  # Camada oculta 1
            Dense(32, activation='relu'),  # Camada oculta 2
            Dense(3, activation='softmax')  # Camada de saída (3 classes: Melhora, Óbito, Óbito por outras causas)
        ])

        # Compilação do Modelo
        model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

        # Callback para parar o treinamento se não houver melhoria
        early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

        # Passo 5: Treinamento do Modelo
        history = model.fit(
            X_train, y_train,
            epochs=100,  # Número máximo de épocas
            batch_size=32,
            validation_split=0.2,
            callbacks=[early_stopping],
            verbose=1
        )

        # Passo 6: Avaliação do Modelo
        y_pred = model.predict(X_test)
        y_pred_classes = np.argmax(y_pred, axis=1)
        y_test_classes = np.argmax(y_test, axis=1)

        # Métricas de Avaliação
        print("Acurácia:", accuracy_score(y_test_classes, y_pred_classes))
        print("\nRelatório de Classificação:\n", classification_report(
            y_test_classes, y_pred_classes,
            target_names=['MELHORA DE QUADRO', 'ÓBITO', 'ÓBITO POR OUTRAS CAUSAS']
        ))

        # Passo 7: Implementação (Previsão para Novos Pacientes)
        novo_paciente = np.array([[65, 2, 3]])  # Exemplo: Paciente de 65 anos, 2 comorbidades, 3 sintomas
        novo_paciente_scaled = scaler.transform(novo_paciente)
        previsao = model.predict(novo_paciente_scaled)
        previsao_classe = np.argmax(previsao, axis=1)

        print("\nPrevisão para o Novo Paciente:")
        if previsao_classe == 0:
            print("Melhora de Quadro")
        elif previsao_classe == 1:
            print("Óbito")
        else:
            print("Óbito por Outras Causas")


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