import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from tensorflow.keras.utils import to_categorical
import matplotlib.pyplot as plt
import seaborn as sns


class Classficacao_Modelos:
    def __init__(self, df):
        self.dataset = df

    def calcular_taxa_mortalidade(self):
        """
        Calcula a taxa de mortalidade para cada localidade.
        """
        self.data["Taxa de Mortalidade"] = (self.data["Óbitos"] / self.data["Casos Confirmados"]) * 100
        print("✅ Taxa de mortalidade calculada!")
        
    def visualizar_mortalidade(self):
        """
        Gera um gráfico de barras da taxa de mortalidade por localidade.
        """
        mortalidade_por_localidade = self.data.groupby("Localidade")["Taxa de Mortalidade"].mean().reset_index()
        
        plt.figure(figsize=(10, 6))
        sns.barplot(x="Localidade", y="Taxa de Mortalidade", data=mortalidade_por_localidade)
        plt.title("Taxa de Mortalidade por Localidade")
        plt.xticks(rotation=45)
        plt.show()

    def preprocessar_dados(self):
        """
        Realiza o pré-processamento dos dados:
        - Converte variáveis categóricas para numéricas
        - Remove colunas desnecessárias
        - Normaliza os dados numéricos
        """
        print("🔄 Iniciando pré-processamento dos dados...")

        # Converter variáveis categóricas
        self.data = pd.get_dummies(self.data, columns=["Sexo", "Etnia/Raça", "Localidade"], drop_first=True)
        
        # Definir variáveis preditoras e alvo
        self.X = self.data.drop(["Taxa de Mortalidade", "Óbitos", "Casos Confirmados"], axis=1)
        self.y = self.data["Taxa de Mortalidade"]
        
        # Normalizar os dados numéricos
        scaler = StandardScaler()
        self.X_scaled = scaler.fit_transform(self.X)

        # Dividir o dataset em treino e teste
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            self.X_scaled, self.y, test_size=0.2, random_state=42
        )

        print("✅ Dados pré-processados com sucesso!")

    def treinar_modelo_regressao_linear(self):
        """
        Treina um modelo de regressão linear para prever a taxa de mortalidade.
        """
        print("🚀 Treinando modelo de Regressão Linear...")

        self.model_lr = LinearRegression()
        self.model_lr.fit(self.X_train, self.y_train)
        
        y_pred = self.model_lr.predict(self.X_test)
        mse = mean_squared_error(self.y_test, y_pred)
        r2 = r2_score(self.y_test, y_pred)

        print(f"📊 Erro Quadrático Médio (MSE - Regressão Linear): {mse}")
        print(f"📈 Coeficiente de Determinação (R² - Regressão Linear): {r2}")

    def treinar_modelo_random_forest(self):
        """
        Treina um modelo de Random Forest para prever a taxa de mortalidade.
        """
        print("🚀 Treinando modelo de Random Forest...")

        self.model_rf = RandomForestRegressor(random_state=42)
        self.model_rf.fit(self.X_train, self.y_train)
        
        y_pred_rf = self.model_rf.predict(self.X_test)
        mse_rf = mean_squared_error(self.y_test, y_pred_rf)
        r2_rf = r2_score(self.y_test, y_pred_rf)

        print(f"📊 Erro Quadrático Médio (MSE - Random Forest): {mse_rf}")
        print(f"📈 Coeficiente de Determinação (R² - Random Forest): {r2_rf}")

        # Exibir importância das variáveis
        importancias = self.model_rf.feature_importances_
        feature_names = self.X.columns
        importancias_df = pd.DataFrame({'Variável': feature_names, 'Importância': importancias})
        importancias_df = importancias_df.sort_values(by='Importância', ascending=False)

        plt.figure(figsize=(10, 6))
        sns.barplot(x='Importância', y='Variável', data=importancias_df)
        plt.title('Importância das Variáveis no Modelo de Random Forest')
        plt.show()

    def visualizar_predicoes(self):
        """
        Gera um gráfico comparando valores reais e previstos pelo modelo de Random Forest.
        """
        y_pred_rf = self.model_rf.predict(self.X_test)

        plt.figure(figsize=(10, 6))
        plt.scatter(self.y_test, y_pred_rf, alpha=0.5)
        plt.plot([self.y_test.min(), self.y_test.max()], [self.y_test.min(), self.y_test.max()], 'k--', lw=2)
        plt.xlabel("Valores Reais")
        plt.ylabel("Valores Previstos")
        plt.title("Valores Reais vs. Previstos (Random Forest)")
        plt.show()



    '''
    import pandas as pd

# Carregar o dataset
data = pd.read_csv('dados_mortalidade.csv')

# Calcular a taxa de mortalidade
data['Taxa de Mortalidade'] = (data['Óbitos'] / data['Casos Confirmados']) * 100

# Exibir o dataset atualizado
print(data.head())

import matplotlib.pyplot as plt
import seaborn as sns

# Agrupar por localidade e calcular a média da taxa de mortalidade
mortalidade_por_localidade = data.groupby('Localidade')['Taxa de Mortalidade'].mean().reset_index()

# Plotar o gráfico
plt.figure(figsize=(10, 6))
sns.barplot(x='Localidade', y='Taxa de Mortalidade', data=mortalidade_por_localidade)
plt.title('Taxa de Mortalidade por Localidade')
plt.xticks(rotation=45)
plt.show()

# Converter variáveis categóricas
data = pd.get_dummies(data, columns=['Sexo', 'Etnia/Raça', 'Localidade'], drop_first=True)

# Exibir o dataset após a codificação
print(data.head())


from sklearn.model_selection import train_test_split

# Definir variáveis independentes (features) e dependente (target)
X = data.drop(['Taxa de Mortalidade', 'Óbitos', 'Casos Confirmados'], axis=1)  # Remover colunas não usadas
y = data['Taxa de Mortalidade']

# Dividir o dataset em treino e teste
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

# Treinar o modelo
model = LinearRegression()
model.fit(X_train, y_train)

# Fazer previsões
y_pred = model.predict(X_test)

# Avaliar o modelo
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)
print(f"Erro Quadrático Médio (MSE): {mse}")
print(f"Coeficiente de Determinação (R²): {r2}")


from sklearn.ensemble import RandomForestRegressor

# Treinar o modelo
model_rf = RandomForestRegressor(random_state=42)
model_rf.fit(X_train, y_train)

# Fazer previsões
y_pred_rf = model_rf.predict(X_test)

# Avaliar o modelo
mse_rf = mean_squared_error(y_test, y_pred_rf)
r2_rf = r2_score(y_test, y_pred_rf)
print(f"Erro Quadrático Médio (MSE - Random Forest): {mse_rf}")
print(f"Coeficiente de Determinação (R² - Random Forest): {r2_rf}")

# Importância das variáveis
importancias = model_rf.feature_importances_
feature_names = X.columns
print("Importância das Variáveis:")
for feature, importance in zip(feature_names, importancias):
    print(f"{feature}: {importance}")

    # Criar um DataFrame com as importâncias
importancias_df = pd.DataFrame({'Variável': feature_names, 'Importância': importancias})

# Ordenar por importância
importancias_df = importancias_df.sort_values(by='Importância', ascending=False)

# Plotar o gráfico
plt.figure(figsize=(10, 6))
sns.barplot(x='Importância', y='Variável', data=importancias_df)
plt.title('Importância das Variáveis no Modelo de Random Forest')
plt.show()


# Plotar valores reais vs. previstos
plt.figure(figsize=(10, 6))
plt.scatter(y_test, y_pred_rf, alpha=0.5)
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=2)
plt.xlabel('Valores Reais')
plt.ylabel('Valores Previstos')
plt.title('Valores Reais vs. Previstos (Random Forest)')
plt.show()
    '''

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


Referências
BRASIL. Ministério da Saúde. Boletim Epidemiológico Especial - Doença pelo Coronavírus COVID-19. Brasília, DF: Ministério da Saúde, 2021. Disponível em: https://www.gov.br/saude. Acesso em: 05 fev. 2024.

HAN, J.; KAMBER, M.; PEI, J. Data Mining: Concepts and Techniques. 3rd ed. Amsterdam: Elsevier, 2011.

RUSSELL, S. J.; NORVIG, P. Inteligência Artificial: Uma Abordagem Moderna. 4. ed. Rio de Janeiro: Elsevier, 2021.

SOUZA, W. M. et al. Epidemiological and clinical characteristics of the COVID-19 epidemic in Brazil. Nature Human Behaviour, v. 4, p. 856-865, 2020. Disponível em: https://www.nature.com/articles. Acesso em: 05 fev. 2024.
'''